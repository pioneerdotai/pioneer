use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use base64::Engine as _;
use async_trait::async_trait;
use hmac::{Hmac, KeyInit, Mac};
use pioneer_config::GatewayArtifactsConfig;
use pioneer_protocol::{
    ArtifactProjectionKind, AuthSessionId, GatewayId, PrincipalId,
};
use rand::fill;
use sha2::Sha256;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroize;

use crate::helpers::unix_timestamp_secs;
use crate::auth::AuthSessionDisconnectHook;

const VIEW_GRANT_HASH_DOMAIN: &[u8] = b"pioneer:view-grant:v1\0";
const VIEW_GRANT_SECRET_BYTES: usize = 32;
const VIEW_GRANT_SECRET_ENCODED_BYTES: usize = 43;
const VIEW_GRANT_TTL_MIN_SECS: u64 = 2 * 60;
const VIEW_GRANT_TTL_MAX_SECS: u64 = 5 * 60;
const MAX_SCOPE_ID_BYTES: usize = 128;

type ViewGrantHash = [u8; 32];

pub(crate) trait ViewGrantClock: Send + Sync {
    fn now_unix(&self) -> Result<u64, ViewGrantError>;
}

#[derive(Debug)]
struct SystemViewGrantClock;

impl ViewGrantClock for SystemViewGrantClock {
    fn now_unix(&self) -> Result<u64, ViewGrantError> {
        unix_timestamp_secs().map_err(|_| ViewGrantError::Clock)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ViewGrantDisposition {
    Inline,
    Attachment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ViewGrantScope {
    pub(crate) gateway_id: GatewayId,
    pub(crate) principal_id: PrincipalId,
    pub(crate) auth_session_id: AuthSessionId,
    pub(crate) workspace_id: String,
    pub(crate) artifact_id: String,
    pub(crate) version_id: String,
    pub(crate) artifact_sha256: [u8; 32],
    pub(crate) projection_kind: Option<ArtifactProjectionKind>,
    pub(crate) disposition: ViewGrantDisposition,
}

impl ViewGrantScope {
    fn validate(&self) -> Result<(), ViewGrantError> {
        for value in [
            self.workspace_id.as_str(),
            self.artifact_id.as_str(),
            self.version_id.as_str(),
        ] {
            if value.is_empty() || value.len() > MAX_SCOPE_ID_BYTES {
                return Err(ViewGrantError::InvalidScope);
            }
        }
        Ok(())
    }
}

pub(crate) struct OpaqueViewGrantSecret {
    value: Option<String>,
}

impl OpaqueViewGrantSecret {
    pub(crate) fn into_relative_url(mut self) -> String {
        let secret = self
            .value
            .take()
            .expect("view-grant secret may be returned only once");
        format!("/storage/views/{secret}")
    }
}

impl fmt::Debug for OpaqueViewGrantSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpaqueViewGrantSecret([REDACTED])")
    }
}

impl Drop for OpaqueViewGrantSecret {
    fn drop(&mut self) {
        if let Some(value) = self.value.as_mut() {
            value.zeroize();
        }
    }
}

pub(crate) struct IssuedViewGrant {
    pub(crate) secret: OpaqueViewGrantSecret,
    pub(crate) expires_at_unix: u64,
}

impl fmt::Debug for IssuedViewGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedViewGrant")
            .field("secret", &"[REDACTED]")
            .field("expires_at_unix", &self.expires_at_unix)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedViewGrant {
    pub(crate) scope: ViewGrantScope,
    pub(crate) protocol_version: u16,
    pub(crate) issued_at_unix: u64,
    pub(crate) expires_at_unix: u64,
}

pub(crate) struct ViewGrantLease {
    pub(crate) grant: ResolvedViewGrant,
    cancellation: CancellationToken,
    cancellation_wait: Pin<Box<dyn Future<Output = ()> + Send>>,
    _permit: OwnedSemaphorePermit,
}

impl fmt::Debug for ViewGrantLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ViewGrantLease")
            .field("grant", &self.grant)
            .field("concurrency_permit", &"held")
            .field("invalidated", &self.cancellation.is_cancelled())
            .finish()
    }
}

impl ViewGrantLease {
    pub(crate) fn invalidated(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub(crate) fn poll_invalidated(&mut self, context: &mut Context<'_>) -> Poll<()> {
        self.cancellation_wait.as_mut().poll(context)
    }
}

struct StoredViewGrant {
    scope: ViewGrantScope,
    protocol_version: u16,
    issued_at_unix: u64,
    expires_at_unix: u64,
    concurrency: Arc<Semaphore>,
    cancellation: CancellationToken,
}

impl fmt::Debug for StoredViewGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredViewGrant")
            .field("scope", &self.scope)
            .field("protocol_version", &self.protocol_version)
            .field("issued_at_unix", &self.issued_at_unix)
            .field("expires_at_unix", &self.expires_at_unix)
            .field("available_concurrency", &self.concurrency.available_permits())
            .finish()
    }
}

#[derive(Default)]
struct ViewGrantState {
    grants: HashMap<ViewGrantHash, StoredViewGrant>,
    by_session: HashMap<AuthSessionId, HashSet<ViewGrantHash>>,
}

#[derive(Debug, Clone, Copy)]
struct ViewGrantLimits {
    ttl_secs: u64,
    global: usize,
    per_session: usize,
    streams_per_grant: usize,
}

impl ViewGrantLimits {
    fn from_config(config: &GatewayArtifactsConfig) -> Result<Self, ViewGrantError> {
        if !(VIEW_GRANT_TTL_MIN_SECS..=VIEW_GRANT_TTL_MAX_SECS)
            .contains(&config.view_grant_ttl_secs)
            || config.view_grants_global == 0
            || config.view_grants_per_session == 0
            || config.view_grants_per_session > config.view_grants_global
            || config.view_grant_streams == 0
        {
            return Err(ViewGrantError::InvalidConfig);
        }
        Ok(Self {
            ttl_secs: config.view_grant_ttl_secs,
            global: config.view_grants_global,
            per_session: config.view_grants_per_session,
            streams_per_grant: config.view_grant_streams,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ViewGrantError {
    InvalidConfig,
    InvalidScope,
    Clock,
    Capacity,
    UnknownOrExpired,
    Concurrency,
}

pub(crate) struct ViewGrantService {
    hash_key: [u8; 32],
    limits: ViewGrantLimits,
    clock: Arc<dyn ViewGrantClock>,
    state: Mutex<ViewGrantState>,
}

impl fmt::Debug for ViewGrantService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ViewGrantService")
            .field("hash_key", &"[REDACTED]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Drop for ViewGrantService {
    fn drop(&mut self) {
        self.hash_key.zeroize();
    }
}

impl ViewGrantService {
    pub(crate) fn new(config: &GatewayArtifactsConfig) -> Result<Arc<Self>, ViewGrantError> {
        let mut hash_key = [0_u8; 32];
        fill(&mut hash_key);
        Self::with_clock_and_key(config, Arc::new(SystemViewGrantClock), hash_key)
    }

    fn with_clock_and_key(
        config: &GatewayArtifactsConfig,
        clock: Arc<dyn ViewGrantClock>,
        hash_key: [u8; 32],
    ) -> Result<Arc<Self>, ViewGrantError> {
        Ok(Arc::new(Self {
            hash_key,
            limits: ViewGrantLimits::from_config(config)?,
            clock,
            state: Mutex::new(ViewGrantState::default()),
        }))
    }

    pub(crate) fn mint(
        &self,
        scope: ViewGrantScope,
    ) -> Result<IssuedViewGrant, ViewGrantError> {
        scope.validate()?;
        let issued_at_unix = self.clock.now_unix()?;
        let expires_at_unix = issued_at_unix
            .checked_add(self.limits.ttl_secs)
            .ok_or(ViewGrantError::Clock)?;
        let mut state = self.lock_state();
        self.prune_expired_locked(&mut state, issued_at_unix);

        let session_count = state
            .by_session
            .get(&scope.auth_session_id)
            .map_or(0, HashSet::len);
        if state.grants.len() >= self.limits.global || session_count >= self.limits.per_session {
            return Err(ViewGrantError::Capacity);
        }

        let (secret, hash) = loop {
            let secret = generate_secret();
            let hash = self.hash_secret(secret.as_str());
            if !state.grants.contains_key(&hash) {
                break (secret, hash);
            }
        };
        state
            .by_session
            .entry(scope.auth_session_id.clone())
            .or_default()
            .insert(hash);
        state.grants.insert(
            hash,
            StoredViewGrant {
                scope,
                protocol_version: crate::transport::protocol::PIONEER_PROTOCOL_VERSION_NUMBER,
                issued_at_unix,
                expires_at_unix,
                concurrency: Arc::new(Semaphore::new(self.limits.streams_per_grant)),
                cancellation: CancellationToken::new(),
            },
        );

        Ok(IssuedViewGrant {
            secret: OpaqueViewGrantSecret {
                value: Some(secret),
            },
            expires_at_unix,
        })
    }

    pub(crate) fn resolve(&self, presented: &str) -> Result<ViewGrantLease, ViewGrantError> {
        if !valid_presented_secret(presented) {
            return Err(ViewGrantError::UnknownOrExpired);
        }
        let now_unix = self.clock.now_unix()?;
        let hash = self.hash_secret(presented);
        let mut state = self.lock_state();
        self.prune_expired_locked(&mut state, now_unix);
        let stored = state
            .grants
            .get(&hash)
            .ok_or(ViewGrantError::UnknownOrExpired)?;
        let permit = stored
            .concurrency
            .clone()
            .try_acquire_owned()
            .map_err(|_| ViewGrantError::Concurrency)?;
        let grant = ResolvedViewGrant {
            scope: stored.scope.clone(),
            protocol_version: stored.protocol_version,
            issued_at_unix: stored.issued_at_unix,
            expires_at_unix: stored.expires_at_unix,
        };
        let cancellation = stored.cancellation.clone();
        drop(state);
        Ok(ViewGrantLease {
            grant,
            cancellation: cancellation.clone(),
            cancellation_wait: Box::pin(cancellation.cancelled_owned()),
            _permit: permit,
        })
    }

    pub(crate) fn invalidate_session(&self, session_id: &AuthSessionId) -> usize {
        let mut state = self.lock_state();
        let Some(hashes) = state.by_session.remove(session_id) else {
            return 0;
        };
        let count = hashes.len();
        for hash in hashes {
            if let Some(grant) = state.grants.remove(&hash) {
                grant.cancellation.cancel();
            }
        }
        count
    }

    fn prune_expired_locked(&self, state: &mut ViewGrantState, now_unix: u64) {
        let expired = state
            .grants
            .iter()
            .filter_map(|(hash, grant)| (grant.expires_at_unix <= now_unix).then_some(*hash))
            .collect::<Vec<_>>();
        for hash in expired {
            if let Some(grant) = state.grants.remove(&hash) {
                grant.cancellation.cancel();
                if let Some(session_grants) =
                    state.by_session.get_mut(&grant.scope.auth_session_id)
                {
                    session_grants.remove(&hash);
                    if session_grants.is_empty() {
                        state.by_session.remove(&grant.scope.auth_session_id);
                    }
                }
            }
        }
    }

    fn hash_secret(&self, secret: &str) -> ViewGrantHash {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&self.hash_key)
            .expect("HMAC-SHA256 accepts a 32-byte key");
        mac.update(VIEW_GRANT_HASH_DOMAIN);
        mac.update(secret.as_bytes());
        mac.finalize().into_bytes().into()
    }

    fn lock_state(&self) -> MutexGuard<'_, ViewGrantState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[async_trait]
impl AuthSessionDisconnectHook for ViewGrantService {
    async fn disconnect_session(
        &self,
        session_id: &AuthSessionId,
        _reason: pioneer_protocol::AuthSessionTerminationReason,
    ) {
        self.invalidate_session(session_id);
    }
}

fn generate_secret() -> String {
    let mut bytes = [0_u8; VIEW_GRANT_SECRET_BYTES];
    fill(&mut bytes);
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    bytes.zeroize();
    encoded
}

fn valid_presented_secret(value: &str) -> bool {
    value.len() == VIEW_GRANT_SECRET_ENCODED_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[derive(Debug)]
    struct TestClock(AtomicU64);

    impl TestClock {
        const fn new(now_unix: u64) -> Self {
            Self(AtomicU64::new(now_unix))
        }

        fn advance(&self, seconds: u64) {
            self.0.fetch_add(seconds, Ordering::SeqCst);
        }
    }

    impl ViewGrantClock for TestClock {
        fn now_unix(&self) -> Result<u64, ViewGrantError> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    fn config() -> GatewayArtifactsConfig {
        GatewayArtifactsConfig::default()
    }

    fn scope(session_suffix: &str) -> ViewGrantScope {
        ViewGrantScope {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            auth_session_id: AuthSessionId::new(format!("S{session_suffix:0>20}")).unwrap(),
            workspace_id: "workspace-1".to_owned(),
            artifact_id: "artifact-1".to_owned(),
            version_id: "version-1".to_owned(),
            artifact_sha256: [0xAB; 32],
            projection_kind: None,
            disposition: ViewGrantDisposition::Inline,
        }
    }

    fn service(
        config: &GatewayArtifactsConfig,
        clock: Arc<TestClock>,
        key_byte: u8,
    ) -> Arc<ViewGrantService> {
        ViewGrantService::with_clock_and_key(config, clock, [key_byte; 32]).unwrap()
    }

    fn extract_secret(issued: IssuedViewGrant) -> String {
        issued
            .secret
            .into_relative_url()
            .strip_prefix("/storage/views/")
            .unwrap()
            .to_owned()
    }

    #[test]
    fn generated_secret_is_url_safe_high_entropy_and_stored_only_as_hash() {
        let clock = Arc::new(TestClock::new(1_000));
        let service = service(&config(), clock, 7);
        let issued = service.mint(scope("1")).unwrap();
        let secret = extract_secret(issued);

        assert!(valid_presented_secret(secret.as_str()));
        assert_eq!(secret.len(), VIEW_GRANT_SECRET_ENCODED_BYTES);
        let state_debug = format!("{:?}", service.lock_state().grants);
        assert!(!state_debug.contains(secret.as_str()));
        assert!(service.resolve(secret.as_str()).is_ok());
    }

    #[test]
    fn raw_secret_and_service_debug_are_redacted() {
        let clock = Arc::new(TestClock::new(1_000));
        let service = service(&config(), clock, 7);
        let issued = service.mint(scope("1")).unwrap();
        let debug = format!("{issued:?} {service:?}");

        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("/storage/views/"));
    }

    #[test]
    fn grant_is_multi_use_until_exact_expiry_and_prunes_afterwards() {
        let clock = Arc::new(TestClock::new(1_000));
        let service = service(&config(), clock.clone(), 7);
        let issued = service.mint(scope("1")).unwrap();
        assert_eq!(issued.expires_at_unix, 1_180);
        let secret = extract_secret(issued);

        drop(service.resolve(secret.as_str()).unwrap());
        drop(service.resolve(secret.as_str()).unwrap());
        clock.advance(179);
        drop(service.resolve(secret.as_str()).unwrap());
        clock.advance(1);
        assert!(matches!(
            service.resolve(secret.as_str()),
            Err(ViewGrantError::UnknownOrExpired)
        ));
        assert!(service.lock_state().grants.is_empty());
        assert!(service.lock_state().by_session.is_empty());
    }

    #[test]
    fn per_grant_concurrency_is_bounded_and_released_by_drop() {
        let mut config = config();
        config.view_grant_streams = 1;
        let clock = Arc::new(TestClock::new(1_000));
        let service = service(&config, clock, 7);
        let secret = extract_secret(service.mint(scope("1")).unwrap());
        let first = service.resolve(secret.as_str()).unwrap();

        assert!(matches!(
            service.resolve(secret.as_str()),
            Err(ViewGrantError::Concurrency)
        ));
        drop(first);
        assert!(service.resolve(secret.as_str()).is_ok());
    }

    #[test]
    fn global_and_per_session_capacity_are_bounded_and_expiry_frees_capacity() {
        let mut config = config();
        config.view_grants_global = 2;
        config.view_grants_per_session = 1;
        let clock = Arc::new(TestClock::new(1_000));
        let service = service(&config, clock.clone(), 7);

        service.mint(scope("1")).unwrap();
        assert!(matches!(
            service.mint(scope("1")),
            Err(ViewGrantError::Capacity)
        ));
        service.mint(scope("2")).unwrap();
        assert!(matches!(
            service.mint(scope("3")),
            Err(ViewGrantError::Capacity)
        ));

        clock.advance(180);
        assert!(service.mint(scope("3")).is_ok());
    }

    #[test]
    fn session_invalidation_removes_only_its_grants() {
        let clock = Arc::new(TestClock::new(1_000));
        let service = service(&config(), clock, 7);
        let first_scope = scope("1");
        let first_session = first_scope.auth_session_id.clone();
        let first = extract_secret(service.mint(first_scope).unwrap());
        let second = extract_secret(service.mint(scope("2")).unwrap());
        let active_lease = service.resolve(first.as_str()).unwrap();

        assert_eq!(service.invalidate_session(&first_session), 1);
        assert!(active_lease.invalidated());
        assert!(matches!(
            service.resolve(first.as_str()),
            Err(ViewGrantError::UnknownOrExpired)
        ));
        assert!(service.resolve(second.as_str()).is_ok());
    }

    #[test]
    fn restart_invalidates_grants_by_empty_store_and_rotated_hash_key() {
        let clock = Arc::new(TestClock::new(1_000));
        let first_service = service(&config(), clock.clone(), 7);
        let secret = extract_secret(first_service.mint(scope("1")).unwrap());
        assert!(first_service.resolve(secret.as_str()).is_ok());

        let restarted_service = service(&config(), clock, 8);
        assert!(matches!(
            restarted_service.resolve(secret.as_str()),
            Err(ViewGrantError::UnknownOrExpired)
        ));
    }

    #[test]
    fn ttl_and_limit_configuration_rejects_out_of_contract_values() {
        let clock = Arc::new(TestClock::new(1_000));
        for ttl in [119, 301] {
            let mut invalid = config();
            invalid.view_grant_ttl_secs = ttl;
            assert!(matches!(
                ViewGrantService::with_clock_and_key(&invalid, clock.clone(), [7; 32]),
                Err(ViewGrantError::InvalidConfig)
            ));
        }

        let mut invalid = config();
        invalid.view_grants_per_session = invalid.view_grants_global + 1;
        assert!(matches!(
            ViewGrantService::with_clock_and_key(&invalid, clock, [7; 32]),
            Err(ViewGrantError::InvalidConfig)
        ));
    }

    #[test]
    fn scope_is_exact_and_bounded() {
        let clock = Arc::new(TestClock::new(1_000));
        let service = service(&config(), clock, 7);
        let expected = scope("1");
        let secret = extract_secret(service.mint(expected.clone()).unwrap());
        let resolved = service.resolve(secret.as_str()).unwrap();

        assert_eq!(resolved.grant.scope, expected);
        assert_eq!(
            resolved.grant.protocol_version,
            crate::transport::protocol::PIONEER_PROTOCOL_VERSION_NUMBER
        );

        let mut invalid = scope("2");
        invalid.version_id = "x".repeat(MAX_SCOPE_ID_BYTES + 1);
        assert!(matches!(service.mint(invalid), Err(ViewGrantError::InvalidScope)));
    }
}
