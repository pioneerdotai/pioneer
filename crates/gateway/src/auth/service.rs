use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Utc};
use pioneer_config::GatewayAuthConfig;
use pioneer_crud::{
    DeviceActivationFailureOutcome, NewPendingDeviceSessionRow, NewRefreshCredentialRow,
    PendingSessionIssuer, activate_pending_auth_session, activate_pending_device,
    advance_auth_session_refresh, auth_session_status_to_db, delete_refresh_for_session,
    expire_pending_auth_session, expire_session_refresh_family, expire_stale_auth_sessions,
    expire_stale_pending_sessions, insert_pending_device_session, insert_refresh_credential,
    list_pending_activation_locator_hashes, list_sessions_for_principal,
    load_active_device_by_installation, load_active_session_by_device, load_current_refresh,
    load_device, load_gateway_singleton, load_pending_local_session,
    load_pending_session_by_activation_locator_hash, load_pending_session_for_creator,
    load_principal_by_id, load_session, load_session_by_activation_hash, mark_device_revoked,
    mark_session_revoked, record_failed_device_activation, replace_current_refresh,
    revoke_session_family_for_refresh_reuse, touch_active_auth_session, touch_active_device,
};
use pioneer_protocol::{
    AUTH_DOMAIN_ID_LEN, AuthDeviceActivateParams, AuthDeviceCreateResponse, AuthDeviceSnapshot,
    AuthGatewaySnapshot, AuthLogoutResponse, AuthMeResponse, AuthPrincipalSnapshot,
    AuthRefreshGrant, AuthRefreshParams, AuthSecretString, AuthSessionGrant, AuthSessionId,
    AuthSessionListItem, AuthSessionListResponse, AuthSessionRevokeReason,
    AuthSessionRevokeResponse, AuthSessionSnapshot, AuthSessionStatus,
    AuthSessionTerminationReason, ClientInstallationDescriptor, ClientKind, CredentialStorageOrder,
    DEVICE_ACTIVATION_ALPHABET, DeviceId, DeviceStatus, PrincipalKind, PrincipalStatus,
    RefreshCredentialId, RequestId, TokenFamilyId, format_device_activation_code, generate_id,
};
use sea_orm::{
    DatabaseConnection, DatabaseTransaction, SqliteTransactionMode, TransactionOptions,
    TransactionTrait,
};
use serde_json::{Value as JsonValue, to_value};
use subtle::ConstantTimeEq;

use crate::helpers::unix_timestamp_secs;
use crate::identity::IdentityBootstrapSnapshot;
use crate::secrets::AuthKeyMaterial;
use crate::transport::{AUTH_DEVICE_ACTIVATE, AUTH_REFRESH, RestrictedExchangeExecutor};

use super::{
    AUTH_SCHEMA_VERSION, AccessCredential, AccessJwtIssuer, AccessJwtSubject, AuthError,
    AuthErrorCode, AuthenticatedSessionPrincipal, OpaqueCredentialFactory, RestrictedAdmission,
    RestrictedAuthContext,
};

const MAX_INSTALLATION_TEXT: usize = 255;
const MAX_DIAGNOSTIC_TEXT: usize = 255;
const AUTH_MAINTENANCE_BATCH_SIZE: u64 = 256;
const AUTH_MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

pub(crate) struct GatewayAuthService {
    database: DatabaseConnection,
    config: GatewayAuthConfig,
    identity: Arc<IdentityBootstrapSnapshot>,
    access_issuer: AccessJwtIssuer,
    opaque_credentials: OpaqueCredentialFactory,
    disconnect_hook: std::sync::RwLock<Option<Arc<dyn AuthSessionDisconnectHook>>>,
    #[cfg(test)]
    activation_failpoint: std::sync::atomic::AtomicU8,
    #[cfg(test)]
    refresh_failpoint: std::sync::atomic::AtomicU8,
}

#[async_trait]
pub(crate) trait AuthSessionDisconnectHook: Send + Sync {
    async fn disconnect_session(
        &self,
        session_id: &AuthSessionId,
        reason: AuthSessionTerminationReason,
    );
}

struct PendingSessionIds {
    device_id: DeviceId,
    session_id: AuthSessionId,
    token_family_id: TokenFamilyId,
}

impl PendingSessionIds {
    fn random() -> Result<Self, AuthError> {
        Ok(Self {
            device_id: typed_id(generate_id(AUTH_DOMAIN_ID_LEN), DeviceId::new)?,
            session_id: typed_id(generate_id(AUTH_DOMAIN_ID_LEN), AuthSessionId::new)?,
            token_family_id: typed_id(generate_id(AUTH_DOMAIN_ID_LEN), TokenFamilyId::new)?,
        })
    }
}

struct IssuedCredentialIds {
    refresh_id: RefreshCredentialId,
    access_jti: String,
}

impl IssuedCredentialIds {
    fn random() -> Result<Self, AuthError> {
        Ok(Self {
            refresh_id: typed_id(generate_id(AUTH_DOMAIN_ID_LEN), RefreshCredentialId::new)?,
            access_jti: generate_id(AUTH_DOMAIN_ID_LEN),
        })
    }
}

#[cfg(test)]
struct SessionGrantIds {
    device_id: DeviceId,
    session_id: AuthSessionId,
    refresh_id: RefreshCredentialId,
    token_family_id: TokenFamilyId,
    access_jti: String,
}

fn typed_id<T, E>(
    value: String,
    constructor: impl FnOnce(String) -> Result<T, E>,
) -> Result<T, AuthError> {
    constructor(value).map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))
}

impl GatewayAuthService {
    pub(crate) fn new(
        database: DatabaseConnection,
        config: GatewayAuthConfig,
        identity: Arc<IdentityBootstrapSnapshot>,
        access_key: &AuthKeyMaterial,
        credential_hmac_key: &AuthKeyMaterial,
    ) -> Result<Self, AuthError> {
        Ok(Self {
            access_issuer: AccessJwtIssuer::new(
                access_key.as_bytes(),
                &config,
                identity.gateway.id.clone(),
            )?,
            opaque_credentials: OpaqueCredentialFactory::new(credential_hmac_key.as_bytes())?,
            database,
            config,
            identity,
            disconnect_hook: std::sync::RwLock::new(None),
            #[cfg(test)]
            activation_failpoint: std::sync::atomic::AtomicU8::new(0),
            #[cfg(test)]
            refresh_failpoint: std::sync::atomic::AtomicU8::new(0),
        })
    }

    pub(crate) async fn exchange_refresh(
        &self,
        admission: RestrictedAdmission,
        mut params: AuthRefreshParams,
    ) -> Result<AuthRefreshGrant, AuthError> {
        if !matches!(admission.context(), RestrictedAuthContext::Refresh(_)) {
            return Err(AuthError::new(AuthErrorCode::MethodNotAllowed));
        }
        RequestId::new(params.refresh_request_id.clone())
            .map_err(|_| AuthError::new(AuthErrorCode::MalformedCredential))?;
        params.client_version = bounded_optional(params.client_version, MAX_DIAGNOSTIC_TEXT)?;
        let presented = self
            .opaque_credentials
            .verify_refresh_raw(admission.credential().expose_for_authentication())?;
        let presented_hash = self
            .opaque_credentials
            .fingerprint_refresh_raw(admission.credential().expose_for_authentication());
        let now_unix =
            unix_timestamp_secs().map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let now = datetime(now_unix)?;
        let next_generation = presented
            .generation
            .checked_add(1)
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let refresh_expires_at_unix = now_unix
            .checked_add(self.config.refresh_token_ttl_seconds)
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let access_expires_at_unix = now_unix
            .checked_add(self.config.access_token_ttl_seconds)
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let refresh_expires_at = datetime(refresh_expires_at_unix)?;
        let successor = self.opaque_credentials.generate_refresh(
            &presented.session_id,
            &presented.token_family_id,
            next_generation,
            refresh_expires_at_unix,
        );
        let successor_hash = self.opaque_credentials.fingerprint(&successor);
        let access_jti = generate_id(AUTH_DOMAIN_ID_LEN);

        let transaction = self
            .database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let session = match load_session(&transaction, &presented.session_id)
            .await
            .map_err(storage_error)?
        {
            Some(session) => session,
            None => {
                let _ = transaction.rollback().await;
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
        };
        if session.token_family_id != presented.token_family_id.as_str() {
            let _ = transaction.rollback().await;
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        if session.status == "revoked" {
            let code = if session.revoke_reason.as_deref() == Some("refresh_reuse") {
                AuthErrorCode::SessionCompromised
            } else {
                AuthErrorCode::SessionRevoked
            };
            let _ = transaction.rollback().await;
            return Err(AuthError::new(code));
        }
        if session.status == "expired" {
            let _ = transaction.rollback().await;
            return Err(AuthError::new(AuthErrorCode::SessionExpired));
        }
        if session.status != "active" {
            let _ = transaction.rollback().await;
            return Err(AuthError::new(AuthErrorCode::SessionRevoked));
        }
        let session_generation = u64::try_from(session.refresh_generation)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let device_id = DeviceId::new(session.device_id.clone())
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        if session
            .refresh_expires_at
            .as_ref()
            .is_none_or(|expires_at| now >= *expires_at)
        {
            expire_session_refresh_family(
                &transaction,
                &presented.session_id,
                &presented.token_family_id,
                now,
            )
            .await
            .map_err(storage_error)?;
            mark_device_revoked(&transaction, &device_id, now)
                .await
                .map_err(storage_error)?;
            transaction
                .commit()
                .await
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            self.disconnect_session_after_commit(
                &presented.session_id,
                AuthSessionTerminationReason::SessionExpired,
            )
            .await;
            return Err(AuthError::new(AuthErrorCode::SessionExpired));
        }
        if presented.generation < session_generation {
            // A retired generation is replay evidence only for as long as that
            // credential itself was valid. After its signed expiry it must not
            // remain a permanent way to revoke an otherwise healthy session.
            if now_unix >= presented.expires_at_unix {
                let _ = transaction.rollback().await;
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
            revoke_session_family_for_refresh_reuse(
                &transaction,
                &presented.session_id,
                &presented.token_family_id,
                now,
            )
            .await
            .map_err(storage_error)?;
            mark_device_revoked(&transaction, &device_id, now)
                .await
                .map_err(storage_error)?;
            transaction
                .commit()
                .await
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            tracing::warn!(
                event = "auth_refresh_reuse_detected",
                session_id = %presented.session_id,
                token_family_id = %presented.token_family_id,
                presented_generation = presented.generation,
                current_generation = session_generation,
                outcome = "family_revoked",
                reason = "cryptographically_valid_prior_generation",
            );
            self.disconnect_session_after_commit(
                &presented.session_id,
                AuthSessionTerminationReason::SessionCompromised,
            )
            .await;
            return Err(AuthError::new(AuthErrorCode::SessionCompromised));
        }
        if presented.generation > session_generation {
            let _ = transaction.rollback().await;
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        let current = load_current_refresh(&transaction, &presented.session_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        if current.session_id != presented.session_id.as_str()
            || current.token_family_id != presented.token_family_id.as_str()
            || current.generation != session.refresh_generation
            || timestamp(current.expires_at)? != presented.expires_at_unix
            || current.token_hash.len() != presented_hash.len()
            || !bool::from(
                current
                    .token_hash
                    .as_slice()
                    .ct_eq(presented_hash.as_slice()),
            )
        {
            let _ = transaction.rollback().await;
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }

        let result = async {
            let current_id = RefreshCredentialId::new(current.id)
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            let gateway_id = pioneer_protocol::GatewayId::new(session.gateway_id.clone())
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            let principal_id = pioneer_protocol::PrincipalId::new(session.principal_id.clone())
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            if gateway_id != self.identity.gateway.id || principal_id != self.identity.superuser.id
            {
                return Err(AuthError::new(AuthErrorCode::GatewayIdentityMismatch));
            }
            let principal = load_principal_by_id(&transaction, &principal_id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
            if principal.gateway_id != gateway_id
                || principal.kind != PrincipalKind::Superuser
                || principal.status != PrincipalStatus::Active
            {
                return Err(AuthError::new(AuthErrorCode::SessionRevoked));
            }
            let device = load_device(&transaction, &device_id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
            if device.status != "active"
                || device.gateway_id != gateway_id.as_str()
                || device.principal_id != principal_id.as_str()
            {
                return Err(AuthError::new(AuthErrorCode::SessionRevoked));
            }
            if !replace_current_refresh(
                &transaction,
                &current_id,
                &presented.session_id,
                &presented.token_family_id,
                presented.generation,
                &presented_hash,
                next_generation,
                &successor_hash,
                now,
                refresh_expires_at,
            )
            .await
            .map_err(storage_error)?
            {
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
            self.inject_refresh_failure(1)?;
            if !advance_auth_session_refresh(
                &transaction,
                &presented.session_id,
                presented.generation,
                next_generation,
                refresh_expires_at,
                now,
            )
            .await
            .map_err(storage_error)?
            {
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
            self.inject_refresh_failure(2)?;
            let access_token = self.access_issuer.issue(
                &AccessJwtSubject {
                    gateway_id: gateway_id.clone(),
                    principal_id: principal_id.clone(),
                    device_id: device_id.clone(),
                    session_id: presented.session_id.clone(),
                },
                now_unix,
                Some(access_jti),
            )?;
            self.inject_refresh_failure(3)?;
            Ok((
                gateway_id,
                principal_id,
                principal,
                device_id,
                presented.session_id.clone(),
                presented.token_family_id.clone(),
                next_generation,
                device,
                access_token,
            ))
        }
        .await;
        let (
            gateway_id,
            principal_id,
            principal,
            device_id,
            session_id,
            token_family_id,
            generation,
            device,
            access_token,
        ) = match result {
            Ok(value) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                value
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };
        Ok(AuthRefreshGrant {
            gateway: AuthGatewaySnapshot { id: gateway_id },
            principal: AuthPrincipalSnapshot {
                id: principal_id,
                kind: principal.kind,
                display_name: principal.display_name,
                nickname: principal.nickname,
            },
            access_token: AuthSecretString::new(access_token),
            access_expires_at_unix,
            refresh_token: AuthSecretString::new(successor.expose_for_exchange().to_owned()),
            refresh_expires_at_unix,
            refresh_generation: generation,
            session: AuthSessionSnapshot {
                id: session_id,
                device_id: device_id.clone(),
                token_family_id,
                status: AuthSessionStatus::Active,
                refresh_generation: generation,
                refresh_expires_at_unix,
            },
            device: AuthDeviceSnapshot {
                id: device_id,
                installation_id: device
                    .installation_id
                    .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
                display_name: device
                    .display_name
                    .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
                client_kind: client_kind_from_db(
                    device
                        .client_kind
                        .as_deref()
                        .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
                )?,
                status: DeviceStatus::Active,
            },
            auth_protocol_version: pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION,
            credential_storage_order: CredentialStorageOrder::PersistRefreshBeforeActivatingAccess,
        })
    }

    pub(crate) async fn authenticate_access(
        &self,
        credential: AccessCredential,
    ) -> Result<Arc<AuthenticatedSessionPrincipal>, AuthError> {
        let principal = AuthenticatedSessionPrincipal {
            gateway_id: credential.subject.gateway_id,
            principal_id: credential.subject.principal_id,
            kind: PrincipalKind::Superuser,
            role_key: None,
            device_id: credential.subject.device_id,
            session_id: credential.subject.session_id,
            access_jti: credential.jti,
            access_expires_at_unix: credential.expires_at_unix,
        };
        let persisted = self.validate_session_lease(&principal).await?;
        self.record_authenticated_activity(&principal).await?;
        Ok(Arc::new(AuthenticatedSessionPrincipal {
            kind: persisted.kind,
            role_key: persisted.role_key,
            ..principal
        }))
    }

    async fn record_authenticated_activity(
        &self,
        principal: &AuthenticatedSessionPrincipal,
    ) -> Result<(), AuthError> {
        let now = chrono::Utc::now().fixed_offset();
        let transaction = self
            .database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let result = async {
            if !touch_active_device(
                &transaction,
                &principal.gateway_id,
                &principal.principal_id,
                &principal.device_id,
                now,
            )
            .await
            .map_err(storage_error)?
                || !touch_active_auth_session(
                    &transaction,
                    &principal.gateway_id,
                    &principal.principal_id,
                    &principal.device_id,
                    &principal.session_id,
                    now,
                )
                .await
                .map_err(storage_error)?
            {
                return Err(AuthError::new(AuthErrorCode::SessionRevoked));
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => transaction
                .commit()
                .await
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential)),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub(crate) fn set_disconnect_hook(&self, hook: Arc<dyn AuthSessionDisconnectHook>) {
        let mut disconnect_hook = self
            .disconnect_hook
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *disconnect_hook = Some(hook);
    }

    async fn disconnect_session_after_commit(
        &self,
        session_id: &AuthSessionId,
        reason: AuthSessionTerminationReason,
    ) {
        let hook = self
            .disconnect_hook
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook.disconnect_session(session_id, reason).await;
        }
    }

    pub(crate) async fn validate_session_lease(
        &self,
        principal: &AuthenticatedSessionPrincipal,
    ) -> Result<SessionLeaseSnapshot, AuthError> {
        let now_unix =
            unix_timestamp_secs().map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        if now_unix >= principal.access_expires_at_unix {
            return Err(AuthError::new(AuthErrorCode::CredentialExpired));
        }
        if principal.gateway_id != self.identity.gateway.id {
            return Err(AuthError::new(AuthErrorCode::GatewayIdentityMismatch));
        }
        let session = load_session(&self.database, &principal.session_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let device = load_device(&self.database, &principal.device_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let owner = load_principal_by_id(&self.database, &principal.principal_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;

        if session.gateway_id != principal.gateway_id.as_str()
            || session.principal_id != principal.principal_id.as_str()
            || session.device_id != principal.device_id.as_str()
            || device.gateway_id != principal.gateway_id.as_str()
            || device.principal_id != principal.principal_id.as_str()
            || owner.gateway_id != principal.gateway_id
        {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        if owner.status != PrincipalStatus::Active || owner.kind != PrincipalKind::Superuser {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        if session.status == "expired" {
            return Err(AuthError::new(AuthErrorCode::SessionExpired));
        }
        if session.status != "active" {
            return Err(AuthError::new(AuthErrorCode::SessionRevoked));
        }
        let now = datetime(now_unix)?;
        let refresh_expires_at = session
            .refresh_expires_at
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        if now >= refresh_expires_at {
            return Err(AuthError::new(AuthErrorCode::SessionExpired));
        }
        if device.status != "active" {
            return Err(AuthError::new(AuthErrorCode::SessionRevoked));
        }
        Ok(SessionLeaseSnapshot {
            kind: owner.kind,
            role_key: owner.role_key,
            me: AuthMeResponse {
                gateway: AuthGatewaySnapshot {
                    id: principal.gateway_id.clone(),
                },
                principal: AuthPrincipalSnapshot {
                    id: principal.principal_id.clone(),
                    kind: owner.kind,
                    display_name: owner.display_name,
                    nickname: owner.nickname,
                },
                device: AuthDeviceSnapshot {
                    id: principal.device_id.clone(),
                    installation_id: device
                        .installation_id
                        .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
                    display_name: device
                        .display_name
                        .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
                    client_kind: client_kind_from_db(
                        device
                            .client_kind
                            .as_deref()
                            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
                    )?,
                    status: DeviceStatus::Active,
                },
                session: AuthSessionSnapshot {
                    id: principal.session_id.clone(),
                    device_id: principal.device_id.clone(),
                    token_family_id: TokenFamilyId::new(session.token_family_id)
                        .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
                    status: AuthSessionStatus::Active,
                    refresh_generation: u64::try_from(session.refresh_generation)
                        .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
                    refresh_expires_at_unix: timestamp(refresh_expires_at)?,
                },
            },
        })
    }

    pub(crate) async fn auth_me(
        &self,
        principal: &AuthenticatedSessionPrincipal,
    ) -> Result<AuthMeResponse, AuthError> {
        Ok(self.validate_session_lease(principal).await?.me)
    }

    pub(crate) async fn expire_pending_device_sessions(
        &self,
        now_unix: u64,
        batch_size: u64,
    ) -> Result<u64, AuthError> {
        let transaction = self
            .database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let expired = match expire_stale_pending_sessions(
            &transaction,
            datetime(now_unix)?,
            batch_size,
        )
        .await
        {
            Ok(expired) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                expired
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(storage_error(error));
            }
        };
        u64::try_from(expired.len()).map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))
    }

    pub(crate) async fn expire_stale_sessions(
        &self,
        now_unix: u64,
        batch_size: u64,
    ) -> Result<u64, AuthError> {
        let transaction = self
            .database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let expired =
            match expire_stale_auth_sessions(&transaction, datetime(now_unix)?, batch_size).await {
                Ok(expired) => {
                    transaction
                        .commit()
                        .await
                        .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                    expired
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(storage_error(error));
                }
            };
        for session_id in &expired {
            self.disconnect_session_after_commit(
                session_id,
                AuthSessionTerminationReason::SessionExpired,
            )
            .await;
        }
        u64::try_from(expired.len()).map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))
    }

    pub(crate) fn spawn_auth_maintenance(self: &Arc<Self>) {
        let service = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(AUTH_MAINTENANCE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Startup performs the first reconciliation synchronously before the
            // listener opens. The detached worker starts with the next tick.
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(service) = service.upgrade() else {
                    break;
                };
                let now_unix = match unix_timestamp_secs() {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::warn!(
                            event = "auth_maintenance_failed",
                            error = %error,
                            "failed to read the clock for auth maintenance"
                        );
                        continue;
                    }
                };
                match service
                    .expire_stale_sessions(now_unix, AUTH_MAINTENANCE_BATCH_SIZE)
                    .await
                {
                    Ok(expired_sessions) if expired_sessions > 0 => tracing::info!(
                        event = "auth_sessions_expired",
                        expired_sessions,
                        "stale auth sessions were expired"
                    ),
                    Ok(_) => {}
                    Err(error) => tracing::warn!(
                        event = "auth_maintenance_failed",
                        maintenance_kind = "session_expiry",
                        error_code = error.code().as_str(),
                        "stale auth session expiry failed"
                    ),
                }
                match service
                    .expire_pending_device_sessions(now_unix, AUTH_MAINTENANCE_BATCH_SIZE)
                    .await
                {
                    Ok(expired_sessions) if expired_sessions > 0 => tracing::info!(
                        event = "auth_pending_device_sessions_expired",
                        expired_sessions,
                        "expired pending device sessions marked terminal"
                    ),
                    Ok(_) => {}
                    Err(error) => tracing::warn!(
                        event = "auth_maintenance_failed",
                        maintenance_kind = "device_activation",
                        error_code = error.code().as_str(),
                        "pending device session expiry failed"
                    ),
                }
            }
        });
    }

    pub(crate) async fn list_sessions(
        &self,
        current: &AuthenticatedSessionPrincipal,
    ) -> Result<AuthSessionListResponse, AuthError> {
        self.validate_session_lease(current).await?;
        let rows = list_sessions_for_principal(&self.database, &current.principal_id)
            .await
            .map_err(storage_error)?;
        let mut sessions = Vec::with_capacity(rows.len());
        for row in rows {
            if row.gateway_id != current.gateway_id.as_str()
                || row.principal_id != current.principal_id.as_str()
            {
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
            let device_id = DeviceId::new(row.device_id.clone())
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            let device = load_device(&self.database, &device_id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
            if device.gateway_id != current.gateway_id.as_str()
                || device.principal_id != current.principal_id.as_str()
            {
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
            sessions.push(AuthSessionListItem {
                current: row.id == current.session_id.as_str(),
                last_seen_at_unix: timestamp(
                    row.last_seen_at
                        .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
                )?,
                device: device_snapshot(device)?,
                session: session_snapshot(row)?,
            });
        }
        Ok(AuthSessionListResponse { sessions })
    }

    pub(crate) async fn revoke_owned_session(
        &self,
        current: &AuthenticatedSessionPrincipal,
        target_id: &AuthSessionId,
        expected_status: Option<AuthSessionStatus>,
        reason: AuthSessionRevokeReason,
    ) -> Result<AuthSessionRevokeResponse, AuthError> {
        self.validate_session_lease(current).await?;
        let transaction = self
            .database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let result = async {
            let now = chrono::Utc::now().fixed_offset();
            self.validate_active_actor_in_transaction(&transaction, current, now)
                .await?;
            let row = load_session(&transaction, target_id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
            if row.gateway_id != current.gateway_id.as_str()
                || row.principal_id != current.principal_id.as_str()
            {
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
            if expected_status.is_some_and(|status| auth_session_status_to_db(status) != row.status)
            {
                return Ok(false);
            }
            let device_id = DeviceId::new(row.device_id.clone())
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            if row.status == "pending" {
                mark_session_revoked(&transaction, row, reason, now)
                    .await
                    .map_err(storage_error)?;
                mark_device_revoked(&transaction, &device_id, now)
                    .await
                    .map_err(storage_error)?;
                return Ok(true);
            }
            if row.status != "active" {
                // Session revocation is idempotent, but the device lifecycle
                // must still converge if an earlier attempt committed only the
                // terminal session state.
                mark_device_revoked(&transaction, &device_id, now)
                    .await
                    .map_err(storage_error)?;
                return Ok(false);
            }
            mark_session_revoked(&transaction, row, reason, now)
                .await
                .map_err(storage_error)?;
            delete_refresh_for_session(&transaction, target_id)
                .await
                .map_err(storage_error)?;
            mark_device_revoked(&transaction, &device_id, now)
                .await
                .map_err(storage_error)?;
            Ok(true)
        }
        .await;
        let revoked = match result {
            Ok(revoked) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                revoked
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };
        if revoked {
            tracing::info!(
                event = "auth_session_revoked",
                principal_id = %current.principal_id,
                device_id = %current.device_id,
                auth_session_id = %target_id,
                outcome = "revoked",
                reason = ?reason,
            );
        }
        Ok(AuthSessionRevokeResponse {
            session_id: target_id.clone(),
            revoked,
        })
    }

    pub(crate) async fn disconnect_committed_session(
        &self,
        session_id: &AuthSessionId,
        reason: AuthSessionTerminationReason,
    ) {
        self.disconnect_session_after_commit(session_id, reason)
            .await;
    }

    pub(crate) async fn logout(
        &self,
        current: &AuthenticatedSessionPrincipal,
    ) -> Result<AuthLogoutResponse, AuthError> {
        let result = self
            .revoke_owned_session(
                current,
                &current.session_id,
                None,
                AuthSessionRevokeReason::Logout,
            )
            .await?;
        Ok(AuthLogoutResponse {
            session_id: result.session_id,
            revoked: result.revoked,
        })
    }

    pub(crate) async fn create_device(
        &self,
        current: &AuthenticatedSessionPrincipal,
    ) -> Result<AuthDeviceCreateResponse, AuthError> {
        self.create_pending_device_session(
            PendingSessionIssuer::AuthenticatedSession(current.session_id.clone()),
            Some(current),
            PendingSessionIds::random()?,
        )
        .await
    }

    pub(crate) async fn create_local_device(&self) -> Result<AuthDeviceCreateResponse, AuthError> {
        self.create_pending_device_session(
            PendingSessionIssuer::LocalCli,
            None,
            PendingSessionIds::random()?,
        )
        .await
    }

    async fn create_pending_device_session(
        &self,
        issuer: PendingSessionIssuer,
        authenticated_actor: Option<&AuthenticatedSessionPrincipal>,
        ids: PendingSessionIds,
    ) -> Result<AuthDeviceCreateResponse, AuthError> {
        let now_unix =
            unix_timestamp_secs().map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let now = datetime(now_unix)?;
        let expires_at_unix = now_unix
            .checked_add(self.config.device_activation_code_ttl_seconds)
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let expires_at = datetime(expires_at_unix)?;
        let transaction = self
            .database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let result = async {
            let gateway = load_gateway_singleton(&transaction)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| AuthError::new(AuthErrorCode::AuthNotReady))?;
            if gateway.id != self.identity.gateway.id
                || gateway.auth_schema_version != AUTH_SCHEMA_VERSION
                || gateway.auth_ready_at.is_none()
            {
                return Err(AuthError::new(AuthErrorCode::AuthNotReady));
            }
            let principal = load_principal_by_id(&transaction, &self.identity.superuser.id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| AuthError::new(AuthErrorCode::AuthNotReady))?;
            if principal.gateway_id != self.identity.gateway.id
                || principal.kind != PrincipalKind::Superuser
                || principal.status != PrincipalStatus::Active
            {
                return Err(AuthError::new(AuthErrorCode::AuthNotReady));
            }
            match authenticated_actor {
                Some(current) => {
                    self.validate_active_actor_in_transaction(&transaction, current, now)
                        .await?;
                    if let Some(previous) =
                        load_pending_session_for_creator(&transaction, &current.session_id)
                            .await
                            .map_err(storage_error)?
                    {
                        let previous_device_id = DeviceId::new(previous.device_id.clone())
                            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                        mark_session_revoked(
                            &transaction,
                            previous,
                            AuthSessionRevokeReason::Superseded,
                            now,
                        )
                        .await
                        .map_err(storage_error)?;
                        mark_device_revoked(&transaction, &previous_device_id, now)
                            .await
                            .map_err(storage_error)?;
                    }
                }
                None => {
                    if let Some(previous) = load_pending_local_session(
                        &transaction,
                        &self.identity.gateway.id,
                        &self.identity.superuser.id,
                    )
                    .await
                    .map_err(storage_error)?
                    {
                        let previous_device_id = DeviceId::new(previous.device_id.clone())
                            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                        mark_session_revoked(
                            &transaction,
                            previous,
                            AuthSessionRevokeReason::Superseded,
                            now,
                        )
                        .await
                        .map_err(storage_error)?;
                        mark_device_revoked(&transaction, &previous_device_id, now)
                            .await
                            .map_err(storage_error)?;
                    }
                }
            }
            expire_stale_pending_sessions(
                &transaction,
                now,
                u64::try_from(DEVICE_ACTIVATION_ALPHABET.len())
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
            )
            .await
            .map_err(storage_error)?;
            let used_locator_hashes =
                list_pending_activation_locator_hashes(&transaction, &self.identity.gateway.id)
                    .await
                    .map_err(storage_error)?;
            if used_locator_hashes.len() >= DEVICE_ACTIVATION_ALPHABET.len() {
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
            let mut activation = self.opaque_credentials.generate_device_activation();
            let initial_locator = self
                .opaque_credentials
                .device_activation_locator(&activation)?;
            let initial_locator_hash = self
                .opaque_credentials
                .fingerprint_device_activation_locator(&activation)?;
            if used_locator_hashes.contains(initial_locator_hash.as_slice()) {
                let start = DEVICE_ACTIVATION_ALPHABET
                    .iter()
                    .position(|candidate| initial_locator.as_bytes().first() == Some(candidate))
                    .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
                let free_locator = (1..=DEVICE_ACTIVATION_ALPHABET.len())
                    .map(|offset| {
                        char::from(
                            DEVICE_ACTIVATION_ALPHABET
                                [(start + offset) % DEVICE_ACTIVATION_ALPHABET.len()],
                        )
                        .to_string()
                    })
                    .find(|candidate| {
                        self.opaque_credentials
                            .fingerprint_device_activation_locator_symbol(candidate.as_str())
                            .is_ok_and(|hash| !used_locator_hashes.contains(hash.as_slice()))
                    })
                    .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
                activation = self
                    .opaque_credentials
                    .generate_device_activation_for_locator(free_locator.as_str())?;
            }
            let activation_hash = self.opaque_credentials.fingerprint(&activation);
            let activation_locator_hash = self
                .opaque_credentials
                .fingerprint_device_activation_locator(&activation)?;
            insert_pending_device_session(
                &transaction,
                NewPendingDeviceSessionRow {
                    device_id: ids.device_id.clone(),
                    session_id: ids.session_id.clone(),
                    gateway_id: self.identity.gateway.id.clone(),
                    principal_id: self.identity.superuser.id.clone(),
                    token_family_id: ids.token_family_id.clone(),
                    issuer,
                    activation_token_hash: activation_hash,
                    activation_locator_hash,
                    activation_expires_at: expires_at,
                    now,
                },
            )
            .await
            .map_err(storage_error)?;
            Ok(activation)
        }
        .await;
        let activation = match result {
            Ok(activation) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                activation
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };
        tracing::info!(
            event = "auth_pending_device_session_created",
            device_id = %ids.device_id,
            auth_session_id = %ids.session_id,
            principal_id = %self.identity.superuser.id,
            issuer = if authenticated_actor.is_some() { "authenticated_session" } else { "local_cli" },
            outcome = "created",
        );
        Ok(AuthDeviceCreateResponse {
            device_id: ids.device_id,
            session_id: ids.session_id,
            activation_code: AuthSecretString::new(
                format_device_activation_code(activation.expose_for_exchange())
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
            ),
            expires_at_unix,
            gateway_id: self.identity.gateway.id.clone(),
        })
    }

    pub(crate) async fn activate_device(
        &self,
        admission: RestrictedAdmission,
        params: AuthDeviceActivateParams,
    ) -> Result<AuthSessionGrant, AuthError> {
        self.activate_device_with_ids(admission, params, IssuedCredentialIds::random()?)
            .await
    }

    async fn activate_device_with_ids(
        &self,
        admission: RestrictedAdmission,
        mut params: AuthDeviceActivateParams,
        issued_ids: IssuedCredentialIds,
    ) -> Result<AuthSessionGrant, AuthError> {
        let admission_gateway_id = match admission.context() {
            RestrictedAuthContext::DeviceActivation(context) => context.gateway_id.clone(),
            RestrictedAuthContext::Refresh(_) => {
                return Err(AuthError::new(AuthErrorCode::MethodNotAllowed));
            }
        };
        validate_device_activation_params(&mut params)?;
        if admission_gateway_id != self.identity.gateway.id {
            return Err(AuthError::new(AuthErrorCode::GatewayIdentityMismatch));
        }
        let activation_hash = self
            .opaque_credentials
            .fingerprint_device_activation_raw(admission.credential().expose_for_authentication());
        let activation_locator_hash = self
            .opaque_credentials
            .fingerprint_device_activation_locator_raw(
                admission.credential().expose_for_authentication(),
            )?;
        let now_unix =
            unix_timestamp_secs().map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let now = datetime(now_unix)?;
        let refresh_expires_at_unix = now_unix
            .checked_add(self.config.refresh_token_ttl_seconds)
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let access_expires_at_unix = now_unix
            .checked_add(self.config.access_token_ttl_seconds)
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let refresh_expires_at = datetime(refresh_expires_at_unix)?;
        let transaction = self
            .database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let exact_session =
            load_session_by_activation_hash(&transaction, &admission_gateway_id, &activation_hash)
                .await
                .map_err(storage_error)?;
        let exact_pending_match = exact_session
            .as_ref()
            .is_some_and(|session| session.status == "pending");
        let terminal_exact_match = exact_session
            .as_ref()
            .is_some_and(|session| session.status != "pending");
        let pending_session = match exact_session {
            Some(session) if session.status == "pending" => session,
            _ => match load_pending_session_by_activation_locator_hash(
                &transaction,
                &admission_gateway_id,
                &activation_locator_hash,
            )
            .await
            .map_err(storage_error)?
            {
                Some(session) => session,
                None => {
                    let _ = transaction.rollback().await;
                    return Err(AuthError::new(if terminal_exact_match {
                        AuthErrorCode::DeviceActivationConsumed
                    } else {
                        AuthErrorCode::InvalidCredential
                    }));
                }
            },
        };
        if pending_session.gateway_id != admission_gateway_id.as_str() {
            let _ = transaction.rollback().await;
            return Err(AuthError::new(AuthErrorCode::GatewayIdentityMismatch));
        }
        if pending_session.status != "pending" {
            let _ = transaction.rollback().await;
            return Err(AuthError::new(AuthErrorCode::DeviceActivationConsumed));
        }
        let session_id = AuthSessionId::new(pending_session.id.clone())
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let device_id = DeviceId::new(pending_session.device_id.clone())
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        if now >= pending_session.activation_expires_at {
            expire_pending_auth_session(&transaction, &session_id, &device_id, now)
                .await
                .map_err(storage_error)?;
            transaction
                .commit()
                .await
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            tracing::info!(
                event = "auth_device_activation_expired",
                auth_session_id = %session_id,
                device_id = %device_id,
                outcome = "expired",
            );
            return Err(AuthError::new(if exact_pending_match {
                AuthErrorCode::DeviceActivationExpired
            } else {
                AuthErrorCode::InvalidCredential
            }));
        }
        let hash_matches: bool = pending_session
            .activation_token_hash
            .as_slice()
            .ct_eq(activation_hash.as_slice())
            .into();
        if !hash_matches {
            let outcome = record_failed_device_activation(&transaction, pending_session, now)
                .await
                .map_err(storage_error)?;
            transaction
                .commit()
                .await
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            let (failed_attempts, request_revoked) = match outcome {
                DeviceActivationFailureOutcome::AttemptRecorded { failed_attempts } => {
                    (failed_attempts, false)
                }
                DeviceActivationFailureOutcome::RequestRevoked { failed_attempts } => {
                    (failed_attempts, true)
                }
            };
            tracing::warn!(
                event = "auth_device_activation_rejected",
                auth_session_id = %session_id,
                device_id = %device_id,
                failed_attempts,
                request_revoked,
                outcome = "invalid_credential",
            );
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        let token_family_id = TokenFamilyId::new(pending_session.token_family_id.clone())
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
        let refresh = self.opaque_credentials.generate_refresh(
            &session_id,
            &token_family_id,
            0,
            refresh_expires_at_unix,
        );
        let refresh_hash = self.opaque_credentials.fingerprint(&refresh);
        let result = async {
            let gateway_id = pioneer_protocol::GatewayId::new(pending_session.gateway_id.clone())
                .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            let principal_id =
                pioneer_protocol::PrincipalId::new(pending_session.principal_id.clone())
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
            if let Some(creator_session_id) = pending_session.created_by_session_id.as_deref() {
                let creator_session_id = AuthSessionId::new(creator_session_id.to_owned())
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                let creator = load_session(&transaction, &creator_session_id)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| AuthError::new(AuthErrorCode::SessionRevoked))?;
                if creator.status != "active"
                    || creator.gateway_id != gateway_id.as_str()
                    || creator.principal_id != principal_id.as_str()
                    || creator
                        .refresh_expires_at
                        .is_none_or(|expires_at| now >= expires_at)
                {
                    return Err(AuthError::new(AuthErrorCode::SessionRevoked));
                }
                let creator_device_id = DeviceId::new(creator.device_id)
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                let creator_device = load_device(&transaction, &creator_device_id)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| AuthError::new(AuthErrorCode::SessionRevoked))?;
                if creator_device.status != "active"
                    || creator_device.gateway_id != gateway_id.as_str()
                    || creator_device.principal_id != principal_id.as_str()
                {
                    return Err(AuthError::new(AuthErrorCode::SessionRevoked));
                }
            }
            let principal = load_principal_by_id(&transaction, &principal_id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
            if principal.gateway_id != gateway_id
                || principal.kind != PrincipalKind::Superuser
                || principal.status != PrincipalStatus::Active
            {
                return Err(AuthError::new(AuthErrorCode::InvalidCredential));
            }
            let existing_device = load_active_device_by_installation(
                &transaction,
                &gateway_id,
                &principal_id,
                params.installation.installation_id.as_str(),
            )
            .await
            .map_err(storage_error)?;
            let mut superseded_session_id = None;
            if let Some(existing_device) = existing_device {
                let existing_device_id = DeviceId::new(existing_device.id)
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                if let Some(existing_session) =
                    load_active_session_by_device(&transaction, &existing_device_id)
                        .await
                        .map_err(storage_error)?
                {
                    let existing_session_id = AuthSessionId::new(existing_session.id.clone())
                        .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                    mark_session_revoked(
                        &transaction,
                        existing_session,
                        AuthSessionRevokeReason::Superseded,
                        now,
                    )
                    .await
                    .map_err(storage_error)?;
                    delete_refresh_for_session(&transaction, &existing_session_id)
                        .await
                        .map_err(storage_error)?;
                    superseded_session_id = Some(existing_session_id);
                }
                mark_device_revoked(&transaction, &existing_device_id, now)
                    .await
                    .map_err(storage_error)?;
            }
            let pending_device = load_device(&transaction, &device_id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?;
            if pending_device.status != "pending"
                || pending_device.gateway_id != gateway_id.as_str()
                || pending_device.principal_id != principal_id.as_str()
            {
                return Err(AuthError::new(AuthErrorCode::DeviceActivationConsumed));
            }
            let device =
                activate_pending_device(&transaction, &device_id, &params.installation, now)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| AuthError::new(AuthErrorCode::DeviceActivationConsumed))?;
            self.inject_activation_failure(1)?;
            activate_pending_auth_session(&transaction, &session_id, refresh_expires_at, now)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| AuthError::new(AuthErrorCode::DeviceActivationConsumed))?;
            self.inject_activation_failure(2)?;
            insert_refresh_credential(
                &transaction,
                NewRefreshCredentialRow {
                    id: issued_ids.refresh_id,
                    session_id: session_id.clone(),
                    token_family_id: token_family_id.clone(),
                    generation: 0,
                    token_hash: refresh_hash,
                    issued_at: now,
                    expires_at: refresh_expires_at,
                },
            )
            .await
            .map_err(storage_error)?;
            self.inject_activation_failure(3)?;
            let access_token = self.access_issuer.issue(
                &AccessJwtSubject {
                    gateway_id: gateway_id.clone(),
                    principal_id: principal_id.clone(),
                    device_id: device_id.clone(),
                    session_id: session_id.clone(),
                },
                now_unix,
                Some(issued_ids.access_jti.clone()),
            )?;
            self.inject_activation_failure(4)?;
            Ok((
                gateway_id,
                principal_id,
                principal,
                device,
                superseded_session_id,
                access_token,
            ))
        }
        .await;
        let (gateway_id, principal_id, principal, device, superseded_session_id, access_token) =
            match result {
                Ok(value) => {
                    transaction
                        .commit()
                        .await
                        .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
                    value
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };
        if let Some(session_id) = superseded_session_id.as_ref() {
            self.disconnect_session_after_commit(
                session_id,
                AuthSessionTerminationReason::SessionRevoked,
            )
            .await;
        }
        tracing::info!(
            event = "auth_device_session_activated",
            principal_id = %principal_id,
            device_id = %device_id,
            auth_session_id = %session_id,
            outcome = "activated",
        );
        Ok(AuthSessionGrant {
            gateway: AuthGatewaySnapshot { id: gateway_id },
            principal: AuthPrincipalSnapshot {
                id: principal_id,
                kind: principal.kind,
                display_name: principal.display_name,
                nickname: principal.nickname,
            },
            device: AuthDeviceSnapshot {
                id: device_id.clone(),
                installation_id: device
                    .installation_id
                    .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
                display_name: device
                    .display_name
                    .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
                client_kind: params.installation.client_kind,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: session_id,
                device_id,
                token_family_id,
                status: AuthSessionStatus::Active,
                refresh_generation: 0,
                refresh_expires_at_unix,
            },
            access_token: AuthSecretString::new(access_token),
            access_expires_at_unix,
            refresh_token: AuthSecretString::new(refresh.expose_for_exchange().to_owned()),
            refresh_expires_at_unix,
            refresh_generation: 0,
            auth_protocol_version: pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION,
            credential_storage_order: CredentialStorageOrder::PersistRefreshBeforeActivatingAccess,
        })
    }

    #[cfg(test)]
    async fn create_initial_session_with_ids(
        &self,
        params: AuthDeviceActivateParams,
        ids: SessionGrantIds,
    ) -> Result<AuthSessionGrant, AuthError> {
        let pending = self
            .create_pending_device_session(
                PendingSessionIssuer::LocalCli,
                None,
                PendingSessionIds {
                    device_id: ids.device_id,
                    session_id: ids.session_id,
                    token_family_id: ids.token_family_id,
                },
            )
            .await?;
        let credential =
            super::PresentedCredential::classify(pending.activation_code.expose_secret())?;
        self.activate_device_with_ids(
            RestrictedAdmission::new(
                credential,
                RestrictedAuthContext::DeviceActivation(super::DeviceActivationContext {
                    gateway_id: pending.gateway_id,
                }),
            ),
            params,
            IssuedCredentialIds {
                refresh_id: ids.refresh_id,
                access_jti: ids.access_jti,
            },
        )
        .await
    }

    async fn validate_active_actor_in_transaction(
        &self,
        transaction: &DatabaseTransaction,
        current: &AuthenticatedSessionPrincipal,
        now: DateTime<FixedOffset>,
    ) -> Result<(), AuthError> {
        if current.gateway_id != self.identity.gateway.id {
            return Err(AuthError::new(AuthErrorCode::GatewayIdentityMismatch));
        }
        let now_unix = timestamp(now)?;
        if now_unix >= current.access_expires_at_unix {
            return Err(AuthError::new(AuthErrorCode::CredentialExpired));
        }
        let session = load_session(transaction, &current.session_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| AuthError::new(AuthErrorCode::SessionRevoked))?;
        if session.status == "expired"
            || session
                .refresh_expires_at
                .is_none_or(|expires_at| now >= expires_at)
        {
            return Err(AuthError::new(AuthErrorCode::SessionExpired));
        }
        if session.status != "active"
            || session.gateway_id != current.gateway_id.as_str()
            || session.principal_id != current.principal_id.as_str()
            || session.device_id != current.device_id.as_str()
        {
            return Err(AuthError::new(AuthErrorCode::SessionRevoked));
        }
        let device = load_device(transaction, &current.device_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| AuthError::new(AuthErrorCode::SessionRevoked))?;
        if device.status != "active"
            || device.gateway_id != current.gateway_id.as_str()
            || device.principal_id != current.principal_id.as_str()
        {
            return Err(AuthError::new(AuthErrorCode::SessionRevoked));
        }
        let owner = load_principal_by_id(transaction, &current.principal_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| AuthError::new(AuthErrorCode::SessionRevoked))?;
        if owner.gateway_id != current.gateway_id
            || owner.status != PrincipalStatus::Active
            || owner.kind != PrincipalKind::Superuser
        {
            return Err(AuthError::new(AuthErrorCode::SessionRevoked));
        }
        Ok(())
    }

    #[cfg(not(test))]
    fn inject_activation_failure(&self, _step: u8) -> Result<(), AuthError> {
        Ok(())
    }

    #[cfg(test)]
    fn inject_activation_failure(&self, step: u8) -> Result<(), AuthError> {
        use std::sync::atomic::Ordering;
        if self.activation_failpoint.load(Ordering::SeqCst) == step {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        Ok(())
    }

    #[cfg(test)]
    fn set_activation_failpoint(&self, step: u8) {
        use std::sync::atomic::Ordering;
        self.activation_failpoint.store(step, Ordering::SeqCst);
    }

    #[cfg(not(test))]
    fn inject_refresh_failure(&self, _step: u8) -> Result<(), AuthError> {
        Ok(())
    }

    #[cfg(test)]
    fn inject_refresh_failure(&self, step: u8) -> Result<(), AuthError> {
        use std::sync::atomic::Ordering;
        if self.refresh_failpoint.load(Ordering::SeqCst) == step {
            return Err(AuthError::new(AuthErrorCode::InvalidCredential));
        }
        Ok(())
    }

    #[cfg(test)]
    fn set_refresh_failpoint(&self, step: u8) {
        use std::sync::atomic::Ordering;
        self.refresh_failpoint.store(step, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub(crate) struct SessionLeaseSnapshot {
    kind: PrincipalKind,
    role_key: Option<String>,
    me: AuthMeResponse,
}

#[async_trait]
impl RestrictedExchangeExecutor for GatewayAuthService {
    async fn execute(
        &self,
        admission: RestrictedAdmission,
        request: pioneer_protocol::JsonRpcRequest,
    ) -> Result<JsonValue, AuthError> {
        let params = request.params.unwrap_or_else(|| serde_json::json!({}));
        match request.method.as_str() {
            AUTH_REFRESH => {
                let params: AuthRefreshParams = serde_json::from_value(params)
                    .map_err(|_| AuthError::new(AuthErrorCode::MalformedCredential))?;
                to_value(self.exchange_refresh(admission, params).await?)
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))
            }
            AUTH_DEVICE_ACTIVATE => {
                let params: AuthDeviceActivateParams = serde_json::from_value(params)
                    .map_err(|_| AuthError::new(AuthErrorCode::MalformedCredential))?;
                to_value(self.activate_device(admission, params).await?)
                    .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))
            }
            _ => Err(AuthError::new(AuthErrorCode::MethodNotAllowed)),
        }
    }
}

fn validate_device_activation_params(
    params: &mut AuthDeviceActivateParams,
) -> Result<(), AuthError> {
    validate_installation(&mut params.installation)
}

fn validate_installation(installation: &mut ClientInstallationDescriptor) -> Result<(), AuthError> {
    installation.installation_id =
        bounded_trimmed(installation.installation_id.as_str(), MAX_INSTALLATION_TEXT)?;
    installation.display_name =
        bounded_trimmed(installation.display_name.as_str(), MAX_INSTALLATION_TEXT)?;
    installation.platform = bounded_optional(installation.platform.take(), MAX_DIAGNOSTIC_TEXT)?;
    installation.client_version =
        bounded_optional(installation.client_version.take(), MAX_DIAGNOSTIC_TEXT)?;
    Ok(())
}

fn bounded_trimmed(value: &str, max_chars: usize) -> Result<String, AuthError> {
    let trimmed = value.trim();
    let count = trimmed.chars().count();
    if count == 0 || count > max_chars || trimmed.chars().any(char::is_control) {
        return Err(AuthError::new(AuthErrorCode::MalformedCredential));
    }
    Ok(trimmed.to_owned())
}

fn bounded_optional(value: Option<String>, max_chars: usize) -> Result<Option<String>, AuthError> {
    value
        .map(|value| bounded_trimmed(value.as_str(), max_chars))
        .transpose()
}

fn datetime(unix: u64) -> Result<DateTime<FixedOffset>, AuthError> {
    let unix = i64::try_from(unix).map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;
    DateTime::<Utc>::from_timestamp(unix, 0)
        .map(|value| value.fixed_offset())
        .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))
}

fn storage_error(_error: anyhow::Error) -> AuthError {
    AuthError::new(AuthErrorCode::InvalidCredential)
}

fn client_kind_from_db(value: &str) -> Result<ClientKind, AuthError> {
    match value {
        "desktop" => Ok(ClientKind::Desktop),
        "mobile" => Ok(ClientKind::Mobile),
        "other" => Ok(ClientKind::Other),
        _ => Err(AuthError::new(AuthErrorCode::InvalidCredential)),
    }
}

fn device_snapshot(device: pioneer_entity::device::Model) -> Result<AuthDeviceSnapshot, AuthError> {
    Ok(AuthDeviceSnapshot {
        id: DeviceId::new(device.id)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
        installation_id: device
            .installation_id
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
        display_name: device
            .display_name
            .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
        client_kind: client_kind_from_db(
            device
                .client_kind
                .as_deref()
                .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
        )?,
        status: match device.status.as_str() {
            "active" => DeviceStatus::Active,
            "revoked" => DeviceStatus::Revoked,
            _ => return Err(AuthError::new(AuthErrorCode::InvalidCredential)),
        },
    })
}

fn session_snapshot(
    session: pioneer_entity::auth_session::Model,
) -> Result<AuthSessionSnapshot, AuthError> {
    Ok(AuthSessionSnapshot {
        id: AuthSessionId::new(session.id)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
        device_id: DeviceId::new(session.device_id)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
        token_family_id: TokenFamilyId::new(session.token_family_id)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
        status: match session.status.as_str() {
            "active" => AuthSessionStatus::Active,
            "revoked" => AuthSessionStatus::Revoked,
            "expired" => AuthSessionStatus::Expired,
            _ => return Err(AuthError::new(AuthErrorCode::InvalidCredential)),
        },
        refresh_generation: u64::try_from(session.refresh_generation)
            .map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?,
        refresh_expires_at_unix: timestamp(
            session
                .refresh_expires_at
                .ok_or_else(|| AuthError::new(AuthErrorCode::InvalidCredential))?,
        )?,
    })
}

fn timestamp(value: DateTime<FixedOffset>) -> Result<u64, AuthError> {
    u64::try_from(value.timestamp()).map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{ConnectionTrait, Database, Statement};
    use std::sync::Mutex;

    use super::*;
    use crate::auth::PresentedCredential;
    use crate::bootstrap::bootstrap as bootstrap_workspace;
    use crate::identity::bootstrap_identity;
    use crate::secrets::AuthKeyMaterial;

    async fn fixture() -> (GatewayAuthService, Arc<IdentityBootstrapSnapshot>) {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        bootstrap_workspace(&database).await.unwrap();
        let identity = Arc::new(bootstrap_identity(&database).await.unwrap().snapshot);
        database
            .execute_unprepared(
                format!(
                    "UPDATE gateway_identity SET auth_schema_version = {AUTH_SCHEMA_VERSION}, auth_ready_at = CURRENT_TIMESTAMP"
                )
                .as_str(),
            )
            .await
            .unwrap();
        let service = GatewayAuthService::new(
            database,
            GatewayAuthConfig::default(),
            identity.clone(),
            &AuthKeyMaterial::from_test_bytes(vec![8; 64]),
            &AuthKeyMaterial::from_test_bytes(vec![9; 64]),
        )
        .unwrap();
        (service, identity)
    }

    #[tokio::test]
    async fn local_device_creation_is_one_use_and_preserves_the_superuser() {
        let (service, identity) = fixture().await;
        let created = service.create_local_device().await.unwrap();
        let raw = created.activation_code.expose_secret().to_owned();
        let grant = service
            .activate_device_with_ids(
                device_activation_admission(&service, &raw),
                params("desktop-installation", ClientKind::Desktop),
                issued_ids(1),
            )
            .await
            .unwrap();
        assert_eq!(grant.principal.id, identity.superuser.id);
        assert_eq!(grant.device.installation_id, "desktop-installation");
        assert_eq!(grant.refresh_generation, 0);
        assert!(
            grant
                .refresh_token
                .expose_secret()
                .starts_with(pioneer_protocol::REFRESH_CREDENTIAL_PREFIX)
        );
        assert!(!format!("{grant:?}").contains(grant.refresh_token.expose_secret()));

        let replay = service
            .activate_device_with_ids(
                device_activation_admission(&service, &raw),
                params("different-installation", ClientKind::Mobile),
                issued_ids(2),
            )
            .await
            .unwrap_err();
        assert_eq!(replay.code(), AuthErrorCode::DeviceActivationConsumed);

        assert_eq!(count(&service.database, "gateway_principal").await, 1);
        assert_eq!(count(&service.database, "device").await, 1);
        assert_eq!(count(&service.database, "auth_session").await, 1);
        assert_eq!(count(&service.database, "auth_refresh_credential").await, 1);
        let raw_refresh = grant.refresh_token.expose_secret();
        let raw_access = grant.access_token.expose_secret();
        let dump = database_text(&service.database).await;
        assert!(!dump.contains(raw_refresh));
        assert!(!dump.contains(raw_access));
    }

    #[tokio::test]
    async fn injected_activation_failure_preserves_only_the_pending_device_session() {
        for step in 1..=4 {
            let (service, _identity) = fixture().await;
            service.set_activation_failpoint(step);
            let error = service
                .create_initial_session_with_ids(
                    params("desktop-installation", ClientKind::Desktop),
                    ids(1),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), AuthErrorCode::InvalidCredential);
            assert_eq!(count(&service.database, "device").await, 1, "step={step}");
            assert_eq!(
                count_where(&service.database, "device", "status = 'pending'").await,
                1,
                "step={step}"
            );
            assert_eq!(
                count(&service.database, "auth_session").await,
                1,
                "step={step}"
            );
            assert_eq!(
                count_where(&service.database, "auth_session", "status = 'pending'").await,
                1,
                "step={step}"
            );
            assert_eq!(
                count(&service.database, "auth_refresh_credential").await,
                0,
                "step={step}"
            );
        }
    }

    #[tokio::test]
    async fn access_requires_exact_active_persisted_ownership_and_builds_session_identity() {
        let (service, identity) = fixture().await;
        let grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let now = unix_timestamp_secs().unwrap();
        let credential = service
            .access_issuer
            .validate(grant.access_token.expose_secret(), now)
            .unwrap();
        let principal = service
            .authenticate_access(credential.clone())
            .await
            .unwrap();
        assert_eq!(principal.gateway_id, identity.gateway.id);
        assert_eq!(principal.principal_id, identity.superuser.id);
        assert_eq!(principal.device_id, grant.device.id);
        assert_eq!(principal.session_id, grant.session.id);
        assert_eq!(principal.kind, PrincipalKind::Superuser);
        let me = service.auth_me(principal.as_ref()).await.unwrap();
        assert_eq!(me.gateway.id, identity.gateway.id);
        assert_eq!(me.principal.id, identity.superuser.id);
        assert_eq!(me.device.id, grant.device.id);
        assert_eq!(me.session.id, grant.session.id);
        assert_eq!(me.session.refresh_generation, 0);

        let forged_subjects = [
            AccessJwtSubject {
                principal_id: pioneer_protocol::PrincipalId::new("P00000000000000000099").unwrap(),
                ..credential.subject.clone()
            },
            AccessJwtSubject {
                device_id: DeviceId::new("D00000000000000000099").unwrap(),
                ..credential.subject.clone()
            },
            AccessJwtSubject {
                session_id: AuthSessionId::new("S00000000000000000099").unwrap(),
                ..credential.subject.clone()
            },
            AccessJwtSubject {
                gateway_id: pioneer_protocol::GatewayId::new("G00000000000000000099").unwrap(),
                ..credential.subject.clone()
            },
        ];
        for subject in forged_subjects {
            let forged = AccessCredential {
                subject,
                jti: credential.jti.clone(),
                issued_at_unix: credential.issued_at_unix,
                expires_at_unix: credential.expires_at_unix,
            };
            assert!(
                service.authenticate_access(forged).await.is_err(),
                "persisted session ownership must reject forged access identity"
            );
        }

        service
            .database
            .execute_unprepared(
                "UPDATE auth_session SET status = 'revoked', revoked_at = CURRENT_TIMESTAMP, revoke_reason = 'self_revoke'; \
                 DELETE FROM auth_refresh_credential",
            )
            .await
            .unwrap();
        assert_eq!(
            service
                .validate_session_lease(principal.as_ref())
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::SessionRevoked
        );
        assert_eq!(
            service
                .exchange_refresh(
                    refresh_admission(grant.refresh_token.expose_secret()),
                    refresh_params(1),
                )
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::SessionRevoked
        );
        assert_eq!(
            scalar_text(
                &service.database,
                "SELECT revoke_reason AS value FROM auth_session"
            )
            .await,
            "self_revoke"
        );
        assert_eq!(
            scalar_text(&service.database, "SELECT status AS value FROM device").await,
            "active"
        );
    }

    #[tokio::test]
    async fn cryptographically_shaped_access_without_matching_rows_is_rejected() {
        let (service, identity) = fixture().await;
        let now = unix_timestamp_secs().unwrap();
        let credential = AccessCredential {
            subject: AccessJwtSubject {
                gateway_id: identity.gateway.id.clone(),
                principal_id: identity.superuser.id.clone(),
                device_id: DeviceId::new("D00000000000000000099").unwrap(),
                session_id: AuthSessionId::new("S00000000000000000099").unwrap(),
            },
            jti: "J00000000000000000099".to_owned(),
            issued_at_unix: now,
            expires_at_unix: now + 60,
        };
        assert_eq!(
            service
                .authenticate_access(credential)
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::InvalidCredential
        );
    }

    #[tokio::test]
    async fn persisted_access_survives_auth_service_restart_with_same_key_domains() {
        let (service, identity) = fixture().await;
        let grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let token = grant.access_token.expose_secret().to_owned();
        let database = service.database.clone();
        drop(service);
        let restarted = GatewayAuthService::new(
            database,
            GatewayAuthConfig::default(),
            identity.clone(),
            &AuthKeyMaterial::from_test_bytes(vec![8; 64]),
            &AuthKeyMaterial::from_test_bytes(vec![9; 64]),
        )
        .unwrap();
        let credential = restarted
            .access_issuer
            .validate(token.as_str(), unix_timestamp_secs().unwrap())
            .unwrap();
        let principal = restarted.authenticate_access(credential).await.unwrap();
        assert_eq!(principal.principal_id, identity.superuser.id);
        assert_eq!(principal.session_id, grant.session.id);
    }

    #[tokio::test]
    async fn access_key_rotation_requires_refresh_without_losing_the_session() {
        let (service, identity) = fixture().await;
        let grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let old_access = grant.access_token.expose_secret().to_owned();
        let refresh = grant.refresh_token.expose_secret().to_owned();
        let session_id = grant.session.id.clone();
        let device_id = grant.device.id.clone();
        let database = service.database.clone();
        drop(service);

        let rotated = GatewayAuthService::new(
            database,
            GatewayAuthConfig::default(),
            identity.clone(),
            &AuthKeyMaterial::from_test_bytes(vec![7; 64]),
            &AuthKeyMaterial::from_test_bytes(vec![9; 64]),
        )
        .unwrap();
        assert_eq!(
            rotated
                .access_issuer
                .validate(old_access.as_str(), unix_timestamp_secs().unwrap())
                .unwrap_err()
                .code(),
            AuthErrorCode::InvalidCredential
        );

        let refreshed = rotated
            .exchange_refresh(refresh_admission(&refresh), refresh_params(1))
            .await
            .unwrap();
        assert_eq!(refreshed.session.id, session_id);
        assert_eq!(refreshed.device.id, device_id);
        let credential = rotated
            .access_issuer
            .validate(
                refreshed.access_token.expose_secret(),
                unix_timestamp_secs().unwrap(),
            )
            .unwrap();
        let principal = rotated.authenticate_access(credential).await.unwrap();
        assert_eq!(principal.principal_id, identity.superuser.id);
        assert_eq!(principal.session_id, session_id);
    }

    #[tokio::test]
    async fn populated_epic2_fixture_accepts_independent_device_activations() {
        let populated = crate::auth::test_support::populated_epic2_database().await;
        Migrator::up(&populated.database, None).await.unwrap();
        crate::auth::ensure_auth_readiness(&populated.database, &populated.identity)
            .await
            .unwrap();
        let identity = Arc::new(populated.identity);
        let service = GatewayAuthService::new(
            populated.database,
            GatewayAuthConfig::default(),
            identity.clone(),
            &AuthKeyMaterial::from_test_bytes(vec![8; 64]),
            &AuthKeyMaterial::from_test_bytes(vec![9; 64]),
        )
        .unwrap();
        service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        service
            .create_initial_session_with_ids(
                params("mobile-installation", ClientKind::Mobile),
                ids(2),
            )
            .await
            .unwrap();
        assert_eq!(count(&service.database, "gateway_principal").await, 1);
        assert_eq!(count(&service.database, "device").await, 2);
        assert_eq!(count(&service.database, "auth_session").await, 2);
    }

    #[tokio::test]
    async fn populated_epic2_two_client_rollout_survives_restart_and_independent_revoke() {
        let populated = crate::auth::test_support::populated_epic2_database().await;
        Migrator::up(&populated.database, None).await.unwrap();
        crate::auth::ensure_auth_readiness(&populated.database, &populated.identity)
            .await
            .unwrap();
        let identity = Arc::new(populated.identity);
        let service = GatewayAuthService::new(
            populated.database,
            GatewayAuthConfig::default(),
            identity.clone(),
            &AuthKeyMaterial::from_test_bytes(vec![8; 64]),
            &AuthKeyMaterial::from_test_bytes(vec![9; 64]),
        )
        .unwrap();
        let desktop = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let mobile = service
            .create_initial_session_with_ids(
                params("mobile-installation", ClientKind::Mobile),
                ids(2),
            )
            .await
            .unwrap();
        let desktop = service
            .exchange_refresh(
                refresh_admission(desktop.refresh_token.expose_secret()),
                refresh_params(1),
            )
            .await
            .unwrap();
        let mobile = service
            .exchange_refresh(
                refresh_admission(mobile.refresh_token.expose_secret()),
                refresh_params(2),
            )
            .await
            .unwrap();
        let database = service.database.clone();
        drop(service);

        let restarted = GatewayAuthService::new(
            database,
            GatewayAuthConfig::default(),
            identity.clone(),
            &AuthKeyMaterial::from_test_bytes(vec![8; 64]),
            &AuthKeyMaterial::from_test_bytes(vec![9; 64]),
        )
        .unwrap();
        let desktop_principal = restarted
            .authenticate_access(
                restarted
                    .access_issuer
                    .validate(
                        desktop.access_token.expose_secret(),
                        unix_timestamp_secs().unwrap(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
        let mobile_principal = restarted
            .authenticate_access(
                restarted
                    .access_issuer
                    .validate(
                        mobile.access_token.expose_secret(),
                        unix_timestamp_secs().unwrap(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();

        let device_activation = restarted
            .create_device(desktop_principal.as_ref())
            .await
            .unwrap();
        let paired = restarted
            .activate_device_with_ids(
                device_activation_admission(
                    &restarted,
                    device_activation.activation_code.expose_secret(),
                ),
                device_activation_params("paired-mobile-installation"),
                issued_ids(3),
            )
            .await
            .unwrap();
        assert_eq!(paired.principal.id, identity.superuser.id);
        assert_ne!(paired.device.id, desktop.device.id);
        assert_ne!(paired.session.id, desktop.session.id);

        let revoked = restarted
            .revoke_owned_session(
                desktop_principal.as_ref(),
                &mobile.session.id,
                None,
                AuthSessionRevokeReason::DeviceRevoke,
            )
            .await
            .unwrap();
        assert!(revoked.revoked);
        assert_eq!(
            restarted
                .validate_session_lease(mobile_principal.as_ref())
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::SessionRevoked
        );
        restarted
            .validate_session_lease(desktop_principal.as_ref())
            .await
            .unwrap();
        let paired_principal = authenticate_grant(&restarted, &paired).await;
        restarted
            .validate_session_lease(paired_principal.as_ref())
            .await
            .unwrap();
        assert_eq!(count(&restarted.database, "gateway_principal").await, 1);
        assert_eq!(count(&restarted.database, "device").await, 3);
        assert_eq!(
            count_where(&restarted.database, "auth_session", "status = 'active'").await,
            2
        );
    }

    #[tokio::test]
    async fn refresh_rotates_sequentially_with_exactly_one_current_generation() {
        let (service, _) = fixture().await;
        let initial_grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let mut raw = initial_grant.refresh_token.expose_secret().to_owned();
        let mut last_expiry = initial_grant.refresh_expires_at_unix;
        for generation in 1..=100 {
            let rotated = service
                .exchange_refresh(refresh_admission(raw.as_str()), refresh_params(generation))
                .await
                .unwrap();
            assert_eq!(rotated.refresh_generation, generation);
            assert!(rotated.refresh_expires_at_unix >= last_expiry);
            raw = rotated.refresh_token.expose_secret().to_owned();
            last_expiry = rotated.refresh_expires_at_unix;
        }
        assert_eq!(count(&service.database, "auth_refresh_credential").await, 1);
        assert_eq!(
            scalar_i64(
                &service.database,
                "SELECT generation AS value FROM auth_refresh_credential"
            )
            .await,
            100
        );
        assert!(
            pioneer_crud::scan_auth_persistence_invariants(&service.database)
                .await
                .unwrap()
                .is_valid()
        );
    }

    #[tokio::test]
    async fn refresh_write_failures_restore_previous_current_generation() {
        for step in 1..=3 {
            let (service, _) = fixture().await;
            let initial_grant = service
                .create_initial_session_with_ids(
                    params("desktop-installation", ClientKind::Desktop),
                    ids(1),
                )
                .await
                .unwrap();
            service.set_refresh_failpoint(step);
            let error = service
                .exchange_refresh(
                    refresh_admission(initial_grant.refresh_token.expose_secret()),
                    refresh_params(1),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), AuthErrorCode::InvalidCredential);
            assert_eq!(count(&service.database, "auth_refresh_credential").await, 1);
            assert_eq!(
                scalar_i64(
                    &service.database,
                    "SELECT generation AS value FROM auth_refresh_credential"
                )
                .await,
                0
            );
            assert_eq!(
                scalar_i64(
                    &service.database,
                    "SELECT refresh_generation AS value FROM auth_session"
                )
                .await,
                0
            );
        }
    }

    #[derive(Default)]
    struct RecordingDisconnectHook(Mutex<Vec<(String, AuthSessionTerminationReason)>>);

    #[async_trait]
    impl AuthSessionDisconnectHook for RecordingDisconnectHook {
        async fn disconnect_session(
            &self,
            session_id: &AuthSessionId,
            reason: AuthSessionTerminationReason,
        ) {
            self.0
                .lock()
                .unwrap()
                .push((session_id.to_string(), reason));
        }
    }

    #[tokio::test]
    async fn expired_refresh_expires_session_and_deletes_current_credential() {
        let (service, _) = fixture().await;
        let hook = Arc::new(RecordingDisconnectHook::default());
        service.set_disconnect_hook(hook.clone());
        let initial_grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let principal = authenticate_grant(&service, &initial_grant).await;
        service
            .database
            .execute_unprepared(
                "UPDATE auth_session SET created_at = datetime('now', '-2 days'), refresh_expires_at = datetime('now', '-1 day'); \
                 UPDATE auth_refresh_credential SET issued_at = datetime('now', '-2 days'), expires_at = datetime('now', '-1 day')",
            )
            .await
            .unwrap();

        let error = service
            .exchange_refresh(
                refresh_admission(initial_grant.refresh_token.expose_secret()),
                refresh_params(1),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), AuthErrorCode::SessionExpired);
        assert_eq!(
            scalar_text(
                &service.database,
                "SELECT status AS value FROM auth_session"
            )
            .await,
            "expired"
        );
        assert_eq!(count(&service.database, "auth_refresh_credential").await, 0);
        assert_eq!(
            hook.0.lock().unwrap().as_slice(),
            &[(
                initial_grant.session.id.to_string(),
                AuthSessionTerminationReason::SessionExpired,
            )]
        );
        assert_eq!(
            service
                .validate_session_lease(principal.as_ref())
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::SessionExpired
        );
        assert!(
            pioneer_crud::scan_auth_persistence_invariants(&service.database)
                .await
                .unwrap()
                .is_valid()
        );
    }

    #[tokio::test]
    async fn maintenance_expires_stale_session_atomically_before_disconnect() {
        let (service, _) = fixture().await;
        let hook = Arc::new(RecordingDisconnectHook::default());
        service.set_disconnect_hook(hook.clone());
        let initial_grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        service
            .database
            .execute_unprepared(
                "UPDATE auth_session SET created_at = datetime('now', '-2 days'), refresh_expires_at = datetime('now', '-1 day'); \
                 UPDATE auth_refresh_credential SET issued_at = datetime('now', '-2 days'), expires_at = datetime('now', '-1 day')",
            )
            .await
            .unwrap();

        assert_eq!(
            service
                .expire_stale_sessions(unix_timestamp_secs().unwrap(), 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            scalar_text(
                &service.database,
                "SELECT status AS value FROM auth_session"
            )
            .await,
            "expired"
        );
        assert_eq!(count(&service.database, "auth_refresh_credential").await, 0);
        assert_eq!(
            scalar_text(&service.database, "SELECT status AS value FROM device").await,
            "revoked"
        );
        assert_eq!(
            hook.0.lock().unwrap().as_slice(),
            &[(
                initial_grant.session.id.to_string(),
                AuthSessionTerminationReason::SessionExpired,
            )]
        );
        assert_eq!(
            service
                .expire_stale_sessions(unix_timestamp_secs().unwrap(), 1)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn signed_older_generation_reuse_revokes_family_but_unknown_token_does_not() {
        let (service, _) = fixture().await;
        let hook = Arc::new(RecordingDisconnectHook::default());
        service.set_disconnect_hook(hook.clone());
        let initial_grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let old = initial_grant.refresh_token.expose_secret().to_owned();
        let generation_one = service
            .exchange_refresh(refresh_admission(old.as_str()), refresh_params(1))
            .await
            .unwrap();
        service
            .exchange_refresh(
                refresh_admission(generation_one.refresh_token.expose_secret()),
                refresh_params(2),
            )
            .await
            .unwrap();
        let reuse = service
            .exchange_refresh(refresh_admission(old.as_str()), refresh_params(3))
            .await
            .unwrap_err();
        assert_eq!(reuse.code(), AuthErrorCode::SessionCompromised);
        assert_eq!(
            scalar_text(
                &service.database,
                "SELECT status AS value FROM auth_session"
            )
            .await,
            "revoked"
        );
        assert_eq!(
            scalar_text(
                &service.database,
                "SELECT revoke_reason AS value FROM auth_session"
            )
            .await,
            "refresh_reuse"
        );
        assert_eq!(count(&service.database, "auth_refresh_credential").await, 0);
        assert_eq!(hook.0.lock().unwrap().len(), 1);

        let (other, _) = fixture().await;
        let active = other
            .create_initial_session_with_ids(
                params("other-installation", ClientKind::Desktop),
                ids(2),
            )
            .await
            .unwrap();
        assert!(
            other
                .exchange_refresh(
                    refresh_admission(
                        format!(
                            "{}{}",
                            pioneer_protocol::REFRESH_CREDENTIAL_PREFIX,
                            "u".repeat(pioneer_protocol::REFRESH_CREDENTIAL_BODY_LEN)
                        )
                        .as_str(),
                    ),
                    refresh_params(1),
                )
                .await
                .is_err()
        );
        assert_eq!(
            scalar_text(&other.database, "SELECT status AS value FROM auth_session").await,
            "active"
        );
        assert!(!active.refresh_token.expose_secret().is_empty());
    }

    #[tokio::test]
    async fn expired_prior_generation_cannot_revoke_an_active_session() {
        let (service, _) = fixture().await;
        let initial = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let rotated = service
            .exchange_refresh(
                refresh_admission(initial.refresh_token.expose_secret()),
                refresh_params(1),
            )
            .await
            .unwrap();
        let expired_prior = service.opaque_credentials.refresh_from_nonce(
            &initial.session.id,
            &initial.session.token_family_id,
            0,
            unix_timestamp_secs().unwrap() - 1,
            &[9; 32],
        );

        let error = service
            .exchange_refresh(
                refresh_admission(expired_prior.expose_for_exchange()),
                refresh_params(2),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), AuthErrorCode::InvalidCredential);
        assert_eq!(
            scalar_text(
                &service.database,
                "SELECT status AS value FROM auth_session"
            )
            .await,
            "active"
        );
        assert_eq!(count(&service.database, "auth_refresh_credential").await, 1);
        assert!(!rotated.refresh_token.expose_secret().is_empty());
    }

    #[tokio::test]
    async fn concurrent_same_refresh_has_one_rotation_then_compromise_revoke() {
        let (service, _) = fixture().await;
        let service = Arc::new(service);
        let initial_grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let raw = initial_grant.refresh_token.expose_secret().to_owned();
        let left = {
            let service = service.clone();
            let raw = raw.clone();
            tokio::spawn(async move {
                service
                    .exchange_refresh(refresh_admission(&raw), refresh_params(1))
                    .await
            })
        };
        let right = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .exchange_refresh(refresh_admission(&raw), refresh_params(2))
                    .await
            })
        };
        let outcomes = [left.await.unwrap(), right.await.unwrap()];
        assert_eq!(outcomes.iter().filter(|value| value.is_ok()).count(), 1);
        assert_eq!(outcomes.iter().filter(|value| value.is_err()).count(), 1);
        assert_eq!(
            scalar_text(
                &service.database,
                "SELECT status AS value FROM auth_session"
            )
            .await,
            "revoked"
        );
    }

    #[tokio::test]
    async fn concurrent_refreshes_for_different_sessions_remain_independent() {
        let (service, _) = fixture().await;
        let service = Arc::new(service);
        let desktop = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let mobile = service
            .create_initial_session_with_ids(
                params("mobile-installation", ClientKind::Mobile),
                ids(2),
            )
            .await
            .unwrap();
        let desktop_session_id = desktop.session.id.clone();
        let mobile_session_id = mobile.session.id.clone();
        let left = {
            let service = service.clone();
            let refresh = desktop.refresh_token.expose_secret().to_owned();
            tokio::spawn(async move {
                service
                    .exchange_refresh(refresh_admission(&refresh), refresh_params(1))
                    .await
            })
        };
        let right = {
            let service = service.clone();
            let refresh = mobile.refresh_token.expose_secret().to_owned();
            tokio::spawn(async move {
                service
                    .exchange_refresh(refresh_admission(&refresh), refresh_params(2))
                    .await
            })
        };

        let desktop_rotated = left.await.unwrap().unwrap();
        let mobile_rotated = right.await.unwrap().unwrap();
        assert_eq!(desktop_rotated.session.id, desktop_session_id);
        assert_eq!(mobile_rotated.session.id, mobile_session_id);
        assert_eq!(
            count_where(&service.database, "auth_session", "status = 'active'").await,
            2
        );
        assert_eq!(count(&service.database, "auth_refresh_credential").await, 2);
    }

    #[tokio::test]
    async fn pending_device_expiry_is_bounded_and_preserves_terminal_audit_rows() {
        let (service, _identity) = fixture().await;
        let initial_grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let principal = authenticate_grant(&service, &initial_grant).await;
        let pending = service.create_device(principal.as_ref()).await.unwrap();

        assert_eq!(
            service
                .expire_pending_device_sessions(unix_timestamp_secs().unwrap(), 1)
                .await
                .unwrap(),
            0
        );
        service
            .database
            .execute_unprepared(
                "UPDATE auth_session \
                 SET activation_expires_at = created_at \
                 WHERE status = 'pending'",
            )
            .await
            .unwrap();
        assert_eq!(
            service
                .expire_pending_device_sessions(unix_timestamp_secs().unwrap(), 0)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            count_where(&service.database, "auth_session", "status = 'pending'").await,
            1
        );
        assert_eq!(
            service
                .expire_pending_device_sessions(unix_timestamp_secs().unwrap(), 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(count(&service.database, "auth_session").await, 2);
        assert_eq!(
            count_where(&service.database, "auth_session", "status = 'expired'").await,
            1
        );
        assert_eq!(
            count_where(&service.database, "device", "status = 'revoked'").await,
            1
        );
        assert_eq!(
            scalar_text(
                &service.database,
                format!(
                    "SELECT status AS value FROM auth_session WHERE id = '{}'",
                    pending.session_id
                )
                .as_str(),
            )
            .await,
            "expired"
        );
        assert!(
            pioneer_crud::scan_auth_persistence_invariants(&service.database)
                .await
                .unwrap()
                .is_valid()
        );
    }

    #[tokio::test]
    async fn session_management_lists_and_revokes_only_owned_session() {
        let (service, _) = fixture().await;
        let hook = Arc::new(RecordingDisconnectHook::default());
        service.set_disconnect_hook(hook.clone());
        let desktop = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let mobile = service
            .create_initial_session_with_ids(
                params("mobile-installation", ClientKind::Mobile),
                ids(2),
            )
            .await
            .unwrap();
        let desktop_principal = authenticate_grant(&service, &desktop).await;
        let mobile_principal = authenticate_grant(&service, &mobile).await;

        let listed = service
            .list_sessions(desktop_principal.as_ref())
            .await
            .unwrap();
        assert_eq!(listed.sessions.len(), 2);
        assert_eq!(
            listed.sessions.iter().filter(|item| item.current).count(),
            1
        );
        assert!(listed.sessions.iter().all(|item| {
            item.device.id == desktop.device.id || item.device.id == mobile.device.id
        }));

        let revoked = service
            .revoke_owned_session(
                desktop_principal.as_ref(),
                &mobile.session.id,
                Some(AuthSessionStatus::Active),
                AuthSessionRevokeReason::SelfRevoke,
            )
            .await
            .unwrap();
        assert!(revoked.revoked);
        assert!(hook.0.lock().unwrap().is_empty());
        service
            .disconnect_committed_session(
                &mobile.session.id,
                AuthSessionTerminationReason::SessionRevoked,
            )
            .await;
        assert_eq!(
            hook.0.lock().unwrap().as_slice(),
            &[(
                mobile.session.id.to_string(),
                AuthSessionTerminationReason::SessionRevoked,
            )]
        );
        service
            .validate_session_lease(desktop_principal.as_ref())
            .await
            .unwrap();
        assert_eq!(
            service
                .validate_session_lease(mobile_principal.as_ref())
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::SessionRevoked
        );
        assert_eq!(
            count_where(
                &service.database,
                "auth_refresh_credential",
                "session_id = 'S00000000000000000002'"
            )
            .await,
            0
        );
        assert!(
            pioneer_crud::scan_auth_persistence_invariants(&service.database)
                .await
                .unwrap()
                .is_valid()
        );
    }

    #[tokio::test]
    async fn logout_commits_before_disconnect_and_denies_subsequent_lease() {
        let (service, _) = fixture().await;
        let hook = Arc::new(RecordingDisconnectHook::default());
        service.set_disconnect_hook(hook.clone());
        let grant = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let principal = authenticate_grant(&service, &grant).await;
        let refresh = grant.refresh_token.expose_secret().to_owned();

        let response = service.logout(principal.as_ref()).await.unwrap();
        assert!(response.revoked);
        assert!(hook.0.lock().unwrap().is_empty());
        assert_eq!(
            scalar_text(
                &service.database,
                "SELECT status AS value FROM auth_session"
            )
            .await,
            "revoked"
        );
        service
            .disconnect_committed_session(
                &response.session_id,
                AuthSessionTerminationReason::SessionRevoked,
            )
            .await;
        assert_eq!(hook.0.lock().unwrap().len(), 1);
        assert_eq!(count(&service.database, "auth_refresh_credential").await, 0);
        assert_eq!(
            service
                .exchange_refresh(refresh_admission(&refresh), refresh_params(1))
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::SessionRevoked
        );
        assert_eq!(
            scalar_text(
                &service.database,
                "SELECT revoke_reason AS value FROM auth_session"
            )
            .await,
            "logout"
        );
        assert_eq!(
            service
                .validate_session_lease(principal.as_ref())
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::SessionRevoked
        );
    }

    #[tokio::test]
    async fn device_activation_is_one_use_and_creates_same_principal_distinct_session() {
        let (service, _) = fixture().await;
        let creator = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let creator_principal = authenticate_grant(&service, &creator).await;
        let device_activation = service
            .create_device(creator_principal.as_ref())
            .await
            .unwrap();
        let raw = device_activation.activation_code.expose_secret().to_owned();
        assert_eq!(raw.len(), 9);
        assert_eq!(raw.as_bytes()[4], b'-');
        assert!(pioneer_protocol::normalize_device_activation_code(&raw).is_ok());
        assert!(!format!("{device_activation:?}").contains(&raw));
        assert!(!database_text(&service.database).await.contains(&raw));

        let accepted = service
            .activate_device_with_ids(
                device_activation_admission(&service, &raw),
                device_activation_params("mobile-installation"),
                issued_ids(2),
            )
            .await
            .unwrap();
        assert_eq!(accepted.principal.id, creator.principal.id);
        assert_ne!(accepted.device.id, creator.device.id);
        assert_ne!(accepted.session.id, creator.session.id);
        assert_eq!(accepted.refresh_generation, 0);
        let replay = service
            .activate_device_with_ids(
                device_activation_admission(&service, &raw),
                device_activation_params("other-installation"),
                issued_ids(3),
            )
            .await
            .unwrap_err();
        assert_eq!(replay.code(), AuthErrorCode::DeviceActivationConsumed);
    }

    #[tokio::test]
    async fn five_wrong_codes_for_one_locator_revoke_the_pending_request() {
        let (service, _) = fixture().await;
        let creator = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let creator_principal = authenticate_grant(&service, &creator).await;
        let pending = service
            .create_device(creator_principal.as_ref())
            .await
            .unwrap();
        let correct = pioneer_protocol::normalize_device_activation_code(
            pending.activation_code.expose_secret(),
        )
        .unwrap();
        let wrong_suffixes = pioneer_protocol::DEVICE_ACTIVATION_ALPHABET
            .iter()
            .copied()
            .map(char::from)
            .filter(|candidate| Some(*candidate) != correct.chars().last())
            .take(5)
            .collect::<Vec<_>>();

        for (index, suffix) in wrong_suffixes.into_iter().enumerate() {
            let mut wrong = correct.clone();
            wrong.replace_range(7..8, suffix.to_string().as_str());
            let error = service
                .activate_device_with_ids(
                    device_activation_admission(&service, wrong.as_str()),
                    device_activation_params("attacker-installation"),
                    issued_ids(u8::try_from(index + 20).unwrap()),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), AuthErrorCode::InvalidCredential);
            assert_eq!(
                scalar_i64(
                    &service.database,
                    format!(
                        "SELECT activation_failed_attempts AS value FROM auth_session WHERE id = '{}'",
                        pending.session_id
                    )
                    .as_str(),
                )
                .await,
                i64::try_from(index + 1).unwrap()
            );
            assert_eq!(
                scalar_text(
                    &service.database,
                    format!(
                        "SELECT status AS value FROM auth_session WHERE id = '{}'",
                        pending.session_id
                    )
                    .as_str(),
                )
                .await,
                if index == 4 { "revoked" } else { "pending" }
            );
        }

        assert_eq!(
            scalar_text(
                &service.database,
                format!(
                    "SELECT revoke_reason AS value FROM auth_session WHERE id = '{}'",
                    pending.session_id
                )
                .as_str(),
            )
            .await,
            "activation_attempts_exceeded"
        );
        assert_eq!(
            scalar_text(
                &service.database,
                format!(
                    "SELECT status AS value FROM device WHERE id = '{}'",
                    pending.device_id
                )
                .as_str(),
            )
            .await,
            "revoked"
        );
        let correct_after_lockout = service
            .activate_device_with_ids(
                device_activation_admission(&service, pending.activation_code.expose_secret()),
                device_activation_params("mobile-installation"),
                issued_ids(30),
            )
            .await
            .unwrap_err();
        assert_eq!(
            correct_after_lockout.code(),
            AuthErrorCode::DeviceActivationConsumed
        );
        assert!(
            pioneer_crud::scan_auth_persistence_invariants(&service.database)
                .await
                .unwrap()
                .is_valid()
        );
    }

    #[tokio::test]
    async fn historical_exact_code_does_not_bypass_current_request_attempt_counter() {
        let (service, _) = fixture().await;
        let creator = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let creator_principal = authenticate_grant(&service, &creator).await;
        let historical = service
            .create_device(creator_principal.as_ref())
            .await
            .unwrap();
        let current = service
            .create_device(creator_principal.as_ref())
            .await
            .unwrap();
        let mut wrong = pioneer_protocol::normalize_device_activation_code(
            current.activation_code.expose_secret(),
        )
        .unwrap();
        let replacement = pioneer_protocol::DEVICE_ACTIVATION_ALPHABET
            .iter()
            .copied()
            .map(char::from)
            .find(|candidate| Some(*candidate) != wrong.chars().last())
            .unwrap();
        wrong.replace_range(7..8, replacement.to_string().as_str());
        let historical_hash = service
            .opaque_credentials
            .fingerprint_device_activation_raw(wrong.as_str());
        service
            .database
            .execute_raw(Statement::from_sql_and_values(
                service.database.get_database_backend(),
                "UPDATE auth_session SET activation_token_hash = ? WHERE id = ?",
                [
                    historical_hash.to_vec().into(),
                    historical.session_id.to_string().into(),
                ],
            ))
            .await
            .unwrap();

        let error = service
            .activate_device_with_ids(
                device_activation_admission(&service, wrong.as_str()),
                device_activation_params("attacker-installation"),
                issued_ids(30),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), AuthErrorCode::InvalidCredential);
        assert_eq!(
            scalar_i64(
                &service.database,
                format!(
                    "SELECT activation_failed_attempts AS value FROM auth_session WHERE id = '{}'",
                    current.session_id
                )
                .as_str(),
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn revoked_creator_session_cannot_authorize_device_activation_redemption() {
        let (service, _identity) = fixture().await;
        let creator = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let creator_principal = authenticate_grant(&service, &creator).await;
        let device_activation = service
            .create_device(creator_principal.as_ref())
            .await
            .unwrap();
        service
            .revoke_owned_session(
                creator_principal.as_ref(),
                &creator.session.id,
                None,
                AuthSessionRevokeReason::SelfRevoke,
            )
            .await
            .unwrap();

        let error = service
            .activate_device_with_ids(
                device_activation_admission(
                    &service,
                    device_activation.activation_code.expose_secret(),
                ),
                device_activation_params("mobile-installation"),
                issued_ids(2),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), AuthErrorCode::SessionRevoked);
        assert_eq!(
            count_where(&service.database, "device", "status = 'active'").await,
            0
        );
        assert_eq!(
            count_where(&service.database, "auth_session", "status = 'active'").await,
            0
        );
    }

    #[tokio::test]
    async fn device_activation_atomically_replaces_an_existing_installation() {
        let (service, _) = fixture().await;
        let hook = Arc::new(RecordingDisconnectHook::default());
        service.set_disconnect_hook(hook.clone());
        let creator = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let creator_principal = authenticate_grant(&service, &creator).await;
        let first_device_activation = service
            .create_device(creator_principal.as_ref())
            .await
            .unwrap();
        let first_mobile = service
            .activate_device_with_ids(
                device_activation_admission(
                    &service,
                    first_device_activation.activation_code.expose_secret(),
                ),
                device_activation_params("mobile-installation"),
                issued_ids(2),
            )
            .await
            .unwrap();

        let replacement_device_activation = service
            .create_device(creator_principal.as_ref())
            .await
            .unwrap();
        let replacement = service
            .activate_device_with_ids(
                device_activation_admission(
                    &service,
                    replacement_device_activation
                        .activation_code
                        .expose_secret(),
                ),
                device_activation_params("mobile-installation"),
                issued_ids(3),
            )
            .await
            .unwrap();

        assert_ne!(replacement.device.id, first_mobile.device.id);
        assert_ne!(replacement.session.id, first_mobile.session.id);
        let old_device_status_query = format!(
            "SELECT status AS value FROM device WHERE id = '{}'",
            first_mobile.device.id
        );
        assert_eq!(
            scalar_text(&service.database, &old_device_status_query).await,
            "revoked"
        );
        let old_session_status_query = format!(
            "SELECT status AS value FROM auth_session WHERE id = '{}'",
            first_mobile.session.id
        );
        assert_eq!(
            scalar_text(&service.database, &old_session_status_query).await,
            "revoked"
        );
        assert_eq!(
            hook.0.lock().unwrap().as_slice(),
            &[(
                first_mobile.session.id.to_string(),
                AuthSessionTerminationReason::SessionRevoked,
            )]
        );
        assert_eq!(
            count_where(&service.database, "device", "status = 'active'").await,
            2
        );
        assert_eq!(
            count_where(&service.database, "auth_session", "status = 'active'").await,
            2
        );
    }

    #[tokio::test]
    async fn second_device_activation_create_replaces_previous_and_explicit_revoke_is_terminal() {
        let (service, _identity) = fixture().await;
        let creator = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let principal = authenticate_grant(&service, &creator).await;
        let first = service.create_device(principal.as_ref()).await.unwrap();
        let first_raw = first.activation_code.expose_secret().to_owned();
        let second = service.create_device(principal.as_ref()).await.unwrap();
        assert_eq!(
            service
                .list_sessions(principal.as_ref())
                .await
                .unwrap()
                .sessions
                .len(),
            1,
            "pending device sessions are not installed devices"
        );
        assert_eq!(
            service
                .activate_device_with_ids(
                    device_activation_admission(&service, &first_raw),
                    device_activation_params("mobile-installation"),
                    issued_ids(2),
                )
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::DeviceActivationConsumed
        );
        let revoked = service
            .revoke_owned_session(
                principal.as_ref(),
                &second.session_id,
                Some(AuthSessionStatus::Pending),
                AuthSessionRevokeReason::DeviceRevoke,
            )
            .await
            .unwrap();
        assert!(revoked.revoked);
        assert_eq!(
            service
                .list_sessions(principal.as_ref())
                .await
                .unwrap()
                .sessions
                .len(),
            1,
            "revoked unactivated rows must not break or pollute device listing"
        );
        assert_eq!(
            service
                .activate_device_with_ids(
                    device_activation_admission(&service, second.activation_code.expose_secret(),),
                    device_activation_params("mobile-installation"),
                    issued_ids(2),
                )
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::DeviceActivationConsumed
        );
    }

    #[tokio::test]
    async fn late_pending_cancel_does_not_revoke_an_already_activated_session() {
        let (service, _identity) = fixture().await;
        let creator = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let creator_principal = authenticate_grant(&service, &creator).await;
        let created = service
            .create_device(creator_principal.as_ref())
            .await
            .unwrap();
        let activated = service
            .activate_device_with_ids(
                device_activation_admission(&service, created.activation_code.expose_secret()),
                device_activation_params("mobile-installation"),
                issued_ids(2),
            )
            .await
            .unwrap();

        let cancelled = service
            .revoke_owned_session(
                creator_principal.as_ref(),
                &activated.session.id,
                Some(AuthSessionStatus::Pending),
                AuthSessionRevokeReason::DeviceRevoke,
            )
            .await
            .unwrap();
        assert!(!cancelled.revoked);

        let activated_principal = authenticate_grant(&service, &activated).await;
        service
            .validate_session_lease(activated_principal.as_ref())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn expired_and_gateway_mismatched_device_activation_fail_closed() {
        let (service, _) = fixture().await;
        let creator = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let principal = authenticate_grant(&service, &creator).await;
        let device_activation = service.create_device(principal.as_ref()).await.unwrap();
        let mismatched_admission = RestrictedAdmission::new(
            PresentedCredential::classify(device_activation.activation_code.expose_secret())
                .unwrap(),
            RestrictedAuthContext::DeviceActivation(super::super::DeviceActivationContext {
                gateway_id: pioneer_protocol::GatewayId::new("G00000000000000000099").unwrap(),
            }),
        );
        assert_eq!(
            service
                .activate_device_with_ids(
                    mismatched_admission,
                    device_activation_params("mobile-installation"),
                    issued_ids(2),
                )
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::GatewayIdentityMismatch
        );
        service
            .database
            .execute_unprepared(
                "UPDATE auth_session SET activation_expires_at = created_at WHERE status = 'pending'",
            )
            .await
            .unwrap();
        assert_eq!(
            service
                .activate_device_with_ids(
                    device_activation_admission(
                        &service,
                        device_activation.activation_code.expose_secret(),
                    ),
                    device_activation_params("mobile-installation"),
                    issued_ids(2),
                )
                .await
                .unwrap_err()
                .code(),
            AuthErrorCode::DeviceActivationExpired
        );
        assert_eq!(
            count_where(&service.database, "auth_session", "status = 'expired'").await,
            1
        );
    }

    #[tokio::test]
    async fn concurrent_device_activation_accept_has_exactly_one_winner() {
        let (service, _) = fixture().await;
        let service = Arc::new(service);
        let creator = service
            .create_initial_session_with_ids(
                params("desktop-installation", ClientKind::Desktop),
                ids(1),
            )
            .await
            .unwrap();
        let principal = authenticate_grant(&service, &creator).await;
        let device_activation = service.create_device(principal.as_ref()).await.unwrap();
        let raw = device_activation.activation_code.expose_secret().to_owned();
        let left = {
            let service = service.clone();
            let raw = raw.clone();
            tokio::spawn(async move {
                service
                    .activate_device_with_ids(
                        device_activation_admission(&service, &raw),
                        device_activation_params("mobile-left"),
                        issued_ids(2),
                    )
                    .await
            })
        };
        let right = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .activate_device_with_ids(
                        device_activation_admission(&service, &raw),
                        device_activation_params("mobile-right"),
                        issued_ids(3),
                    )
                    .await
            })
        };
        let outcomes = [left.await.unwrap(), right.await.unwrap()];
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(outcomes.iter().filter(|result| result.is_err()).count(), 1);
        assert_eq!(count(&service.database, "device").await, 2);
        assert_eq!(count(&service.database, "auth_session").await, 2);
    }

    fn params(installation_id: &str, client_kind: ClientKind) -> AuthDeviceActivateParams {
        AuthDeviceActivateParams {
            installation: ClientInstallationDescriptor {
                installation_id: installation_id.to_owned(),
                display_name: installation_id.to_owned(),
                client_kind,
                platform: Some("test".to_owned()),
                client_version: Some("1.0".to_owned()),
            },
        }
    }

    fn refresh_params(generation: u64) -> AuthRefreshParams {
        AuthRefreshParams {
            refresh_request_id: format!("Q{generation:020}"),
            client_version: Some("1.0".to_owned()),
        }
    }

    fn device_activation_params(installation_id: &str) -> AuthDeviceActivateParams {
        AuthDeviceActivateParams {
            installation: ClientInstallationDescriptor {
                installation_id: installation_id.to_owned(),
                display_name: installation_id.to_owned(),
                client_kind: ClientKind::Mobile,
                platform: Some("ios".to_owned()),
                client_version: Some("1.0".to_owned()),
            },
        }
    }

    #[test]
    fn installation_metadata_is_trimmed_bounded_and_control_free() {
        assert_eq!(bounded_trimmed("  Desktop  ", 255).unwrap(), "Desktop");
        assert_eq!(
            bounded_trimmed("Desktop\nInjected", 255)
                .unwrap_err()
                .code(),
            AuthErrorCode::MalformedCredential
        );
        assert_eq!(
            bounded_optional(Some("iOS\0hidden".to_owned()), 255)
                .unwrap_err()
                .code(),
            AuthErrorCode::MalformedCredential
        );
    }

    fn refresh_admission(raw: &str) -> RestrictedAdmission {
        RestrictedAdmission::new(
            PresentedCredential::classify(raw).unwrap(),
            RestrictedAuthContext::Refresh(super::super::RefreshExchangeContext),
        )
    }

    fn device_activation_admission(service: &GatewayAuthService, raw: &str) -> RestrictedAdmission {
        RestrictedAdmission::new(
            PresentedCredential::classify(raw).unwrap(),
            RestrictedAuthContext::DeviceActivation(super::super::DeviceActivationContext {
                gateway_id: service.identity.gateway.id.clone(),
            }),
        )
    }

    async fn authenticate_grant(
        service: &GatewayAuthService,
        grant: &AuthSessionGrant,
    ) -> Arc<AuthenticatedSessionPrincipal> {
        authenticate_access_token(service, grant.access_token.expose_secret()).await
    }

    async fn authenticate_access_token(
        service: &GatewayAuthService,
        access_token: &str,
    ) -> Arc<AuthenticatedSessionPrincipal> {
        let credential = service
            .access_issuer
            .validate(access_token, unix_timestamp_secs().unwrap())
            .unwrap();
        service.authenticate_access(credential).await.unwrap()
    }

    fn ids(seed: u8) -> SessionGrantIds {
        SessionGrantIds {
            device_id: DeviceId::new(format!("D{seed:020}")).unwrap(),
            session_id: AuthSessionId::new(format!("S{seed:020}")).unwrap(),
            refresh_id: RefreshCredentialId::new(format!("R{seed:020}")).unwrap(),
            token_family_id: TokenFamilyId::new(format!("F{seed:020}")).unwrap(),
            access_jti: format!("J{seed:020}"),
        }
    }

    fn issued_ids(seed: u8) -> IssuedCredentialIds {
        IssuedCredentialIds {
            refresh_id: RefreshCredentialId::new(format!("R{seed:020}")).unwrap(),
            access_jti: format!("J{seed:020}"),
        }
    }

    async fn count(database: &DatabaseConnection, table: &str) -> i64 {
        database
            .query_one_raw(Statement::from_string(
                database.get_database_backend(),
                format!("SELECT COUNT(*) AS count FROM {table}"),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "count")
            .unwrap()
    }

    async fn count_where(database: &DatabaseConnection, table: &str, condition: &str) -> i64 {
        scalar_i64(
            database,
            format!("SELECT COUNT(*) AS value FROM {table} WHERE {condition}").as_str(),
        )
        .await
    }

    async fn scalar_i64(database: &DatabaseConnection, query: &str) -> i64 {
        database
            .query_one_raw(Statement::from_string(
                database.get_database_backend(),
                query.to_owned(),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "value")
            .unwrap()
    }

    async fn scalar_text(database: &DatabaseConnection, query: &str) -> String {
        database
            .query_one_raw(Statement::from_string(
                database.get_database_backend(),
                query.to_owned(),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "value")
            .unwrap()
    }

    async fn database_text(database: &DatabaseConnection) -> String {
        let rows = database
            .query_all_raw(Statement::from_string(
                database.get_database_backend(),
                "SELECT id || ':' || COALESCE(installation_id, '') || ':' || COALESCE(display_name, '') AS value FROM device UNION ALL SELECT id || ':' || status || ':' || token_family_id FROM auth_session UNION ALL SELECT id || ':' || hex(token_hash) || ':' || generation FROM auth_refresh_credential".to_owned(),
            ))
            .await
            .unwrap();
        rows.into_iter()
            .map(|row| row.try_get::<String>("", "value").unwrap())
            .collect::<Vec<_>>()
            .join("\n")
    }
}
