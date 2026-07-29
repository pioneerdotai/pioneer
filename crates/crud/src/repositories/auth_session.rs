use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use pioneer_entity::{
    auth_refresh_credential, auth_session, device, gateway_identity, gateway_principal,
};
use pioneer_protocol::{
    AuthSessionId, AuthSessionRevokeReason, AuthSessionStatus, ClientInstallationDescriptor,
    ClientKind, DEVICE_ACTIVATION_MAX_FAILED_ATTEMPTS, DeviceId, DeviceStatus, GatewayId,
    PrincipalId, RefreshCredentialId, RefreshCredentialStatus, TokenFamilyId,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, Statement, sea_query::Expr,
};

#[derive(Clone)]
pub struct NewPendingDeviceSessionRow {
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub token_family_id: TokenFamilyId,
    pub issuer: PendingSessionIssuer,
    pub activation_token_hash: [u8; 32],
    pub activation_locator_hash: [u8; 32],
    pub activation_expires_at: DateTimeWithTimeZone,
    pub now: DateTimeWithTimeZone,
}

#[derive(Clone)]
pub struct NewRefreshCredentialRow {
    pub id: RefreshCredentialId,
    pub session_id: AuthSessionId,
    pub token_family_id: TokenFamilyId,
    pub generation: u64,
    pub token_hash: [u8; 32],
    pub issued_at: DateTimeWithTimeZone,
    pub expires_at: DateTimeWithTimeZone,
}

#[derive(Clone)]
pub enum PendingSessionIssuer {
    LocalCli,
    AuthenticatedSession(AuthSessionId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceActivationFailureOutcome {
    AttemptRecorded { failed_attempts: u32 },
    RequestRevoked { failed_attempts: u32 },
}

impl PendingSessionIssuer {
    fn session_id(&self) -> Option<String> {
        match self {
            Self::LocalCli => None,
            Self::AuthenticatedSession(session_id) => Some(session_id.to_string()),
        }
    }
}

impl std::fmt::Debug for PendingSessionIssuer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalCli => formatter.write_str("LocalCli"),
            Self::AuthenticatedSession(session_id) => formatter
                .debug_tuple("AuthenticatedSession")
                .field(session_id)
                .finish(),
        }
    }
}

impl std::fmt::Debug for NewPendingDeviceSessionRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NewPendingDeviceSessionRow")
            .field("device_id", &self.device_id)
            .field("session_id", &self.session_id)
            .field("gateway_id", &self.gateway_id)
            .field("principal_id", &self.principal_id)
            .field("token_family_id", &self.token_family_id)
            .field("issuer", &self.issuer)
            .field("activation_token_hash", &"[redacted]")
            .field("activation_locator_hash", &"[redacted]")
            .field("activation_expires_at", &self.activation_expires_at)
            .field("now", &self.now)
            .finish()
    }
}

impl std::fmt::Debug for NewRefreshCredentialRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NewRefreshCredentialRow")
            .field("id", &self.id)
            .field("session_id", &self.session_id)
            .field("token_family_id", &self.token_family_id)
            .field("generation", &self.generation)
            .field("token_hash", &"[redacted]")
            .field("issued_at", &self.issued_at)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthPersistenceInvariantReport {
    pub violations: Vec<String>,
}

impl AuthPersistenceInvariantReport {
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }
}

pub const fn client_kind_to_db(value: ClientKind) -> &'static str {
    match value {
        ClientKind::Desktop => "desktop",
        ClientKind::Mobile => "mobile",
        ClientKind::Other => "other",
    }
}

pub const fn device_status_to_db(value: DeviceStatus) -> &'static str {
    match value {
        DeviceStatus::Pending => "pending",
        DeviceStatus::Active => "active",
        DeviceStatus::Revoked => "revoked",
    }
}

pub const fn auth_session_status_to_db(value: AuthSessionStatus) -> &'static str {
    match value {
        AuthSessionStatus::Pending => "pending",
        AuthSessionStatus::Active => "active",
        AuthSessionStatus::Revoked => "revoked",
        AuthSessionStatus::Expired => "expired",
    }
}

pub const fn refresh_status_to_db(value: RefreshCredentialStatus) -> &'static str {
    match value {
        RefreshCredentialStatus::Current => "current",
        RefreshCredentialStatus::Rotated => "rotated",
        RefreshCredentialStatus::Revoked => "revoked",
        RefreshCredentialStatus::Expired => "expired",
    }
}

pub const fn revoke_reason_to_db(value: AuthSessionRevokeReason) -> &'static str {
    match value {
        AuthSessionRevokeReason::Logout => "logout",
        AuthSessionRevokeReason::SelfRevoke => "self_revoke",
        AuthSessionRevokeReason::DeviceRevoke => "device_revoke",
        AuthSessionRevokeReason::ActivationAttemptsExceeded => "activation_attempts_exceeded",
        AuthSessionRevokeReason::RefreshReuse => "refresh_reuse",
        AuthSessionRevokeReason::PrincipalSuspended => "principal_suspended",
        AuthSessionRevokeReason::PrincipalRemoved => "principal_removed",
        AuthSessionRevokeReason::Superseded => "superseded",
        AuthSessionRevokeReason::SecurityReset => "security_reset",
    }
}

pub async fn insert_pending_device_session<C: ConnectionTrait>(
    db: &C,
    row: NewPendingDeviceSessionRow,
) -> Result<(device::Model, auth_session::Model)> {
    let device = device::ActiveModel {
        id: Set(row.device_id.to_string()),
        gateway_id: Set(row.gateway_id.to_string()),
        principal_id: Set(row.principal_id.to_string()),
        installation_id: Set(None),
        display_name: Set(None),
        client_kind: Set(None),
        platform: Set(None),
        client_version: Set(None),
        status: Set(device_status_to_db(DeviceStatus::Pending).to_owned()),
        created_at: Set(row.now),
        updated_at: Set(row.now),
        last_seen_at: Set(None),
        revoked_at: Set(None),
    }
    .insert(db)
    .await
    .context("failed to insert pending device")?;
    let session = auth_session::ActiveModel {
        id: Set(row.session_id.to_string()),
        gateway_id: Set(row.gateway_id.to_string()),
        principal_id: Set(row.principal_id.to_string()),
        device_id: Set(row.device_id.to_string()),
        token_family_id: Set(row.token_family_id.to_string()),
        created_by_session_id: Set(row.issuer.session_id()),
        activation_token_hash: Set(row.activation_token_hash.to_vec()),
        activation_locator_hash: Set(row.activation_locator_hash.to_vec()),
        activation_failed_attempts: Set(0),
        activation_expires_at: Set(row.activation_expires_at),
        activated_at: Set(None),
        status: Set(auth_session_status_to_db(AuthSessionStatus::Pending).to_owned()),
        refresh_generation: Set(0),
        created_at: Set(row.now),
        updated_at: Set(row.now),
        last_seen_at: Set(None),
        last_refreshed_at: Set(None),
        refresh_expires_at: Set(None),
        revoked_at: Set(None),
        revoke_reason: Set(None),
    }
    .insert(db)
    .await
    .context("failed to insert pending auth session")?;
    Ok((device, session))
}

pub async fn activate_pending_device<C: ConnectionTrait>(
    db: &C,
    device_id: &DeviceId,
    installation: &ClientInstallationDescriptor,
    now: DateTimeWithTimeZone,
) -> Result<Option<device::Model>> {
    let result = device::Entity::update_many()
        .col_expr(
            device::Column::InstallationId,
            Expr::value(Some(installation.installation_id.clone())),
        )
        .col_expr(
            device::Column::DisplayName,
            Expr::value(Some(installation.display_name.clone())),
        )
        .col_expr(
            device::Column::ClientKind,
            Expr::value(Some(client_kind_to_db(installation.client_kind).to_owned())),
        )
        .col_expr(
            device::Column::Platform,
            Expr::value(installation.platform.clone()),
        )
        .col_expr(
            device::Column::ClientVersion,
            Expr::value(installation.client_version.clone()),
        )
        .col_expr(device::Column::Status, Expr::value("active"))
        .col_expr(device::Column::UpdatedAt, Expr::value(now))
        .col_expr(device::Column::LastSeenAt, Expr::value(Some(now)))
        .filter(device::Column::Id.eq(device_id.to_string()))
        .filter(device::Column::Status.eq("pending"))
        .exec(db)
        .await
        .context("failed to activate pending device")?;
    if result.rows_affected != 1 {
        return Ok(None);
    }
    load_device(db, device_id).await
}

pub async fn activate_pending_auth_session<C: ConnectionTrait>(
    db: &C,
    session_id: &AuthSessionId,
    refresh_expires_at: DateTimeWithTimeZone,
    now: DateTimeWithTimeZone,
) -> Result<Option<auth_session::Model>> {
    let result = auth_session::Entity::update_many()
        .col_expr(auth_session::Column::ActivatedAt, Expr::value(Some(now)))
        .col_expr(auth_session::Column::Status, Expr::value("active"))
        .col_expr(auth_session::Column::UpdatedAt, Expr::value(now))
        .col_expr(auth_session::Column::LastSeenAt, Expr::value(Some(now)))
        .col_expr(
            auth_session::Column::LastRefreshedAt,
            Expr::value(Some(now)),
        )
        .col_expr(
            auth_session::Column::RefreshExpiresAt,
            Expr::value(Some(refresh_expires_at)),
        )
        .filter(auth_session::Column::Id.eq(session_id.to_string()))
        .filter(auth_session::Column::Status.eq("pending"))
        .filter(auth_session::Column::ActivationExpiresAt.gt(now))
        .exec(db)
        .await
        .context("failed to activate pending auth session")?;
    if result.rows_affected != 1 {
        return Ok(None);
    }
    load_session(db, session_id).await
}

pub async fn insert_refresh_credential<C: ConnectionTrait>(
    db: &C,
    row: NewRefreshCredentialRow,
) -> Result<auth_refresh_credential::Model> {
    auth_refresh_credential::ActiveModel {
        id: Set(row.id.to_string()),
        session_id: Set(row.session_id.to_string()),
        token_family_id: Set(row.token_family_id.to_string()),
        generation: Set(i64::try_from(row.generation).context("refresh generation overflow")?),
        token_hash: Set(row.token_hash.to_vec()),
        status: Set(refresh_status_to_db(RefreshCredentialStatus::Current).to_owned()),
        issued_at: Set(row.issued_at),
        expires_at: Set(row.expires_at),
        consumed_at: Set(None),
        replaced_by_id: Set(None),
    }
    .insert(db)
    .await
    .context("failed to insert refresh credential")
}

pub async fn load_device<C: ConnectionTrait>(
    db: &C,
    id: &DeviceId,
) -> Result<Option<device::Model>> {
    device::Entity::find_by_id(id.to_string())
        .one(db)
        .await
        .context("failed to load device")
}

pub async fn load_active_device_by_installation<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
    installation_id: &str,
) -> Result<Option<device::Model>> {
    device::Entity::find()
        .filter(device::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(device::Column::PrincipalId.eq(principal_id.to_string()))
        .filter(device::Column::InstallationId.eq(installation_id))
        .filter(device::Column::Status.eq("active"))
        .one(db)
        .await
        .context("failed to load active device by installation")
}

pub async fn load_session<C: ConnectionTrait>(
    db: &C,
    id: &AuthSessionId,
) -> Result<Option<auth_session::Model>> {
    auth_session::Entity::find_by_id(id.to_string())
        .one(db)
        .await
        .context("failed to load auth session")
}

pub async fn load_active_session_by_device<C: ConnectionTrait>(
    db: &C,
    device_id: &DeviceId,
) -> Result<Option<auth_session::Model>> {
    auth_session::Entity::find()
        .filter(auth_session::Column::DeviceId.eq(device_id.to_string()))
        .filter(auth_session::Column::Status.eq("active"))
        .one(db)
        .await
        .context("failed to load active auth session for device")
}

pub async fn touch_active_device<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
    device_id: &DeviceId,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = device::Entity::update_many()
        .col_expr(device::Column::LastSeenAt, Expr::value(Some(now)))
        .col_expr(device::Column::UpdatedAt, Expr::value(now))
        .filter(device::Column::Id.eq(device_id.to_string()))
        .filter(device::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(device::Column::PrincipalId.eq(principal_id.to_string()))
        .filter(device::Column::Status.eq("active"))
        .exec(db)
        .await
        .context("failed to touch active device")?;
    Ok(result.rows_affected == 1)
}

pub async fn touch_active_auth_session<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
    device_id: &DeviceId,
    session_id: &AuthSessionId,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = auth_session::Entity::update_many()
        .col_expr(auth_session::Column::LastSeenAt, Expr::value(Some(now)))
        .col_expr(auth_session::Column::UpdatedAt, Expr::value(now))
        .filter(auth_session::Column::Id.eq(session_id.to_string()))
        .filter(auth_session::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(auth_session::Column::PrincipalId.eq(principal_id.to_string()))
        .filter(auth_session::Column::DeviceId.eq(device_id.to_string()))
        .filter(auth_session::Column::Status.eq("active"))
        .exec(db)
        .await
        .context("failed to touch active auth session")?;
    Ok(result.rows_affected == 1)
}

pub async fn load_refresh_by_hash<C: ConnectionTrait>(
    db: &C,
    token_hash: &[u8; 32],
) -> Result<Option<auth_refresh_credential::Model>> {
    auth_refresh_credential::Entity::find()
        .filter(auth_refresh_credential::Column::TokenHash.eq(token_hash.to_vec()))
        .one(db)
        .await
        .context("failed to lookup refresh credential")
}

pub async fn load_current_refresh<C: ConnectionTrait>(
    db: &C,
    session_id: &AuthSessionId,
) -> Result<Option<auth_refresh_credential::Model>> {
    auth_refresh_credential::Entity::find()
        .filter(auth_refresh_credential::Column::SessionId.eq(session_id.to_string()))
        .filter(auth_refresh_credential::Column::Status.eq("current"))
        .one(db)
        .await
        .context("failed to load current refresh credential")
}

pub async fn load_session_by_activation_hash<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    token_hash: &[u8; 32],
) -> Result<Option<auth_session::Model>> {
    let pending = auth_session::Entity::find()
        .filter(auth_session::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(auth_session::Column::ActivationTokenHash.eq(token_hash.to_vec()))
        .filter(auth_session::Column::Status.eq("pending"))
        .one(db)
        .await
        .context("failed to lookup pending device activation code")?;
    if pending.is_some() {
        return Ok(pending);
    }
    auth_session::Entity::find()
        .filter(auth_session::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(auth_session::Column::ActivationTokenHash.eq(token_hash.to_vec()))
        .order_by_desc(auth_session::Column::CreatedAt)
        .order_by_desc(auth_session::Column::Id)
        .one(db)
        .await
        .context("failed to lookup terminal device activation code")
}

pub async fn load_pending_session_by_activation_locator_hash<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    locator_hash: &[u8; 32],
) -> Result<Option<auth_session::Model>> {
    auth_session::Entity::find()
        .filter(auth_session::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(auth_session::Column::ActivationLocatorHash.eq(locator_hash.to_vec()))
        .filter(auth_session::Column::Status.eq("pending"))
        .one(db)
        .await
        .context("failed to lookup pending device activation request")
}

pub async fn list_pending_activation_locator_hashes<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
) -> Result<HashSet<Vec<u8>>> {
    let rows = auth_session::Entity::find()
        .filter(auth_session::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(auth_session::Column::Status.eq("pending"))
        .all(db)
        .await
        .context("failed to list pending device activation locators")?;
    Ok(rows
        .into_iter()
        .map(|session| session.activation_locator_hash)
        .collect())
}

pub async fn record_failed_device_activation<C: ConnectionTrait>(
    db: &C,
    session: auth_session::Model,
    now: DateTimeWithTimeZone,
) -> Result<DeviceActivationFailureOutcome> {
    if session.status != "pending"
        || session.activation_failed_attempts < 0
        || session.activation_failed_attempts >= i64::from(DEVICE_ACTIVATION_MAX_FAILED_ATTEMPTS)
    {
        bail!("cannot record a failed attempt for a terminal activation request");
    }
    let failed_attempts = session
        .activation_failed_attempts
        .checked_add(1)
        .context("device activation failed-attempt counter overflow")?;
    let device_id =
        DeviceId::new(session.device_id.clone()).context("invalid pending activation device id")?;
    let mut active: auth_session::ActiveModel = session.into();
    active.activation_failed_attempts = Set(failed_attempts);
    active.updated_at = Set(now);
    if failed_attempts >= i64::from(DEVICE_ACTIVATION_MAX_FAILED_ATTEMPTS) {
        active.status = Set(auth_session_status_to_db(AuthSessionStatus::Revoked).to_owned());
        active.revoked_at = Set(Some(now));
        active.revoke_reason = Set(Some(
            revoke_reason_to_db(AuthSessionRevokeReason::ActivationAttemptsExceeded).to_owned(),
        ));
    }
    active
        .update(db)
        .await
        .context("failed to record device activation attempt")?;
    let failed_attempts =
        u32::try_from(failed_attempts).context("device activation failed-attempt overflow")?;
    if failed_attempts < DEVICE_ACTIVATION_MAX_FAILED_ATTEMPTS {
        return Ok(DeviceActivationFailureOutcome::AttemptRecorded { failed_attempts });
    }
    if !mark_device_revoked(db, &device_id, now).await? {
        bail!("locked device activation request did not own one pending device");
    }
    Ok(DeviceActivationFailureOutcome::RequestRevoked { failed_attempts })
}

pub async fn load_pending_session_for_creator<C: ConnectionTrait>(
    db: &C,
    creator_session_id: &AuthSessionId,
) -> Result<Option<auth_session::Model>> {
    auth_session::Entity::find()
        .filter(auth_session::Column::CreatedBySessionId.eq(creator_session_id.to_string()))
        .filter(auth_session::Column::Status.eq("pending"))
        .one(db)
        .await
        .context("failed to load pending session for creator")
}

pub async fn load_pending_local_session<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
) -> Result<Option<auth_session::Model>> {
    auth_session::Entity::find()
        .filter(auth_session::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(auth_session::Column::PrincipalId.eq(principal_id.to_string()))
        .filter(auth_session::Column::CreatedBySessionId.is_null())
        .filter(auth_session::Column::Status.eq("pending"))
        .one(db)
        .await
        .context("failed to load pending local session")
}

pub async fn expire_pending_auth_session<C: ConnectionTrait>(
    db: &C,
    session_id: &AuthSessionId,
    device_id: &DeviceId,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let session = auth_session::Entity::update_many()
        .col_expr(auth_session::Column::Status, Expr::value("expired"))
        .col_expr(auth_session::Column::UpdatedAt, Expr::value(now))
        .col_expr(auth_session::Column::RevokedAt, Expr::value(Some(now)))
        .filter(auth_session::Column::Id.eq(session_id.to_string()))
        .filter(auth_session::Column::DeviceId.eq(device_id.to_string()))
        .filter(auth_session::Column::Status.eq("pending"))
        .filter(auth_session::Column::ActivationExpiresAt.lte(now))
        .exec(db)
        .await
        .context("failed to expire pending auth session")?;
    if session.rows_affected != 1 {
        return Ok(false);
    }
    let device = device::Entity::update_many()
        .col_expr(device::Column::Status, Expr::value("revoked"))
        .col_expr(device::Column::UpdatedAt, Expr::value(now))
        .col_expr(device::Column::RevokedAt, Expr::value(Some(now)))
        .filter(device::Column::Id.eq(device_id.to_string()))
        .filter(device::Column::Status.eq("pending"))
        .exec(db)
        .await
        .context("failed to expire pending device")?;
    if device.rows_affected != 1 {
        bail!("expired pending auth session did not own one pending device");
    }
    Ok(true)
}

pub async fn list_sessions_for_principal<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
) -> Result<Vec<auth_session::Model>> {
    auth_session::Entity::find()
        .filter(auth_session::Column::PrincipalId.eq(principal_id.to_string()))
        .filter(auth_session::Column::Status.ne("pending"))
        .filter(auth_session::Column::ActivatedAt.is_not_null())
        .order_by_desc(auth_session::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list auth sessions")
}

pub async fn mark_session_revoked<C: ConnectionTrait>(
    db: &C,
    session: auth_session::Model,
    reason: AuthSessionRevokeReason,
    now: DateTimeWithTimeZone,
) -> Result<auth_session::Model> {
    let mut active: auth_session::ActiveModel = session.into();
    active.status = Set(auth_session_status_to_db(AuthSessionStatus::Revoked).to_owned());
    active.updated_at = Set(now);
    active.revoked_at = Set(Some(now));
    active.revoke_reason = Set(Some(revoke_reason_to_db(reason).to_owned()));
    active
        .update(db)
        .await
        .context("failed to revoke auth session")
}

pub async fn mark_device_revoked<C: ConnectionTrait>(
    db: &C,
    device_id: &DeviceId,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = device::Entity::update_many()
        .col_expr(device::Column::Status, Expr::value("revoked"))
        .col_expr(device::Column::UpdatedAt, Expr::value(now))
        .col_expr(device::Column::RevokedAt, Expr::value(Some(now)))
        .filter(device::Column::Id.eq(device_id.to_string()))
        .filter(device::Column::Status.ne("revoked"))
        .exec(db)
        .await
        .context("failed to revoke device")?;
    Ok(result.rows_affected == 1)
}

pub async fn revoke_current_refresh_for_session<C: ConnectionTrait>(
    db: &C,
    session_id: &AuthSessionId,
) -> Result<u64> {
    let result = auth_refresh_credential::Entity::update_many()
        .col_expr(
            auth_refresh_credential::Column::Status,
            Expr::value(refresh_status_to_db(RefreshCredentialStatus::Revoked)),
        )
        .filter(auth_refresh_credential::Column::SessionId.eq(session_id.to_string()))
        .filter(auth_refresh_credential::Column::Status.eq("current"))
        .exec(db)
        .await
        .context("failed to revoke current refresh credential")?;
    Ok(result.rows_affected)
}

pub async fn expire_session_refresh_family<C: ConnectionTrait>(
    db: &C,
    session_id: &AuthSessionId,
    token_family_id: &TokenFamilyId,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let session = auth_session::Entity::update_many()
        .col_expr(
            auth_session::Column::Status,
            Expr::value(auth_session_status_to_db(AuthSessionStatus::Expired)),
        )
        .col_expr(auth_session::Column::UpdatedAt, Expr::value(now))
        .col_expr(auth_session::Column::RevokedAt, Expr::value(Some(now)))
        .col_expr(
            auth_session::Column::RevokeReason,
            Expr::value(None::<String>),
        )
        .filter(auth_session::Column::Id.eq(session_id.to_string()))
        .filter(auth_session::Column::TokenFamilyId.eq(token_family_id.to_string()))
        .filter(auth_session::Column::Status.eq("active"))
        .exec(db)
        .await
        .context("failed to expire auth session")?;
    auth_refresh_credential::Entity::update_many()
        .col_expr(
            auth_refresh_credential::Column::Status,
            Expr::value(refresh_status_to_db(RefreshCredentialStatus::Expired)),
        )
        .filter(auth_refresh_credential::Column::SessionId.eq(session_id.to_string()))
        .filter(auth_refresh_credential::Column::TokenFamilyId.eq(token_family_id.to_string()))
        .filter(auth_refresh_credential::Column::Status.eq("current"))
        .exec(db)
        .await
        .context("failed to expire current refresh credential")?;
    Ok(session.rows_affected == 1)
}

pub async fn expire_stale_auth_sessions<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    batch_size: u64,
) -> Result<Vec<AuthSessionId>> {
    if batch_size == 0 {
        return Ok(Vec::new());
    }
    let rows = auth_session::Entity::find()
        .filter(auth_session::Column::Status.eq("active"))
        .filter(auth_session::Column::RefreshExpiresAt.lte(now))
        .order_by_asc(auth_session::Column::RefreshExpiresAt)
        .order_by_asc(auth_session::Column::Id)
        .limit(batch_size)
        .all(db)
        .await
        .context("failed to list expired active auth sessions")?;
    let mut expired = Vec::with_capacity(rows.len());
    for row in rows {
        let device_id = DeviceId::new(row.device_id.clone())
            .context("expired auth session has an invalid device id")?;
        let session_id =
            AuthSessionId::new(row.id).context("expired auth session has an invalid id")?;
        let token_family_id = TokenFamilyId::new(row.token_family_id)
            .context("expired auth session has an invalid token family id")?;
        if expire_session_refresh_family(db, &session_id, &token_family_id, now).await? {
            mark_device_revoked(db, &device_id, now).await?;
            expired.push(session_id);
        }
    }
    Ok(expired)
}

pub async fn expire_stale_pending_sessions<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    batch_size: u64,
) -> Result<Vec<AuthSessionId>> {
    if batch_size == 0 {
        return Ok(Vec::new());
    }
    let rows = auth_session::Entity::find()
        .filter(auth_session::Column::Status.eq("pending"))
        .filter(auth_session::Column::ActivationExpiresAt.lte(now))
        .order_by_asc(auth_session::Column::ActivationExpiresAt)
        .order_by_asc(auth_session::Column::Id)
        .limit(batch_size)
        .all(db)
        .await
        .context("failed to list expired pending auth sessions")?;
    let mut expired = Vec::with_capacity(rows.len());
    for row in rows {
        let device_id =
            DeviceId::new(row.device_id).context("pending session has an invalid device id")?;
        let session_id =
            AuthSessionId::new(row.id).context("pending session has an invalid auth session id")?;
        if expire_pending_auth_session(db, &session_id, &device_id, now).await? {
            expired.push(session_id);
        }
    }
    Ok(expired)
}

pub async fn rotate_current_refresh<C: ConnectionTrait>(
    db: &C,
    current_id: &RefreshCredentialId,
    expected_generation: u64,
    replacement_id: &RefreshCredentialId,
    consumed_at: DateTimeWithTimeZone,
    retain_until: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = auth_refresh_credential::Entity::update_many()
        .col_expr(
            auth_refresh_credential::Column::Status,
            Expr::value(refresh_status_to_db(RefreshCredentialStatus::Rotated)),
        )
        .col_expr(
            auth_refresh_credential::Column::ConsumedAt,
            Expr::value(Some(consumed_at)),
        )
        .col_expr(
            auth_refresh_credential::Column::ReplacedById,
            Expr::value(Some(replacement_id.to_string())),
        )
        .col_expr(
            auth_refresh_credential::Column::ExpiresAt,
            Expr::value(retain_until),
        )
        .filter(auth_refresh_credential::Column::Id.eq(current_id.to_string()))
        .filter(auth_refresh_credential::Column::Status.eq("current"))
        .filter(
            auth_refresh_credential::Column::Generation
                .eq(i64::try_from(expected_generation).context("refresh generation overflow")?),
        )
        .exec(db)
        .await
        .context("failed to rotate current refresh credential")?;
    Ok(result.rows_affected == 1)
}

pub async fn advance_auth_session_refresh<C: ConnectionTrait>(
    db: &C,
    session_id: &AuthSessionId,
    expected_generation: u64,
    next_generation: u64,
    refresh_expires_at: DateTimeWithTimeZone,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = auth_session::Entity::update_many()
        .col_expr(
            auth_session::Column::RefreshGeneration,
            Expr::value(i64::try_from(next_generation).context("refresh generation overflow")?),
        )
        .col_expr(
            auth_session::Column::RefreshExpiresAt,
            Expr::value(Some(refresh_expires_at)),
        )
        .col_expr(
            auth_session::Column::LastRefreshedAt,
            Expr::value(Some(now)),
        )
        .col_expr(auth_session::Column::UpdatedAt, Expr::value(now))
        .filter(auth_session::Column::Id.eq(session_id.to_string()))
        .filter(auth_session::Column::Status.eq("active"))
        .filter(
            auth_session::Column::RefreshGeneration
                .eq(i64::try_from(expected_generation).context("refresh generation overflow")?),
        )
        .exec(db)
        .await
        .context("failed to advance auth session refresh generation")?;
    Ok(result.rows_affected == 1)
}

pub async fn revoke_session_family_for_refresh_reuse<C: ConnectionTrait>(
    db: &C,
    session_id: &AuthSessionId,
    token_family_id: &TokenFamilyId,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let session = auth_session::Entity::update_many()
        .col_expr(auth_session::Column::Status, Expr::value("revoked"))
        .col_expr(auth_session::Column::UpdatedAt, Expr::value(now))
        .col_expr(auth_session::Column::RevokedAt, Expr::value(Some(now)))
        .col_expr(
            auth_session::Column::RevokeReason,
            Expr::value(Some("refresh_reuse")),
        )
        .filter(auth_session::Column::Id.eq(session_id.to_string()))
        .filter(auth_session::Column::TokenFamilyId.eq(token_family_id.to_string()))
        .exec(db)
        .await
        .context("failed to revoke compromised auth session")?;
    auth_refresh_credential::Entity::update_many()
        .col_expr(
            auth_refresh_credential::Column::Status,
            Expr::value("revoked"),
        )
        .filter(auth_refresh_credential::Column::TokenFamilyId.eq(token_family_id.to_string()))
        .filter(auth_refresh_credential::Column::Status.eq("current"))
        .exec(db)
        .await
        .context("failed to revoke compromised refresh branch")?;
    Ok(session.rows_affected == 1)
}

pub async fn cleanup_terminal_refresh_evidence<C: ConnectionTrait>(
    db: &C,
    expired_before: DateTimeWithTimeZone,
    batch_size: u64,
) -> Result<u64> {
    if batch_size == 0 {
        return Ok(0);
    }
    let statement = Statement::from_sql_and_values(
        db.get_database_backend(),
        "DELETE FROM auth_refresh_credential WHERE id IN (\
            SELECT candidate.id FROM auth_refresh_credential candidate \
            WHERE candidate.status IN ('rotated', 'revoked', 'expired') \
            AND candidate.expires_at < ? \
            AND NOT EXISTS (SELECT 1 FROM auth_refresh_credential predecessor \
                WHERE predecessor.replaced_by_id = candidate.id) \
            ORDER BY candidate.expires_at ASC, candidate.generation ASC LIMIT ?\
        )",
        [
            expired_before.into(),
            i64::try_from(batch_size).unwrap_or(i64::MAX).into(),
        ],
    );
    db.execute_raw(statement)
        .await
        .map(|result| result.rows_affected())
        .context("failed to clean retained refresh evidence")
}

pub async fn scan_auth_persistence_invariants<C: ConnectionTrait>(
    db: &C,
) -> Result<AuthPersistenceInvariantReport> {
    let mut violations = scan_auth_schema_invariants(db).await?;
    let gateways = gateway_identity::Entity::find().all(db).await?;
    let principals = gateway_principal::Entity::find().all(db).await?;
    let devices = device::Entity::find().all(db).await?;
    let sessions = auth_session::Entity::find().all(db).await?;
    let refreshes = auth_refresh_credential::Entity::find().all(db).await?;

    let gateway_ids = gateways
        .iter()
        .map(|row| row.id.as_str())
        .collect::<HashSet<_>>();
    let principal_by_id = principals
        .iter()
        .map(|row| (row.id.as_str(), row))
        .collect::<HashMap<_, _>>();
    let device_by_id = devices
        .iter()
        .map(|row| (row.id.as_str(), row))
        .collect::<HashMap<_, _>>();
    let session_by_id = sessions
        .iter()
        .map(|row| (row.id.as_str(), row))
        .collect::<HashMap<_, _>>();
    let refresh_by_id = refreshes
        .iter()
        .map(|row| (row.id.as_str(), row))
        .collect::<HashMap<_, _>>();
    let mut active_installations = HashSet::new();

    for row in &devices {
        let metadata_present = row.installation_id.is_some()
            && row.display_name.is_some()
            && row.client_kind.is_some();
        let metadata_absent = row.installation_id.is_none()
            && row.display_name.is_none()
            && row.client_kind.is_none()
            && row.platform.is_none()
            && row.client_version.is_none();
        let invalid_domain = DeviceId::new(row.id.clone()).is_err()
            || GatewayId::new(row.gateway_id.clone()).is_err()
            || PrincipalId::new(row.principal_id.clone()).is_err()
            || row
                .installation_id
                .as_deref()
                .is_some_and(|value| !valid_bounded_text(value, 255))
            || row
                .display_name
                .as_deref()
                .is_some_and(|value| !valid_bounded_text(value, 255))
            || row
                .platform
                .as_deref()
                .is_some_and(|value| !valid_bounded_text(value, 255))
            || row
                .client_version
                .as_deref()
                .is_some_and(|value| !valid_bounded_text(value, 255))
            || row
                .client_kind
                .as_deref()
                .is_some_and(|value| !matches!(value, "desktop" | "mobile" | "other"))
            || (!metadata_present && !metadata_absent);
        if invalid_domain {
            violations.push(format!("device `{}` has invalid domain data", row.id));
        }
        if row.status == "active" {
            if let Some(installation_id) = row.installation_id.as_deref()
                && !active_installations.insert((
                    row.gateway_id.as_str(),
                    row.principal_id.as_str(),
                    installation_id,
                ))
            {
                violations.push(format!(
                    "installation `{installation_id}` has multiple active devices"
                ));
            }
        }
        let owner = principal_by_id.get(row.principal_id.as_str());
        if !gateway_ids.contains(row.gateway_id.as_str())
            || owner.is_none_or(|owner| owner.gateway_id != row.gateway_id)
        {
            violations.push(format!("device `{}` has mismatched ownership", row.id));
        }
        let valid_state = match row.status.as_str() {
            "pending" => metadata_absent && row.last_seen_at.is_none() && row.revoked_at.is_none(),
            "active" => metadata_present && row.last_seen_at.is_some() && row.revoked_at.is_none(),
            "revoked" => {
                row.revoked_at.is_some()
                    && ((metadata_absent && row.last_seen_at.is_none())
                        || (metadata_present && row.last_seen_at.is_some()))
            }
            _ => false,
        };
        if !valid_state {
            violations.push(format!("device `{}` has impossible revoke state", row.id));
        }
    }

    let mut active_devices = HashSet::new();
    let mut pending_creators = HashSet::new();
    let mut pending_local_principals = HashSet::new();
    let mut pending_activation_locator_hashes = HashSet::new();
    let mut pending_activation_hashes = HashSet::new();
    let mut families = HashSet::new();
    let mut sessions_by_device = HashMap::<&str, Vec<&auth_session::Model>>::new();
    for row in &sessions {
        if AuthSessionId::new(row.id.clone()).is_err()
            || GatewayId::new(row.gateway_id.clone()).is_err()
            || PrincipalId::new(row.principal_id.clone()).is_err()
            || DeviceId::new(row.device_id.clone()).is_err()
            || TokenFamilyId::new(row.token_family_id.clone()).is_err()
            || row
                .created_by_session_id
                .as_ref()
                .is_some_and(|id| AuthSessionId::new(id.clone()).is_err())
            || row.activation_token_hash.len() != 32
            || row.activation_locator_hash.len() != 32
            || row.activation_failed_attempts < 0
            || row.activation_failed_attempts > i64::from(DEVICE_ACTIVATION_MAX_FAILED_ATTEMPTS)
        {
            violations.push(format!("session `{}` has invalid domain IDs", row.id));
        }
        sessions_by_device
            .entry(row.device_id.as_str())
            .or_default()
            .push(row);
        let owner = principal_by_id.get(row.principal_id.as_str());
        let owned_device = device_by_id.get(row.device_id.as_str());
        if !gateway_ids.contains(row.gateway_id.as_str())
            || owner.is_none_or(|owner| owner.gateway_id != row.gateway_id)
            || owned_device.is_none_or(|device| {
                device.gateway_id != row.gateway_id || device.principal_id != row.principal_id
            })
        {
            violations.push(format!("session `{}` has mismatched ownership", row.id));
        }
        if let Some(creator_session_id) = row.created_by_session_id.as_deref() {
            let creator = session_by_id.get(creator_session_id);
            if creator.is_none_or(|creator| {
                creator.gateway_id != row.gateway_id || creator.principal_id != row.principal_id
            }) {
                violations.push(format!(
                    "session `{}` has a mismatched creator session",
                    row.id
                ));
            }
            if row.status == "pending" && !pending_creators.insert(creator_session_id) {
                violations.push(format!(
                    "session `{creator_session_id}` created multiple pending sessions"
                ));
            }
        } else if row.status == "pending"
            && !pending_local_principals
                .insert((row.gateway_id.as_str(), row.principal_id.as_str()))
        {
            violations.push(format!(
                "principal `{}` has multiple pending local sessions",
                row.principal_id
            ));
        }
        if row.status == "pending"
            && !pending_activation_locator_hashes.insert((
                row.gateway_id.as_str(),
                row.activation_locator_hash.as_slice(),
            ))
        {
            violations.push(format!(
                "Gateway `{}` has duplicate pending activation locator",
                row.gateway_id
            ));
        }
        if row.status == "active" && !active_devices.insert(row.device_id.as_str()) {
            violations.push(format!(
                "device `{}` has multiple active sessions",
                row.device_id
            ));
        }
        if !families.insert(row.token_family_id.as_str()) {
            violations.push(format!("duplicate token family `{}`", row.token_family_id));
        }
        if row.status == "pending"
            && !pending_activation_hashes.insert(row.activation_token_hash.as_slice())
        {
            violations.push("multiple pending sessions share one activation token hash".to_owned());
        }
        if row.status == "pending" && owned_device.is_some_and(|device| device.status != "pending")
        {
            violations.push(format!(
                "pending session `{}` does not own a pending device",
                row.id
            ));
        }
        if row.status == "active" && owned_device.is_some_and(|device| device.status != "active") {
            violations.push(format!(
                "active session `{}` belongs to a revoked device",
                row.id
            ));
        }
        if matches!(row.status.as_str(), "revoked" | "expired")
            && owned_device.is_some_and(|device| device.status != "revoked")
        {
            violations.push(format!(
                "terminal session `{}` does not own a revoked device",
                row.id
            ));
        }
        let runtime_absent = row.activated_at.is_none()
            && row.last_seen_at.is_none()
            && row.last_refreshed_at.is_none()
            && row.refresh_expires_at.is_none();
        let runtime_present = row.activated_at.is_some()
            && row.last_seen_at.is_some()
            && row.last_refreshed_at.is_some()
            && row.refresh_expires_at.is_some();
        let valid_state = match row.status.as_str() {
            "pending" => {
                row.activation_failed_attempts < i64::from(DEVICE_ACTIVATION_MAX_FAILED_ATTEMPTS)
                    && runtime_absent
                    && row.revoked_at.is_none()
                    && row.revoke_reason.is_none()
            }
            "active" => {
                row.activation_failed_attempts < i64::from(DEVICE_ACTIVATION_MAX_FAILED_ATTEMPTS)
                    && runtime_present
                    && row.revoked_at.is_none()
                    && row.revoke_reason.is_none()
            }
            "revoked" => {
                row.revoked_at.is_some()
                    && row
                        .revoke_reason
                        .as_deref()
                        .is_some_and(valid_session_revoke_reason)
            }
            "expired" => row.revoked_at.is_some() && row.revoke_reason.is_none(),
            _ => false,
        };
        if !valid_state {
            violations.push(format!("session `{}` has impossible revoke state", row.id));
        }
        if row.refresh_generation < 0
            || row.activation_expires_at < row.created_at
            || row
                .refresh_expires_at
                .is_some_and(|expires_at| expires_at < row.created_at)
            || (!runtime_absent && !runtime_present)
        {
            violations.push(format!("session `{}` has impossible refresh state", row.id));
        }
    }

    for row in &devices {
        let owned_sessions = sessions_by_device
            .get(row.id.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if owned_sessions.is_empty() {
            violations.push(format!("device `{}` has no auth session", row.id));
        }
        if row.status == "active"
            && !owned_sessions
                .iter()
                .any(|session| session.status == "active")
        {
            violations.push(format!("active device `{}` has no active session", row.id));
        }
        if row.status == "pending"
            && !owned_sessions
                .iter()
                .any(|session| session.status == "pending")
        {
            violations.push(format!(
                "pending device `{}` has no pending session",
                row.id
            ));
        }
    }

    let mut current_sessions = HashSet::new();
    let mut refresh_hashes = HashSet::new();
    let mut refresh_generations = HashSet::new();
    let mut refreshes_by_session = HashMap::<&str, Vec<&auth_refresh_credential::Model>>::new();
    for row in &refreshes {
        if RefreshCredentialId::new(row.id.clone()).is_err()
            || AuthSessionId::new(row.session_id.clone()).is_err()
            || TokenFamilyId::new(row.token_family_id.clone()).is_err()
            || row
                .replaced_by_id
                .as_ref()
                .is_some_and(|id| RefreshCredentialId::new(id.clone()).is_err())
        {
            violations.push(format!("refresh `{}` has invalid domain IDs", row.id));
        }
        let session = session_by_id.get(row.session_id.as_str());
        refreshes_by_session
            .entry(row.session_id.as_str())
            .or_default()
            .push(row);
        if !refresh_hashes.insert(row.token_hash.as_slice()) {
            violations.push("multiple refresh credentials share one token hash".to_owned());
        }
        if !refresh_generations.insert((row.session_id.as_str(), row.generation)) {
            violations.push(format!(
                "session `{}` has duplicate refresh generation {}",
                row.session_id, row.generation
            ));
        }
        if session.is_none_or(|session| session.token_family_id != row.token_family_id) {
            violations.push(format!("refresh `{}` has mismatched ownership", row.id));
        }
        if row.generation < 0
            || row.token_hash.len() != 32
            || row.expires_at < row.issued_at
            || session.is_some_and(|session| row.generation > session.refresh_generation)
        {
            violations.push(format!(
                "refresh `{}` has impossible generation state",
                row.id
            ));
        }
        match row.status.as_str() {
            "current" => {
                if row.consumed_at.is_some() || row.replaced_by_id.is_some() {
                    violations.push(format!(
                        "current refresh `{}` has terminal metadata",
                        row.id
                    ));
                }
                if !current_sessions.insert(row.session_id.as_str()) {
                    violations.push(format!(
                        "session `{}` has multiple current refreshes",
                        row.session_id
                    ));
                }
                if session.is_some_and(|session| {
                    session.status != "active"
                        || session.refresh_generation != row.generation
                        || session.refresh_expires_at.as_ref() != Some(&row.expires_at)
                }) {
                    violations.push(format!(
                        "current refresh `{}` does not match its active session",
                        row.id
                    ));
                }
            }
            "rotated" => {
                let successor = row
                    .replaced_by_id
                    .as_deref()
                    .and_then(|id| refresh_by_id.get(id).copied());
                if row.consumed_at.is_none() || successor.is_none() {
                    violations.push(format!(
                        "rotated refresh `{}` has a dangling replacement",
                        row.id
                    ));
                }
                if successor.is_some_and(|successor| {
                    successor.id == row.id
                        || successor.session_id != row.session_id
                        || successor.token_family_id != row.token_family_id
                        || row.generation.checked_add(1) != Some(successor.generation)
                        || row.expires_at < successor.expires_at
                        || row
                            .consumed_at
                            .is_some_and(|consumed| successor.issued_at < consumed)
                }) {
                    violations.push(format!(
                        "rotated refresh `{}` has an invalid successor",
                        row.id
                    ));
                }
            }
            "revoked" | "expired" => {
                if row.consumed_at.is_some() || row.replaced_by_id.is_some() {
                    violations.push(format!(
                        "terminal refresh `{}` has rotation metadata",
                        row.id
                    ));
                }
            }
            _ => violations.push(format!("refresh `{}` has an unknown status", row.id)),
        }
    }

    for row in &sessions {
        let session_refreshes = refreshes_by_session
            .get(row.id.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let generation_row = session_refreshes
            .iter()
            .find(|refresh| refresh.generation == row.refresh_generation);
        if generation_row.is_none() && (row.status == "active" || !session_refreshes.is_empty()) {
            violations.push(format!(
                "session `{}` has no refresh at its recorded generation",
                row.id
            ));
        }
        if row.status == "active" && !current_sessions.contains(row.id.as_str()) {
            violations.push(format!(
                "active session `{}` has no current refresh credential",
                row.id
            ));
        }
        if row.status == "pending" && !session_refreshes.is_empty() {
            violations.push(format!(
                "pending session `{}` already has refresh credentials",
                row.id
            ));
        }
        if row.status != "active" && current_sessions.contains(row.id.as_str()) {
            violations.push(format!(
                "terminal session `{}` still has a current refresh credential",
                row.id
            ));
        }
        if row.status == "revoked"
            && generation_row.is_some_and(|refresh| refresh.status != "revoked")
        {
            violations.push(format!(
                "revoked session `{}` has a non-revoked latest refresh",
                row.id
            ));
        }
        if row.status == "expired"
            && generation_row.is_some_and(|refresh| refresh.status != "expired")
        {
            violations.push(format!(
                "expired session `{}` has a non-expired latest refresh",
                row.id
            ));
        }
    }

    const MAX_REPORTED_AUTH_INVARIANTS: usize = 64;
    if violations.len() > MAX_REPORTED_AUTH_INVARIANTS {
        violations.truncate(MAX_REPORTED_AUTH_INVARIANTS);
        violations.push("additional auth persistence violations omitted".to_owned());
    }
    Ok(AuthPersistenceInvariantReport { violations })
}

async fn scan_auth_schema_invariants<C: ConnectionTrait>(db: &C) -> Result<Vec<String>> {
    if db.get_database_backend() != DbBackend::Sqlite {
        return Ok(vec![
            "auth persistence schema requires the SQLite backend".to_owned(),
        ]);
    }

    const REQUIRED_INDEXES: &[&str] = &[
        "idx_device_owner_status",
        "idx_device_active_installation",
        "idx_device_principal_last_seen",
        "idx_auth_session_token_family",
        "idx_auth_session_activation_hash",
        "idx_auth_session_pending_activation_locator_hash",
        "idx_auth_session_pending_creator",
        "idx_auth_session_pending_local",
        "idx_auth_session_active_device",
        "idx_auth_session_principal_status",
        "idx_auth_session_device_status",
        "idx_auth_session_expiry_status",
        "idx_auth_refresh_token_hash",
        "idx_auth_refresh_generation",
        "idx_auth_refresh_current",
        "idx_auth_refresh_family_status",
        "idx_auth_refresh_expiry_status",
    ];
    const REQUIRED_TABLE_CONSTRAINTS: &[(&str, &[&str])] = &[
        (
            "device",
            &[
                "ck_device_ids",
                "ck_device_installation",
                "ck_device_display_name",
                "ck_device_client_kind",
                "ck_device_metadata_group",
                "ck_device_status",
                "ck_device_state",
            ],
        ),
        (
            "auth_session",
            &[
                "ck_auth_session_ids",
                "ck_auth_session_activation_hash",
                "ck_auth_session_activation_locator_hash",
                "ck_auth_session_activation_attempts",
                "ck_auth_session_activation_expiry",
                "ck_auth_session_status",
                "ck_auth_session_generation",
                "ck_auth_session_refresh_expiry",
                "ck_auth_session_runtime_group",
                "ck_auth_session_state",
                "ck_auth_session_revoke_reason",
            ],
        ),
        (
            "auth_refresh_credential",
            &[
                "ck_auth_refresh_ids",
                "ck_auth_refresh_generation",
                "ck_auth_refresh_hash",
                "ck_auth_refresh_expiry",
                "ck_auth_refresh_status",
                "ck_auth_refresh_terminal",
            ],
        ),
    ];

    let index_rows = db
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE type = 'index'".to_owned(),
        ))
        .await
        .context("failed to inspect auth persistence indexes")?;
    let index_names = index_rows
        .iter()
        .map(|row| row.try_get::<String>("", "name"))
        .collect::<std::result::Result<HashSet<_>, _>>()
        .context("failed to decode auth persistence index metadata")?;

    let table_rows = db
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT name, sql FROM sqlite_master WHERE type = 'table'".to_owned(),
        ))
        .await
        .context("failed to inspect auth persistence tables")?;
    let mut table_sql = HashMap::new();
    for row in &table_rows {
        let name = row
            .try_get::<String>("", "name")
            .context("failed to decode auth persistence table name")?;
        let sql = row
            .try_get::<Option<String>>("", "sql")
            .context("failed to decode auth persistence table definition")?
            .unwrap_or_default();
        table_sql.insert(name, sql);
    }

    let mut violations = Vec::new();
    for required in REQUIRED_INDEXES {
        if !index_names.contains(*required) {
            violations.push(format!("required auth index `{required}` is missing"));
        }
    }
    for (table, constraints) in REQUIRED_TABLE_CONSTRAINTS {
        let Some(sql) = table_sql.get(*table) else {
            violations.push(format!("required auth table `{table}` is missing"));
            continue;
        };
        for constraint in *constraints {
            if !sql.contains(constraint) {
                violations.push(format!(
                    "required auth constraint `{constraint}` is missing from `{table}`"
                ));
            }
        }
    }
    Ok(violations)
}

fn valid_session_revoke_reason(reason: &str) -> bool {
    matches!(
        reason,
        "logout"
            | "self_revoke"
            | "device_revoke"
            | "activation_attempts_exceeded"
            | "refresh_reuse"
            | "principal_suspended"
            | "principal_removed"
            | "superseded"
            | "security_reset"
    )
}

fn valid_bounded_text(value: &str, max_chars: usize) -> bool {
    value.trim() == value
        && !value.is_empty()
        && value.chars().count() <= max_chars
        && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{Database, Statement};

    use super::*;

    async fn fixture() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("database");
        Migrator::up(&db, None).await.expect("migrations");
        db.execute_unprepared(
            "INSERT INTO gateway_identity(id, singleton_key, identity_bootstrap_version, auth_schema_version, created_at, updated_at) VALUES ('G00000000000000000001', 1, 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .expect("identity");
        db.execute_unprepared(
            "INSERT INTO gateway_principal(id, gateway_id, kind, role_key, status, display_name, nickname, nickname_key, created_at, updated_at, removed_at) VALUES ('P00000000000000000001', 'G00000000000000000001', 'superuser', NULL, 'active', 'Superuser', 'superuser', 'superuser', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)",
        )
        .await
        .expect("principal");
        db
    }

    async fn valid_auth_fixture() -> sea_orm::DatabaseConnection {
        let db = fixture().await;
        insert_active_session_fixture(
            &db,
            "D00000000000000000001",
            "S00000000000000000001",
            "F00000000000000000001",
            "R00000000000000000001",
            "desktop-a",
            ClientKind::Desktop,
            [1; 32],
            [11; 32],
        )
        .await;
        assert!(
            scan_auth_persistence_invariants(&db)
                .await
                .expect("valid invariant scan")
                .is_valid()
        );
        db
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_active_session_fixture(
        db: &sea_orm::DatabaseConnection,
        device_raw: &str,
        session_raw: &str,
        family_raw: &str,
        refresh_raw: &str,
        installation_id: &str,
        client_kind: ClientKind,
        activation_hash: [u8; 32],
        refresh_hash: [u8; 32],
    ) {
        let now = chrono::Utc::now().fixed_offset();
        let refresh_expires_at = now + chrono::Duration::days(90);
        let device_id = DeviceId::new(device_raw).unwrap();
        let session_id = AuthSessionId::new(session_raw).unwrap();
        let token_family_id = TokenFamilyId::new(family_raw).unwrap();
        insert_pending_device_session(
            db,
            NewPendingDeviceSessionRow {
                device_id: device_id.clone(),
                session_id: session_id.clone(),
                gateway_id: gateway_id(),
                principal_id: principal_id(),
                token_family_id: token_family_id.clone(),
                issuer: PendingSessionIssuer::LocalCli,
                activation_token_hash: activation_hash,
                activation_locator_hash: [17; 32],
                activation_expires_at: now + chrono::Duration::minutes(10),
                now,
            },
        )
        .await
        .unwrap();
        let installation = ClientInstallationDescriptor {
            installation_id: installation_id.to_owned(),
            display_name: installation_id.to_owned(),
            client_kind,
            platform: None,
            client_version: None,
        };
        activate_pending_device(db, &device_id, &installation, now)
            .await
            .unwrap()
            .expect("pending device activates");
        activate_pending_auth_session(db, &session_id, refresh_expires_at, now)
            .await
            .unwrap()
            .expect("pending session activates");
        insert_refresh_credential(
            db,
            NewRefreshCredentialRow {
                id: RefreshCredentialId::new(refresh_raw).unwrap(),
                session_id,
                token_family_id,
                generation: 0,
                token_hash: refresh_hash,
                issued_at: now,
                expires_at: refresh_expires_at,
            },
        )
        .await
        .unwrap();
    }

    fn gateway_id() -> GatewayId {
        GatewayId::new("G00000000000000000001").unwrap()
    }

    fn principal_id() -> PrincipalId {
        PrincipalId::new("P00000000000000000001").unwrap()
    }

    #[tokio::test]
    async fn repositories_support_two_independent_devices_and_refresh_history() {
        let db = fixture().await;

        for (device_raw, session_raw, installation) in [
            (
                "D00000000000000000001",
                "S00000000000000000001",
                "desktop-a",
            ),
            ("D00000000000000000002", "S00000000000000000002", "mobile-b"),
        ] {
            let suffix: u8 = if installation == "desktop-a" { 1 } else { 2 };
            insert_active_session_fixture(
                &db,
                device_raw,
                session_raw,
                if suffix == 1 {
                    "F00000000000000000001"
                } else {
                    "F00000000000000000002"
                },
                if suffix == 1 {
                    "R00000000000000000001"
                } else {
                    "R00000000000000000002"
                },
                installation,
                ClientKind::Other,
                [suffix; 32],
                [suffix + 10; 32],
            )
            .await;
        }

        assert_eq!(
            list_sessions_for_principal(&db, &principal_id())
                .await
                .unwrap()
                .len(),
            2
        );
        assert!(
            scan_auth_persistence_invariants(&db)
                .await
                .unwrap()
                .is_valid()
        );

        let now = chrono::Utc::now().fixed_offset();
        let expires = now + chrono::Duration::days(90);
        let duplicate = insert_refresh_credential(
            &db,
            NewRefreshCredentialRow {
                id: RefreshCredentialId::new("R00000000000000000003").unwrap(),
                session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
                token_family_id: TokenFamilyId::new("S00000000000000000001").unwrap(),
                generation: 1,
                token_hash: [3; 32],
                issued_at: now,
                expires_at: expires,
            },
        )
        .await;
        assert!(
            duplicate.is_err(),
            "database must reject a second current branch"
        );
    }

    #[tokio::test]
    async fn invariant_scan_fails_closed_for_dangling_owner() {
        let db = fixture().await;
        insert_active_session_fixture(
            &db,
            "D00000000000000000001",
            "S00000000000000000001",
            "F00000000000000000001",
            "R00000000000000000001",
            "desktop-a",
            ClientKind::Desktop,
            [1; 32],
            [11; 32],
        )
        .await;
        db.execute_unprepared("PRAGMA foreign_keys = OFF")
            .await
            .unwrap();
        db.execute_raw(Statement::from_string(
            db.get_database_backend(),
            "UPDATE device SET principal_id = 'P00000000000000000099'".to_owned(),
        ))
        .await
        .unwrap();

        let report = scan_auth_persistence_invariants(&db).await.unwrap();
        assert!(!report.is_valid());
        assert!(
            report
                .violations
                .iter()
                .any(|value| value.contains("mismatched ownership"))
        );
    }

    #[tokio::test]
    async fn invariant_scan_uses_character_bounds_for_normalized_device_metadata() {
        let db = valid_auth_fixture().await;
        let device = device::Entity::find_by_id("D00000000000000000001")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let mut active: device::ActiveModel = device.into();
        active.display_name = Set(Some("Я".repeat(255)));
        active.platform = Set(Some("мобильная".to_owned()));
        active.update(&db).await.unwrap();

        assert!(
            scan_auth_persistence_invariants(&db)
                .await
                .unwrap()
                .is_valid()
        );
    }

    #[tokio::test]
    async fn invariant_scan_rejects_missing_auth_schema_guards_before_new_corruption() {
        let db = valid_auth_fixture().await;
        db.execute_unprepared("DROP INDEX idx_auth_refresh_token_hash")
            .await
            .unwrap();

        let report = scan_auth_persistence_invariants(&db).await.unwrap();
        assert!(
            report
                .violations
                .iter()
                .any(|value| value.contains("idx_auth_refresh_token_hash"))
        );
    }

    #[tokio::test]
    async fn invariant_scan_detects_every_epic3_corruption_class() {
        let cases = [
            (
                "PRAGMA ignore_check_constraints = ON; UPDATE device SET id = 'invalid-device-id'",
                "invalid domain data",
            ),
            (
                "PRAGMA ignore_check_constraints = ON; UPDATE auth_session SET status = 'revoked'",
                "impossible revoke state",
            ),
            (
                "DROP INDEX idx_device_active_installation;
                 INSERT INTO device(id,gateway_id,principal_id,installation_id,display_name,client_kind,status,created_at,updated_at,last_seen_at) VALUES('D00000000000000000002','G00000000000000000001','P00000000000000000001','desktop-a','Duplicate','desktop','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
                 INSERT INTO auth_session(id,gateway_id,principal_id,device_id,token_family_id,created_by_session_id,activation_token_hash,activation_locator_hash,activation_failed_attempts,activation_expires_at,activated_at,status,refresh_generation,created_at,updated_at,last_seen_at,last_refreshed_at,refresh_expires_at) VALUES('S00000000000000000002','G00000000000000000001','P00000000000000000001','D00000000000000000002','F00000000000000000002',NULL,randomblob(32),randomblob(32),0,datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,datetime('now','+90 days'));
                 INSERT INTO auth_refresh_credential(id,session_id,token_family_id,generation,token_hash,status,issued_at,expires_at) VALUES('R00000000000000000002','S00000000000000000002','F00000000000000000002',0,randomblob(32),'current',CURRENT_TIMESTAMP,datetime('now','+90 days'))",
                "multiple active devices",
            ),
            (
                "DROP INDEX idx_auth_refresh_current;
                 INSERT INTO auth_refresh_credential(id,session_id,token_family_id,generation,token_hash,status,issued_at,expires_at) VALUES('R00000000000000000002','S00000000000000000001','F00000000000000000001',1,randomblob(32),'current',CURRENT_TIMESTAMP,datetime('now','+90 days'))",
                "multiple current refreshes",
            ),
            (
                "UPDATE auth_session SET created_by_session_id = 'S00000000000000000099'",
                "mismatched creator session",
            ),
            (
                "PRAGMA ignore_check_constraints = ON; UPDATE auth_session SET activation_failed_attempts = 6",
                "invalid domain IDs",
            ),
        ];

        for (mutation, expected) in cases {
            let db = valid_auth_fixture().await;
            db.execute_unprepared("PRAGMA foreign_keys = OFF")
                .await
                .expect("disable fixture foreign keys");
            db.execute_unprepared(mutation)
                .await
                .expect("inject auth corruption");
            let report = scan_auth_persistence_invariants(&db)
                .await
                .expect("corruption scan");
            assert!(
                report
                    .violations
                    .iter()
                    .any(|violation| violation.contains(expected)),
                "expected `{expected}` in {:?}",
                report.violations
            );
        }
    }
}
