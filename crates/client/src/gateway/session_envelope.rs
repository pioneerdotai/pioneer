//! Durable Gateway credential envelope and shared session identity validation.
use pioneer_protocol::{
    AuthSecretString, AuthSessionId, DeviceId, GatewayId, PrincipalId, REFRESH_CREDENTIAL_BODY_LEN,
    REFRESH_CREDENTIAL_PREFIX, TokenFamilyId,
};
use serde::{Deserialize, Serialize};

pub const GATEWAY_SESSION_SCHEMA_VERSION: u32 = 2;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GatewaySessionEnvelope {
    pub schema_version: u32,
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub token_family_id: TokenFamilyId,
    pub installation_id: String,
    pub refresh_generation: u64,
    pub refresh_expires_at_unix: u64,
    pub refresh_token: AuthSecretString,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_refresh_request_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewaySessionEnvelopeError {
    Version,
    Credential,
    Installation,
    Expiry,
    RequestId,
}

impl GatewaySessionEnvelope {
    pub fn validate(&self) -> Result<(), GatewaySessionEnvelopeError> {
        use GatewaySessionEnvelopeError::*;
        if self.schema_version != GATEWAY_SESSION_SCHEMA_VERSION {
            return Err(Version);
        }
        if !is_valid_refresh_credential(self.refresh_token.expose_secret()) {
            return Err(Credential);
        }
        let installation = self.installation_id.trim();
        if installation != self.installation_id
            || installation.is_empty()
            || installation.chars().count() > 255
            || installation.chars().any(char::is_control)
        {
            return Err(Installation);
        }
        if self.refresh_expires_at_unix == 0 {
            return Err(Expiry);
        }
        if self.pending_refresh_request_id.as_ref().is_some_and(|id| {
            pioneer_protocol::RequestId::new(id.clone()).is_err()
                || !id.bytes().all(|byte| byte.is_ascii_alphanumeric())
        }) {
            return Err(RequestId);
        }
        Ok(())
    }

    pub fn accepts_refresh(
        &self,
        installation_id: &str,
        client_kind: pioneer_protocol::ClientKind,
        grant: &pioneer_protocol::AuthRefreshGrant,
    ) -> bool {
        grant.auth_protocol_version == pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION
            && grant.credential_storage_order
                == pioneer_protocol::CredentialStorageOrder::PersistRefreshBeforeActivatingAccess
            && grant.gateway.id == self.gateway_id
            && grant.principal.id == self.principal_id
            && matches!(
                grant.principal.kind,
                pioneer_protocol::PrincipalKind::Superuser | pioneer_protocol::PrincipalKind::User
            )
            && grant.session.id == self.session_id
            && grant.session.device_id == self.device_id
            && grant.session.token_family_id == self.token_family_id
            && grant.device.id == self.device_id
            && grant.device.installation_id == installation_id
            && grant.device.installation_id == self.installation_id
            && grant.device.client_kind == client_kind
            && grant.device.status == pioneer_protocol::DeviceStatus::Active
            && grant.session.status == pioneer_protocol::AuthSessionStatus::Active
            && self.refresh_generation.checked_add(1) == Some(grant.refresh_generation)
            && grant.session.refresh_generation == grant.refresh_generation
            && grant.session.refresh_expires_at_unix == grant.refresh_expires_at_unix
            && !grant.access_token.expose_secret().is_empty()
            && grant.access_expires_at_unix != 0
            && is_valid_refresh_credential(grant.refresh_token.expose_secret())
    }

    pub fn identity_failure(
        &self,
        pinned_gateway_id: Option<&GatewayId>,
        installation_id: &str,
        client_kind: pioneer_protocol::ClientKind,
        identity: &pioneer_protocol::AuthMeResponse,
    ) -> Option<super::session_lifecycle::SessionTerminalReason> {
        use super::session_lifecycle::SessionTerminalReason;
        if pinned_gateway_id != Some(&self.gateway_id) || identity.gateway.id != self.gateway_id {
            return Some(SessionTerminalReason::GatewayIdentityMismatch);
        }
        if identity.principal.id != self.principal_id
            || !matches!(
                identity.principal.kind,
                pioneer_protocol::PrincipalKind::Superuser | pioneer_protocol::PrincipalKind::User
            )
            || identity.device.id != self.device_id
            || identity.device.installation_id != installation_id
            || identity.device.installation_id != self.installation_id
            || identity.device.client_kind != client_kind
            || identity.device.status != pioneer_protocol::DeviceStatus::Active
            || identity.session.id != self.session_id
            || identity.session.device_id != self.device_id
            || identity.session.token_family_id != self.token_family_id
            || identity.session.status != pioneer_protocol::AuthSessionStatus::Active
            || identity.session.refresh_generation != self.refresh_generation
            || identity.session.refresh_expires_at_unix != self.refresh_expires_at_unix
        {
            return Some(SessionTerminalReason::SessionCompromised);
        }
        None
    }
}

pub fn is_valid_refresh_credential(value: &str) -> bool {
    let Some(body) = value.strip_prefix(REFRESH_CREDENTIAL_PREFIX) else {
        return false;
    };
    body.len() == REFRESH_CREDENTIAL_BODY_LEN
        && body
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}
