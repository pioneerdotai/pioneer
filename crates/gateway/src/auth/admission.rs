use std::sync::Arc;

use pioneer_config::AppConfig;
use tokio_tungstenite::tungstenite::handshake::server::Request;

use crate::helpers::unix_timestamp_secs;
use crate::identity::IdentityBootstrapSnapshot;
use crate::secrets::AuthKeyMaterial;

use super::{
    AccessCredential, AccessJwtIssuer, AuthError, AuthErrorCode, DeviceActivationContext,
    PresentedCredential, PresentedCredentialKind, RefreshExchangeContext, RestrictedAuthContext,
};

#[derive(Clone)]
pub(crate) struct AuthAdmissionService {
    access: Arc<AccessJwtIssuer>,
    gateway_id: pioneer_protocol::GatewayId,
}

#[derive(Debug)]
pub(crate) enum CapturedAdmission {
    Access(AccessCredential),
    Restricted(RestrictedAdmission),
}

#[derive(Debug)]
pub(crate) struct RestrictedAdmission {
    credential: PresentedCredential,
    context: RestrictedAuthContext,
}

impl RestrictedAdmission {
    pub(crate) fn new(credential: PresentedCredential, context: RestrictedAuthContext) -> Self {
        Self {
            credential,
            context,
        }
    }

    pub(crate) fn context(&self) -> &RestrictedAuthContext {
        &self.context
    }

    pub(crate) fn credential(&self) -> &PresentedCredential {
        &self.credential
    }
}

impl AuthAdmissionService {
    pub(crate) fn new(
        config: &AppConfig,
        access_key: &AuthKeyMaterial,
        identity: &IdentityBootstrapSnapshot,
    ) -> Result<Self, AuthError> {
        let gateway_id = identity.gateway.id.clone();
        Ok(Self {
            access: Arc::new(AccessJwtIssuer::new(
                access_key.as_bytes(),
                &config.gateway.auth,
                gateway_id.clone(),
            )?),
            gateway_id,
        })
    }

    /// Performs only bounded header parsing, format classification and local
    /// cryptographic verification. Persisted session state is deliberately not
    /// available to this synchronous handshake path.
    pub(crate) fn capture_request(
        &self,
        request: &Request,
    ) -> Result<CapturedAdmission, AuthError> {
        let mut headers = request.headers().get_all("authorization").iter();
        let header = headers
            .next()
            .ok_or_else(|| AuthError::new(AuthErrorCode::MissingCredential))?;
        if headers.next().is_some() {
            return Err(AuthError::new(AuthErrorCode::DuplicateCredential));
        }
        let header = header
            .to_str()
            .map_err(|_| AuthError::new(AuthErrorCode::MalformedCredential))?;
        let raw = extract_bearer_token(header)
            .ok_or_else(|| AuthError::new(AuthErrorCode::MalformedCredential))?;
        let presented = PresentedCredential::classify(raw)?;
        let now =
            unix_timestamp_secs().map_err(|_| AuthError::new(AuthErrorCode::InvalidCredential))?;

        match presented.kind() {
            PresentedCredentialKind::AccessV2 => self
                .access
                .validate(presented.expose_for_authentication(), now)
                .map(CapturedAdmission::Access),
            PresentedCredentialKind::Refresh => {
                Ok(CapturedAdmission::Restricted(RestrictedAdmission::new(
                    presented,
                    RestrictedAuthContext::Refresh(RefreshExchangeContext),
                )))
            }
            PresentedCredentialKind::DeviceActivation => {
                Ok(CapturedAdmission::Restricted(RestrictedAdmission::new(
                    presented,
                    RestrictedAuthContext::DeviceActivation(DeviceActivationContext {
                        gateway_id: self.gateway_id.clone(),
                    }),
                )))
            }
        }
    }
}

fn extract_bearer_token(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let scheme = parts.next()?;
    let token = parts.next()?.trim();
    if !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() {
        return None;
    }
    Some(token)
}
