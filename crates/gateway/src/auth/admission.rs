use std::sync::Arc;

use axum::http::HeaderMap;
use pioneer_config::AppConfig;
use pioneer_protocol::InvitationTransportSecurity;

use crate::helpers::unix_timestamp_secs;
use crate::identity::IdentityBootstrapSnapshot;
use crate::secrets::AuthKeyMaterial;

use super::{
    AccessCredential, AccessJwtIssuer, AuthError, AuthErrorCode, DeviceActivationContext,
    InvitationExchangeContext, PresentedCredential, PresentedCredentialKind,
    RefreshExchangeContext, RestrictedAuthContext,
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

    /// Performs only bounded header parsing, format classification and access-JWT
    /// verification. Refresh-envelope verification and persisted session checks
    /// remain in the restricted async exchange path.
    pub(crate) fn capture_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<CapturedAdmission, AuthError> {
        let raw = extract_single_bearer(headers)?;
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
            PresentedCredentialKind::Invitation => {
                Ok(CapturedAdmission::Restricted(RestrictedAdmission::new(
                    presented,
                    RestrictedAuthContext::Invitation(InvitationExchangeContext {
                        gateway_id: self.gateway_id.clone(),
                        // Axum receives the Gateway's direct listener transport.
                        // Trusted proxy/TLS classification is qualified in Phase 7.
                        transport: InvitationTransportSecurity::InsecureWs,
                    }),
                )))
            }
        }
    }

    pub(crate) fn capture_access_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<AccessCredential, AuthError> {
        match self.capture_headers(headers)? {
            CapturedAdmission::Access(credential) => Ok(credential),
            CapturedAdmission::Restricted(_) => {
                Err(AuthError::new(AuthErrorCode::UnsupportedCredential))
            }
        }
    }
}

fn extract_single_bearer(headers: &HeaderMap) -> Result<&str, AuthError> {
    let mut authorization = headers.get_all("authorization").iter();
    let header = authorization
        .next()
        .ok_or_else(|| AuthError::new(AuthErrorCode::MissingCredential))?;
    if authorization.next().is_some() {
        return Err(AuthError::new(AuthErrorCode::DuplicateCredential));
    }
    let header = header
        .to_str()
        .map_err(|_| AuthError::new(AuthErrorCode::MalformedCredential))?;
    extract_bearer_token(header).ok_or_else(|| AuthError::new(AuthErrorCode::MalformedCredential))
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

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};

    use super::*;

    #[test]
    fn bearer_extraction_requires_exactly_one_authorization_header() {
        let empty = HeaderMap::new();
        assert_eq!(
            extract_single_bearer(&empty).unwrap_err().code(),
            AuthErrorCode::MissingCredential
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer access.header.signature"),
        );
        assert_eq!(
            extract_single_bearer(&headers).unwrap(),
            "access.header.signature"
        );

        headers.append(
            "authorization",
            HeaderValue::from_static("Bearer second.header.signature"),
        );
        assert_eq!(
            extract_single_bearer(&headers).unwrap_err().code(),
            AuthErrorCode::DuplicateCredential
        );
    }

    #[test]
    fn cookies_and_non_bearer_schemes_never_supply_access_credentials() {
        let mut cookie_only = HeaderMap::new();
        cookie_only.insert("cookie", HeaderValue::from_static("access_token=private"));
        assert_eq!(
            extract_single_bearer(&cookie_only).unwrap_err().code(),
            AuthErrorCode::MissingCredential
        );

        for value in ["Basic private", "Bearer", "private"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "authorization",
                HeaderValue::from_str(value).expect("test header"),
            );
            assert_eq!(
                extract_single_bearer(&headers).unwrap_err().code(),
                AuthErrorCode::MalformedCredential
            );
        }
    }
}
