mod admission;
mod context;
mod credential;
mod error;
mod jwt_v2;
mod opaque;
mod readiness;
mod service;

pub(crate) const AUTH_SCHEMA_VERSION: i64 = 2;

pub(crate) use admission::{AuthAdmissionService, CapturedAdmission, RestrictedAdmission};
pub(crate) use context::{
    AuthenticatedSessionPrincipal, DeviceActivationContext, RefreshExchangeContext,
    RestrictedAuthContext,
};
pub(crate) use credential::{PresentedCredential, PresentedCredentialKind};
pub(crate) use error::{AuthError, AuthErrorCode};
pub(crate) use jwt_v2::{AccessCredential, AccessJwtIssuer, AccessJwtSubject};
pub(crate) use opaque::OpaqueCredentialFactory;
pub(crate) use readiness::ensure_auth_readiness;
pub(crate) use service::{AuthSessionDisconnectHook, GatewayAuthService};

#[cfg(test)]
pub(crate) mod test_support;
