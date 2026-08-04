mod admission;
mod context;
mod credential;
mod error;
mod installation;
mod jwt_v2;
mod opaque;
mod readiness;
mod service;

pub(crate) const AUTH_SCHEMA_VERSION: i64 = 3;

pub(crate) use admission::{AuthAdmissionService, CapturedAdmission, RestrictedAdmission};
pub(crate) use context::{
    AuthenticatedSessionPrincipal, DeviceActivationContext, InvitationExchangeContext,
    RefreshExchangeContext, RestrictedAuthContext,
};
pub(crate) use credential::{PresentedCredential, PresentedCredentialKind};
pub(crate) use error::{AuthError, AuthErrorCode};
pub(crate) use installation::validate_installation_descriptor;
pub(crate) use jwt_v2::{AccessCredential, AccessJwtIssuer, AccessJwtSubject};
pub(crate) use opaque::OpaqueCredentialFactory;
pub(crate) use readiness::ensure_auth_readiness;
pub(crate) use service::{
    AuthSessionDisconnectHook, FirstMemberSessionIds, GatewayAuthService,
    InvitationAcceptCommitted, InvitationAcceptPostCommitHook,
};

#[cfg(test)]
pub(crate) mod test_support;
