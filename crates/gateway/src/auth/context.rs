use pioneer_protocol::{AuthSessionId, DeviceId, GatewayId, PrincipalId, PrincipalKind, RoleKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthenticatedSessionPrincipal {
    pub(crate) gateway_id: GatewayId,
    pub(crate) principal_id: PrincipalId,
    pub(crate) kind: PrincipalKind,
    pub(crate) role_key: Option<RoleKey>,
    pub(crate) device_id: DeviceId,
    pub(crate) session_id: AuthSessionId,
    pub(crate) access_jti: String,
    pub(crate) access_expires_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestrictedAuthContext {
    Refresh(RefreshExchangeContext),
    DeviceActivation(DeviceActivationContext),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeviceActivationContext {
    pub(crate) gateway_id: GatewayId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RefreshExchangeContext;
