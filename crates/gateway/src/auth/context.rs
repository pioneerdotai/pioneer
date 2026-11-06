use pioneer_protocol::{
    AuthSessionId, DeviceId, GatewayId, InvitationTransportSecurity, PrincipalId, PrincipalKind,
    RoleKey,
};

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

impl AuthenticatedSessionPrincipal {
    /// Identity continuity only. Kind/role and lease changes still require a
    /// fresh authorization decision and exact principal check at delivery.
    pub(crate) fn same_subject(&self, other: &Self) -> bool {
        self.gateway_id == other.gateway_id
            && self.principal_id == other.principal_id
            && self.device_id == other.device_id
            && self.session_id == other.session_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestrictedAuthContext {
    Refresh(RefreshExchangeContext),
    DeviceActivation(DeviceActivationContext),
    Invitation(InvitationExchangeContext),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeviceActivationContext {
    pub(crate) gateway_id: GatewayId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RefreshExchangeContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InvitationExchangeContext {
    pub(crate) gateway_id: GatewayId,
    pub(crate) transport: InvitationTransportSecurity,
}
