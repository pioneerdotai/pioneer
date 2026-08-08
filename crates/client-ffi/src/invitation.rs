use pioneer_client::gateway::invitation::{
    InvitationAccessGrant, InvitationQrPresentation, InvitationRefreshEnvelope,
    InvitationRegistryBinding, InvitationSessionCommitState,
};
use pioneer_client::transport::ws::auth_exchange::{
    InvitationExchangeError, InvitationExchangeErrorKind,
};
use pioneer_protocol::{
    AuthSecretString, ClientKind, GatewayBaseUrl, GatewayId, InvitationAcceptParams,
    InvitationPreviewResponse, InvitationTransportSecurity, MemberSummary, PrincipalId,
    WorkspaceId,
};
use serde::{Deserialize, Serialize};

pub const INVALID_INVITATION_REQUEST_CODE: &str = "invalid_invitation_request";
pub const INVITATION_COMMIT_UNAVAILABLE_CODE: &str = "invitation_commit_unavailable";

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientInvitationPresentationRequest {
    pub uri: AuthSecretString,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct ClientInvitationPresentationResult {
    pub gateway_base_url: GatewayBaseUrl,
    pub gateway_id: GatewayId,
    pub transport_security: InvitationTransportSecurity,
    pub canonical_uri: AuthSecretString,
    pub qr_payload: AuthSecretString,
    pub qr_width: usize,
    pub qr_modules: Vec<bool>,
}

impl ClientInvitationPresentationResult {
    pub fn from_request(request: ClientInvitationPresentationRequest) -> Result<Self, String> {
        let presentation = InvitationQrPresentation::parse(request.uri.expose_secret())
            .map_err(|_| "invalid invitation URI".to_owned())?;
        let canonical_uri = AuthSecretString::new(presentation.deep_link());
        let qr_payload = std::str::from_utf8(presentation.qr_payload())
            .map(|payload| AuthSecretString::new(payload.to_owned()))
            .map_err(|_| "invalid invitation QR payload".to_owned())?;
        let (qr_width, qr_modules) = presentation
            .qr_modules()
            .map_err(|_| "invalid invitation QR payload".to_owned())?;
        Ok(Self {
            gateway_base_url: presentation.gateway_base_url().clone(),
            gateway_id: presentation.gateway_id().clone(),
            transport_security: presentation.transport_security(),
            canonical_uri,
            qr_payload,
            qr_width,
            qr_modules,
        })
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientInvitationPreviewRequest {
    pub uri: AuthSecretString,
    #[serde(default = "default_exchange_timeout_ms")]
    pub timeout_ms: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientInvitationAcceptRequest {
    pub uri: AuthSecretString,
    pub params: InvitationAcceptParams,
    pub expected_installation_id: String,
    #[serde(default = "default_exchange_timeout_ms")]
    pub timeout_ms: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClientInvitationCommitState {
    RefreshReady,
    AwaitingSecureStorage,
    AwaitingRegistry,
    DurableSessionUnbound,
    ReadyToConnect,
}

impl From<InvitationSessionCommitState> for ClientInvitationCommitState {
    fn from(value: InvitationSessionCommitState) -> Self {
        match value {
            InvitationSessionCommitState::RefreshReady => Self::RefreshReady,
            InvitationSessionCommitState::AwaitingSecureStorage => Self::AwaitingSecureStorage,
            InvitationSessionCommitState::AwaitingRegistry => Self::AwaitingRegistry,
            InvitationSessionCommitState::DurableSessionUnbound => Self::DurableSessionUnbound,
            InvitationSessionCommitState::ReadyToConnect => Self::ReadyToConnect,
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientInvitationAcceptResult {
    pub commit_id: String,
    pub state: ClientInvitationCommitState,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientInvitationCommitRequest {
    pub commit_id: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientInvitationCommitCleanupRequest {
    pub commit_id: String,
    #[serde(default = "default_exchange_timeout_ms")]
    pub timeout_ms: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct ClientInvitationRefreshWrite {
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub device_id: pioneer_protocol::DeviceId,
    pub session_id: pioneer_protocol::AuthSessionId,
    pub token_family_id: pioneer_protocol::TokenFamilyId,
    pub installation_id: String,
    pub client_kind: ClientKind,
    pub refresh_generation: u64,
    pub refresh_expires_at_unix: u64,
    pub refresh_token: AuthSecretString,
}

impl From<InvitationRefreshEnvelope> for ClientInvitationRefreshWrite {
    fn from(value: InvitationRefreshEnvelope) -> Self {
        let refresh_token = AuthSecretString::new(value.refresh_token());
        Self {
            gateway_id: value.gateway_id,
            principal_id: value.principal_id,
            device_id: value.device_id,
            session_id: value.session_id,
            token_family_id: value.token_family_id,
            installation_id: value.installation_id,
            client_kind: value.client_kind,
            refresh_generation: value.refresh_generation,
            refresh_expires_at_unix: value.refresh_expires_at_unix,
            refresh_token,
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientInvitationRegistryWrite {
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub device_id: pioneer_protocol::DeviceId,
    pub session_id: pioneer_protocol::AuthSessionId,
    pub member: MemberSummary,
    pub workspace_ids: Vec<WorkspaceId>,
}

impl From<InvitationRegistryBinding> for ClientInvitationRegistryWrite {
    fn from(value: InvitationRegistryBinding) -> Self {
        Self {
            gateway_id: value.gateway_id,
            principal_id: value.principal_id,
            device_id: value.device_id,
            session_id: value.session_id,
            member: value.member,
            workspace_ids: value.workspace_ids,
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct ClientInvitationAccessResult {
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub device_id: pioneer_protocol::DeviceId,
    pub session_id: pioneer_protocol::AuthSessionId,
    pub access_expires_at_unix: u64,
    pub access_token: AuthSecretString,
}

impl From<InvitationAccessGrant> for ClientInvitationAccessResult {
    fn from(value: InvitationAccessGrant) -> Self {
        let access_token = AuthSecretString::new(value.access_token());
        Self {
            gateway_id: value.gateway_id,
            principal_id: value.principal_id,
            device_id: value.device_id,
            session_id: value.session_id,
            access_expires_at_unix: value.access_expires_at_unix,
            access_token,
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientInvitationCommitFailureResult {
    pub released: bool,
    pub cleanup_attempted: bool,
}

macro_rules! redacted_debug {
    ($type:ty, $name:literal) => {
        impl std::fmt::Debug for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct($name)
                    .field("secret", &"[redacted]")
                    .finish()
            }
        }
    };
}

redacted_debug!(
    ClientInvitationPresentationRequest,
    "ClientInvitationPresentationRequest"
);
redacted_debug!(
    ClientInvitationPresentationResult,
    "ClientInvitationPresentationResult"
);
redacted_debug!(
    ClientInvitationPreviewRequest,
    "ClientInvitationPreviewRequest"
);
redacted_debug!(
    ClientInvitationAcceptRequest,
    "ClientInvitationAcceptRequest"
);
redacted_debug!(ClientInvitationRefreshWrite, "ClientInvitationRefreshWrite");
redacted_debug!(ClientInvitationAccessResult, "ClientInvitationAccessResult");

pub fn parse_preview(
    request: &ClientInvitationPreviewRequest,
) -> Result<InvitationQrPresentation, String> {
    InvitationQrPresentation::parse(request.uri.expose_secret())
        .map_err(|_| "invalid invitation URI".to_owned())
}

pub fn parse_accept(
    request: &ClientInvitationAcceptRequest,
) -> Result<InvitationQrPresentation, String> {
    if request.expected_installation_id != request.params.installation.installation_id {
        return Err("invitation installation identity mismatch".to_owned());
    }
    InvitationQrPresentation::parse(request.uri.expose_secret())
        .map_err(|_| "invalid invitation URI".to_owned())
}

pub fn exchange_error(error: InvitationExchangeError) -> crate::ClientFfiError {
    let code = match error.kind {
        InvitationExchangeErrorKind::Unavailable => "invitation_unavailable",
        InvitationExchangeErrorKind::InvalidProfile => "invalid_profile",
        InvitationExchangeErrorKind::NicknameUnavailable => "nickname_unavailable",
        InvitationExchangeErrorKind::InvalidInstallation => "invalid_installation",
        InvitationExchangeErrorKind::AvatarInvalid => "avatar_invalid",
        InvitationExchangeErrorKind::InvalidEndpoint => "invalid_invitation_endpoint",
        InvitationExchangeErrorKind::Timeout => "invitation_exchange_timeout",
        InvitationExchangeErrorKind::Transport => "invitation_transport_failed",
        InvitationExchangeErrorKind::Protocol => "invitation_protocol_failed",
    };
    crate::ClientFfiError::new(error.to_string(), code)
}

pub type ClientInvitationPreviewResult = InvitationPreviewResponse;

const fn default_exchange_timeout_ms() -> u64 {
    15_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presentation_and_secret_results_have_redacted_debug() {
        let secret = format!("pinv1_{}", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let uri = format!(
            "pioneer://invite?gateway_base_url=https%3A%2F%2Fgateway.example%2Fpioneer%2F&gateway_id=G00000000000000000001#token={secret}"
        );
        let request = ClientInvitationPresentationRequest {
            uri: AuthSecretString::new(uri.clone()),
        };
        assert!(!format!("{request:?}").contains(&secret));

        let result = ClientInvitationPresentationResult::from_request(request)
            .expect("valid shared invitation presentation");
        assert_eq!(result.canonical_uri.expose_secret(), uri);
        assert!(!format!("{result:?}").contains(&secret));
    }

    #[test]
    fn accept_rejects_local_installation_mismatch_before_exchange() {
        let secret = format!("pinv1_{}", "A".repeat(43));
        let uri = format!(
            "pioneer://invite?gateway_base_url=https%3A%2F%2Fgateway.example%2Fpioneer%2F&gateway_id=G00000000000000000001#token={secret}"
        );
        let request: ClientInvitationAcceptRequest = serde_json::from_value(serde_json::json!({
            "uri": uri,
            "expected_installation_id": "installation-local",
            "params": {
                "profile": {
                    "display_name": "Member",
                    "nickname": "member"
                },
                "installation": {
                    "installation_id": "installation-request",
                    "display_name": "Pioneer App",
                    "client_kind": "mobile"
                }
            }
        }))
        .expect("bounded invitation request");

        assert_eq!(
            parse_accept(&request),
            Err("invitation installation identity mismatch".to_owned())
        );
    }
}
