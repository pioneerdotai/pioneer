use std::fmt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use zeroize::Zeroizing;

use crate::{
    AuthSecretString, AuthSessionGrant, ClientInstallationDescriptor, GatewayBaseUrl, GatewayId,
    GatewayTransportSecurity, InvitationId, MemberSummary, NewMemberProfile, PrincipalId,
    PrincipalKind, WorkspaceId,
};

pub const INVITATION_TTL_SECONDS: u64 = 7 * 24 * 60 * 60;
pub const INVITATION_MIN_WORKSPACE_GRANTS: usize = 1;
pub const INVITATION_MAX_WORKSPACE_GRANTS: usize = 64;
pub const INVITATION_CREDENTIAL_PREFIX: &str = "pinv1_";
pub const INVITATION_CREDENTIAL_BODY_LEN: usize = 43;
pub const INVITATION_CREDENTIAL_ENTROPY_BYTES: usize = 32;
pub const INVITATION_PAGE_DEFAULT_LIMIT: u32 = 50;
pub const INVITATION_PAGE_MAX_LIMIT: u32 = 100;
pub const INVITATION_CURSOR_MAX_BYTES: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvitationStatus {
    Pending,
    Accepted,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvitationRevokeReason {
    InviterRevoked,
    InviterUnavailable,
    GrantAuthorityLost,
    WorkspaceUnavailable,
}

#[derive(Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct InvitationCredential(AuthSecretString);

impl InvitationCredential {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvitationCredentialError> {
        let value = AuthSecretString::new(value);
        let Some(body) = value
            .expose_secret()
            .strip_prefix(INVITATION_CREDENTIAL_PREFIX)
        else {
            return Err(InvitationCredentialError);
        };
        if body.len() != INVITATION_CREDENTIAL_BODY_LEN
            || body.contains('=')
            || !body
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(InvitationCredentialError);
        }
        let decoded = Zeroizing::new(
            URL_SAFE_NO_PAD
                .decode(body)
                .map_err(|_| InvitationCredentialError)?,
        );
        if decoded.len() != INVITATION_CREDENTIAL_ENTROPY_BYTES {
            return Err(InvitationCredentialError);
        }
        Ok(Self(value))
    }

    pub fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for InvitationCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InvitationCredential([redacted])")
    }
}

impl<'de> Deserialize<'de> for InvitationCredential {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvitationCredentialError;

impl fmt::Display for InvitationCredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid invitation credential")
    }
}

impl std::error::Error for InvitationCredentialError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvitationTransportSecurity {
    SecureWss,
    InsecureWs,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "InvitationPresentationWire")]
pub struct InvitationPresentation {
    pub gateway_base_url: GatewayBaseUrl,
    pub gateway_id: GatewayId,
    token: InvitationCredential,
    deep_link: AuthSecretString,
}

impl InvitationPresentation {
    pub fn new(
        gateway_base_url: GatewayBaseUrl,
        gateway_id: GatewayId,
        token: InvitationCredential,
    ) -> Result<Self, InvitationUriError> {
        let mut uri = url::Url::parse("pioneer://invite").expect("static invite URI is valid");
        uri.query_pairs_mut()
            .append_pair("gateway_base_url", gateway_base_url.as_str())
            .append_pair("gateway_id", gateway_id.as_str());
        let mut deep_link = Zeroizing::new(uri.to_string());
        deep_link.push_str("#token=");
        deep_link.push_str(token.expose_secret());
        if deep_link.len() > crate::MAX_PROTECTED_GATEWAY_URI_BYTES {
            return Err(InvitationUriError);
        }
        let deep_link = std::mem::take(&mut *deep_link);
        Ok(Self {
            gateway_base_url,
            gateway_id,
            token,
            deep_link: AuthSecretString::new(deep_link),
        })
    }

    pub fn parse(uri: &str) -> Result<Self, InvitationUriError> {
        if uri.is_empty() || uri.len() > crate::MAX_PROTECTED_GATEWAY_URI_BYTES {
            return Err(InvitationUriError);
        }
        let (base, fragment) = uri.split_once('#').ok_or(InvitationUriError)?;
        let parsed = url::Url::parse(base).map_err(|_| InvitationUriError)?;
        if parsed.scheme() != "pioneer"
            || parsed.host_str() != Some("invite")
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || !parsed.path().is_empty()
        {
            return Err(InvitationUriError);
        }

        let mut gateway_base_url = None;
        let mut gateway_id = None;
        for (key, value) in parsed.query_pairs() {
            match key.as_ref() {
                "gateway_base_url" if gateway_base_url.is_none() => {
                    gateway_base_url = Some(
                        GatewayBaseUrl::parse_presentation(value.as_ref())
                            .map_err(|_| InvitationUriError)?,
                    );
                }
                "gateway_id" if gateway_id.is_none() => {
                    gateway_id =
                        Some(GatewayId::new(value.into_owned()).map_err(|_| InvitationUriError)?);
                }
                "token" | "gateway_base_url" | "gateway_id" => {
                    return Err(InvitationUriError);
                }
                _ => return Err(InvitationUriError),
            }
        }
        let token = fragment.strip_prefix("token=").ok_or(InvitationUriError)?;
        let token =
            InvitationCredential::parse(token.to_owned()).map_err(|_| InvitationUriError)?;
        Self::new(
            gateway_base_url.ok_or(InvitationUriError)?,
            gateway_id.ok_or(InvitationUriError)?,
            token,
        )
    }

    pub fn token(&self) -> &str {
        self.token.expose_secret()
    }

    pub fn deep_link(&self) -> &str {
        self.deep_link.expose_secret()
    }

    pub fn transport_security(&self) -> InvitationTransportSecurity {
        match self.gateway_base_url.transport_security() {
            GatewayTransportSecurity::Tls => InvitationTransportSecurity::SecureWss,
            GatewayTransportSecurity::LoopbackPlaintext
            | GatewayTransportSecurity::RemotePlaintext => InvitationTransportSecurity::InsecureWs,
        }
    }

    pub fn verify_gateway_id(&self, actual: &GatewayId) -> Result<(), InvitationErrorReason> {
        if actual == &self.gateway_id {
            Ok(())
        } else {
            Err(InvitationErrorReason::InvitationUnavailable)
        }
    }
}

impl fmt::Debug for InvitationPresentation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InvitationPresentation")
            .field("gateway_base_url", &self.gateway_base_url)
            .field("gateway_id", &self.gateway_id)
            .field("token", &"[redacted]")
            .field("deep_link", &"[redacted]")
            .finish()
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InvitationPresentationWire {
    gateway_base_url: GatewayBaseUrl,
    gateway_id: GatewayId,
    token: InvitationCredential,
    deep_link: AuthSecretString,
}

impl TryFrom<InvitationPresentationWire> for InvitationPresentation {
    type Error = InvitationUriError;

    fn try_from(value: InvitationPresentationWire) -> Result<Self, Self::Error> {
        let presentation = Self::new(value.gateway_base_url, value.gateway_id, value.token)?;
        if presentation.deep_link() != value.deep_link.expose_secret() {
            return Err(InvitationUriError);
        }
        Ok(presentation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvitationUriError;

impl fmt::Display for InvitationUriError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid invitation URI")
    }
}

impl std::error::Error for InvitationUriError {}

/// Immutable persisted grant. Epic 5 intentionally provides no update shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InvitationWorkspaceGrant {
    pub invitation_id: InvitationId,
    pub workspace_id: WorkspaceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "InvitationCreateParamsWire")]
pub struct InvitationCreateParams {
    #[schemars(length(min = 1, max = 64))]
    pub workspace_ids: Vec<WorkspaceId>,
}

impl InvitationCreateParams {
    pub fn new(mut workspace_ids: Vec<WorkspaceId>) -> Result<Self, InvitationParamsError> {
        workspace_ids.sort();
        workspace_ids.dedup();
        validate_workspace_count(workspace_ids.len())?;
        Ok(Self { workspace_ids })
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InvitationCreateParamsWire {
    #[schemars(length(min = 1, max = 64))]
    workspace_ids: Vec<WorkspaceId>,
}

impl TryFrom<InvitationCreateParamsWire> for InvitationCreateParams {
    type Error = InvitationParamsError;

    fn try_from(value: InvitationCreateParamsWire) -> Result<Self, Self::Error> {
        Self::new(value.workspace_ids)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvitationInviterSummary {
    pub principal_id: PrincipalId,
    pub kind: PrincipalKind,
    pub display_name: String,
    pub nickname: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvitationWorkspaceSummary {
    pub workspace_id: WorkspaceId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvitationSummary {
    pub invitation_id: InvitationId,
    pub status: InvitationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoke_reason: Option<InvitationRevokeReason>,
    pub inviter: InvitationInviterSummary,
    pub workspaces: Vec<InvitationWorkspaceSummary>,
    pub created_at_unix: u64,
    pub expires_at_unix: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_at_unix: Option<u64>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvitationCreateResponse {
    pub invitation: InvitationSummary,
    pub presentation: InvitationPresentation,
}

impl fmt::Debug for InvitationCreateResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InvitationCreateResponse")
            .field("invitation", &self.invitation)
            .field("presentation", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct InvitationListParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<InvitationStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator_principal_id: Option<PrincipalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl InvitationListParams {
    pub fn validate(&self) -> Result<u32, InvitationParamsError> {
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > INVITATION_CURSOR_MAX_BYTES)
        {
            return Err(InvitationParamsError::InvalidCursor);
        }
        let limit = self.limit.unwrap_or(INVITATION_PAGE_DEFAULT_LIMIT);
        if !(1..=INVITATION_PAGE_MAX_LIMIT).contains(&limit) {
            return Err(InvitationParamsError::InvalidLimit);
        }
        Ok(limit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvitationListResponse {
    pub invitations: Vec<InvitationSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InvitationRevokeParams {
    pub invitation_id: InvitationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvitationRevokeResponse {
    pub invitation: InvitationSummary,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvitationPreviewResponse {
    pub gateway_id: GatewayId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_display_name: Option<String>,
    pub inviter: InvitationInviterSummary,
    pub workspaces: Vec<InvitationWorkspaceSummary>,
    pub expires_at_unix: u64,
    pub transport: InvitationTransportSecurity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InvitationAcceptParams {
    pub profile: NewMemberProfile,
    pub installation: ClientInstallationDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvitationAcceptResponse {
    pub grant: AuthSessionGrant,
    pub member: MemberSummary,
    pub workspace_ids: Vec<WorkspaceId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvitationErrorReason {
    InvitationUnavailable,
    InvalidProfile,
    NicknameUnavailable,
    InvalidInstallation,
    AvatarInvalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvitationParamsError {
    WorkspaceCount {
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    InvalidCursor,
    InvalidLimit,
}

impl fmt::Display for InvitationParamsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkspaceCount {
                minimum,
                maximum,
                actual,
            } => write!(
                formatter,
                "invitation must grant {minimum} to {maximum} workspaces, got {actual}"
            ),
            Self::InvalidCursor => formatter.write_str("invalid invitation cursor"),
            Self::InvalidLimit => formatter.write_str("invalid invitation page limit"),
        }
    }
}

impl std::error::Error for InvitationParamsError {}

fn validate_workspace_count(actual: usize) -> Result<(), InvitationParamsError> {
    if !(INVITATION_MIN_WORKSPACE_GRANTS..=INVITATION_MAX_WORKSPACE_GRANTS).contains(&actual) {
        return Err(InvitationParamsError::WorkspaceCount {
            minimum: INVITATION_MIN_WORKSPACE_GRANTS,
            maximum: INVITATION_MAX_WORKSPACE_GRANTS,
            actual,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvitationChangedNotification {
    pub revision: u64,
    pub invitation_id: InvitationId,
}

#[cfg(test)]
mod tests {
    use super::{
        INVITATION_CREDENTIAL_BODY_LEN, INVITATION_CREDENTIAL_PREFIX, INVITATION_CURSOR_MAX_BYTES,
        INVITATION_MAX_WORKSPACE_GRANTS, INVITATION_MIN_WORKSPACE_GRANTS,
        INVITATION_PAGE_DEFAULT_LIMIT, INVITATION_PAGE_MAX_LIMIT, INVITATION_TTL_SECONDS,
        InvitationAcceptParams, InvitationCreateParams, InvitationCredential, InvitationListParams,
        InvitationPresentation, InvitationRevokeParams, InvitationRevokeReason, InvitationStatus,
        InvitationTransportSecurity, InvitationWorkspaceGrant,
    };
    use crate::{GatewayBaseUrl, GatewayId, InvitationId, WorkspaceId};
    use serde_json::json;

    #[test]
    fn invitation_state_vocabulary_is_exact_and_stable() {
        assert_eq!(INVITATION_TTL_SECONDS, 604_800);
        assert_eq!(INVITATION_MIN_WORKSPACE_GRANTS, 1);
        assert_eq!(INVITATION_MAX_WORKSPACE_GRANTS, 64);
        assert_eq!(
            serde_json::to_value(InvitationStatus::Pending).unwrap(),
            json!("pending")
        );
        assert_eq!(
            serde_json::to_value(InvitationRevokeReason::GrantAuthorityLost).unwrap(),
            json!("grant_authority_lost")
        );
        assert!(serde_json::from_value::<InvitationStatus>(json!("used")).is_err());
        assert!(serde_json::from_value::<InvitationRevokeReason>(json!("unknown")).is_err());
    }

    #[test]
    fn invitation_workspace_grant_uses_checked_ids_and_rejects_unknown_fields() {
        let value = json!({
            "invitation_id": "I00000000000000000001",
            "workspace_id": "W00000000000000000001"
        });
        let grant: InvitationWorkspaceGrant = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            grant,
            InvitationWorkspaceGrant {
                invitation_id: InvitationId::new("I00000000000000000001").unwrap(),
                workspace_id: WorkspaceId::new("W00000000000000000001").unwrap(),
            }
        );
        assert_eq!(serde_json::to_value(grant).unwrap(), value);
        assert!(
            serde_json::from_value::<InvitationWorkspaceGrant>(json!({
                "invitation_id": "I00000000000000000001",
                "workspace_id": "W00000000000000000001",
                "role": "member"
            }))
            .is_err()
        );
    }

    fn credential() -> String {
        format!("{INVITATION_CREDENTIAL_PREFIX}{}", "A".repeat(43))
    }

    #[test]
    fn invitation_credential_is_exact_and_debug_redacted() {
        let raw = credential();
        let parsed = InvitationCredential::parse(raw.clone()).unwrap();
        assert_eq!(INVITATION_CREDENTIAL_BODY_LEN, 43);
        assert_eq!(parsed.expose_secret(), raw);
        assert!(!format!("{parsed:?}").contains(&raw));
        for invalid in [
            format!("PINV1_{}", "A".repeat(43)),
            format!("pinv1_{}", "A".repeat(42)),
            format!("pinv1_{}=", "A".repeat(42)),
            format!("pinv1_{}+", "A".repeat(42)),
            format!(" pinv1_{}", "A".repeat(43)),
        ] {
            let error = InvitationCredential::parse(invalid).unwrap_err();
            assert!(!error.to_string().contains(&raw));
        }
    }

    #[test]
    fn invitation_uri_round_trips_fragment_only_secret_and_gateway_pin() {
        let raw = credential();
        let gateway_id = GatewayId::new("G00000000000000000001").unwrap();
        let presentation = InvitationPresentation::new(
            GatewayBaseUrl::parse_presentation("91.224.86.172:17878").unwrap(),
            gateway_id.clone(),
            InvitationCredential::parse(raw.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            presentation.gateway_base_url.as_str(),
            "http://91.224.86.172:17878/"
        );
        assert_eq!(
            presentation.transport_security(),
            InvitationTransportSecurity::InsecureWs
        );
        let parsed_url = url::Url::parse(presentation.deep_link()).unwrap();
        assert!(!parsed_url.query().unwrap_or_default().contains(&raw));
        assert!(parsed_url.fragment().unwrap_or_default().contains("token="));
        assert_eq!(
            InvitationPresentation::parse(presentation.deep_link()).unwrap(),
            presentation
        );
        assert!(presentation.verify_gateway_id(&gateway_id).is_ok());
        assert!(
            presentation
                .verify_gateway_id(&GatewayId::new("G00000000000000000002").unwrap())
                .is_err()
        );
        let rendered = format!("{presentation:?}");
        assert!(!rendered.contains(&raw));
        assert!(!rendered.contains("pioneer://invite"));
    }

    #[test]
    fn invitation_uri_accepts_canonical_bases_and_rejects_legacy_or_leaky_fields() {
        let raw = credential();
        let gateway_id = GatewayId::new("G00000000000000000001").unwrap();
        let secure = InvitationPresentation::new(
            GatewayBaseUrl::parse_presentation("https://gateway.example.test/pioneer").unwrap(),
            gateway_id.clone(),
            InvitationCredential::parse(raw.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            secure.transport_security(),
            InvitationTransportSecurity::SecureWss
        );
        for endpoint in [
            "http://localhost:17878",
            "http://192.168.1.10:17878",
            "http://gateway.example.test:17878",
            "https://gateway.example.test/pioneer",
        ] {
            assert!(
                InvitationPresentation::new(
                    GatewayBaseUrl::parse_presentation(endpoint).unwrap(),
                    gateway_id.clone(),
                    InvitationCredential::parse(raw.clone()).unwrap(),
                )
                .is_ok(),
                "{endpoint}"
            );
        }
        for uri in [
            format!(
                "pioneer://invite?gateway_base_url=http%3A%2F%2Flocalhost%3A17878%2F&gateway_id={gateway_id}&token={raw}#token={raw}"
            ),
            format!(
                "pioneer://invite?gateway_base_url=http%3A%2F%2Flocalhost%3A17878%2F&gateway_base_url=http%3A%2F%2Fother%3A17878%2F&gateway_id={gateway_id}#token={raw}"
            ),
            format!(
                "pioneer://invite?gateway_base_url=http%3A%2F%2Flocalhost%3A17878%2F&gateway_id={gateway_id}#token={raw}&token={raw}"
            ),
            format!(
                "pioneer://invite?gateway_base_url=http%3A%2F%2Flocalhost%3A17878%2F&gateway_id={gateway_id}&extra=1#token={raw}"
            ),
            format!(
                "pioneer://invite/other?gateway_base_url=http%3A%2F%2Flocalhost%3A17878%2F&gateway_id={gateway_id}#token={raw}"
            ),
            format!(
                "pioneer://invite/?gateway_base_url=http%3A%2F%2Flocalhost%3A17878%2F&gateway_id={gateway_id}#token={raw}"
            ),
            format!(
                "pioneer://invite?gateway=ws%3A%2F%2Flocalhost%3A17878&gateway_id={gateway_id}#token={raw}"
            ),
        ] {
            assert!(InvitationPresentation::parse(&uri).is_err(), "{uri}");
        }
    }

    #[test]
    fn invitation_create_params_are_canonical_bounded_and_have_no_authority_fields() {
        let params = InvitationCreateParams::new(vec![
            WorkspaceId::new("W00000000000000000002").unwrap(),
            WorkspaceId::new("W00000000000000000001").unwrap(),
            WorkspaceId::new("W00000000000000000001").unwrap(),
        ])
        .unwrap();
        assert_eq!(
            params
                .workspace_ids
                .iter()
                .map(WorkspaceId::as_str)
                .collect::<Vec<_>>(),
            vec!["W00000000000000000001", "W00000000000000000002"]
        );
        assert!(InvitationCreateParams::new(Vec::new()).is_err());
        assert!(
            InvitationCreateParams::new(
                (0..=INVITATION_MAX_WORKSPACE_GRANTS)
                    .map(|index| WorkspaceId::new(format!("W{index:020}")).unwrap())
                    .collect(),
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<InvitationCreateParams>(json!({
                "workspace_ids": ["W00000000000000000001"],
                "role": "member"
            }))
            .is_err()
        );
    }

    #[test]
    fn invitation_request_dtos_enforce_page_bounds_and_reject_unknown_fields() {
        assert_eq!(
            InvitationListParams::default().validate().unwrap(),
            INVITATION_PAGE_DEFAULT_LIMIT
        );
        for invalid in [
            InvitationListParams {
                limit: Some(0),
                ..Default::default()
            },
            InvitationListParams {
                limit: Some(INVITATION_PAGE_MAX_LIMIT + 1),
                ..Default::default()
            },
            InvitationListParams {
                cursor: Some(String::new()),
                ..Default::default()
            },
            InvitationListParams {
                cursor: Some("x".repeat(INVITATION_CURSOR_MAX_BYTES + 1)),
                ..Default::default()
            },
        ] {
            assert!(invalid.validate().is_err());
        }

        assert!(serde_json::from_value::<InvitationListParams>(json!({ "extra": true })).is_err());
        assert!(
            serde_json::from_value::<InvitationRevokeParams>(json!({
                "invitation_id": "I00000000000000000001",
                "reason": "caller_controlled"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<InvitationAcceptParams>(json!({
                "profile": { "display_name": "Member", "nickname": "member" },
                "installation": {
                    "installation_id": "installation-1",
                    "display_name": "Pioneer",
                    "client_kind": "desktop"
                },
                "workspace_ids": ["W00000000000000000001"]
            }))
            .is_err()
        );
        for forbidden in [
            "principal_id",
            "device_id",
            "session_id",
            "invitation_id",
            "token",
        ] {
            let mut value = json!({
                "profile": { "display_name": "Member", "nickname": "member" },
                "installation": {
                    "installation_id": "installation-1",
                    "display_name": "Pioneer",
                    "client_kind": "desktop"
                }
            });
            value[forbidden] = json!("caller-owned-authority");
            assert!(serde_json::from_value::<InvitationAcceptParams>(value).is_err());
        }
    }

    #[test]
    fn invitation_json_schema_is_deterministic_and_secret_shapes_stay_strings() {
        let first = serde_json::to_value(schemars::schema_for!(InvitationPresentation)).unwrap();
        let second = serde_json::to_value(schemars::schema_for!(InvitationPresentation)).unwrap();
        assert_eq!(first, second);
        assert_eq!(first["properties"]["token"]["type"], "string");
    }
}
