//! Secret-bearing invitation presentation shared by every Pioneer shell.
//!
//! Parsing and URI construction remain owned by `pioneer-protocol`. This
//! module only exposes shell-neutral workflow helpers and the exact bytes a
//! shell may pass to its QR renderer. It never renders an image and must not be
//! serialized into client state, logs or diagnostics.

use pioneer_protocol::{
    AuthSecretString, AuthSessionId, ClientKind, CredentialStorageOrder, DeviceId, DeviceStatus,
    GatewayBaseUrl, GatewayId, InvitationAcceptResponse, InvitationPresentation,
    InvitationTransportSecurity, MemberSummary, PrincipalId, PrincipalKind, PrincipalStatus,
    TokenFamilyId, WorkspaceId,
};

use crate::{ClientError, ClientResult};

#[derive(Clone, PartialEq, Eq)]
pub struct InvitationQrPresentation {
    presentation: InvitationPresentation,
}

impl InvitationQrPresentation {
    pub fn from_presentation(presentation: InvitationPresentation) -> Self {
        Self { presentation }
    }

    pub fn parse(uri: &str) -> ClientResult<Self> {
        InvitationPresentation::parse(uri)
            .map(Self::from_presentation)
            .map_err(|_| ClientError::protocol("invalid invitation URI"))
    }

    pub fn verify_gateway_id(&self, actual: &GatewayId) -> ClientResult<()> {
        self.presentation
            .verify_gateway_id(actual)
            .map_err(|_| ClientError::protocol("invitation Gateway identity mismatch"))
    }

    pub fn gateway_id(&self) -> &GatewayId {
        &self.presentation.gateway_id
    }

    pub fn gateway_base_url(&self) -> &GatewayBaseUrl {
        &self.presentation.gateway_base_url
    }

    pub fn transport_security(&self) -> InvitationTransportSecurity {
        self.presentation.transport_security()
    }

    pub fn credential(&self) -> &str {
        self.presentation.token()
    }

    pub fn deep_link(&self) -> &str {
        self.presentation.deep_link()
    }

    /// Exact canonical secret URI bytes for a shell-owned QR renderer.
    pub fn qr_payload(&self) -> &[u8] {
        self.presentation.deep_link().as_bytes()
    }
}

impl std::fmt::Debug for InvitationQrPresentation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InvitationQrPresentation")
            .field("gateway_id", self.gateway_id())
            .field("gateway_base_url", &self.gateway_base_url())
            .field("transport_security", &self.transport_security())
            .field("credential", &"[redacted]")
            .field("deep_link", &"[redacted]")
            .field("qr_payload", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvitationSessionCommitState {
    RefreshReady,
    AwaitingSecureStorage,
    AwaitingRegistry,
    DurableSessionUnbound,
    ReadyToConnect,
}

pub struct InvitationRefreshEnvelope {
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub token_family_id: TokenFamilyId,
    pub installation_id: String,
    pub client_kind: ClientKind,
    pub refresh_generation: u64,
    pub refresh_expires_at_unix: u64,
    refresh_token: AuthSecretString,
}

impl InvitationRefreshEnvelope {
    pub fn refresh_token(&self) -> &str {
        self.refresh_token.expose_secret()
    }
}

impl std::fmt::Debug for InvitationRefreshEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InvitationRefreshEnvelope")
            .field("gateway_id", &self.gateway_id)
            .field("principal_id", &self.principal_id)
            .field("device_id", &self.device_id)
            .field("session_id", &self.session_id)
            .field("token_family_id", &self.token_family_id)
            .field("installation_id", &self.installation_id)
            .field("client_kind", &self.client_kind)
            .field("refresh_generation", &self.refresh_generation)
            .field("refresh_expires_at_unix", &self.refresh_expires_at_unix)
            .field("refresh_token", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvitationRegistryBinding {
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub member: MemberSummary,
    pub workspace_ids: Vec<WorkspaceId>,
}

pub struct InvitationAccessGrant {
    pub gateway_id: GatewayId,
    pub principal_id: PrincipalId,
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub access_expires_at_unix: u64,
    access_token: AuthSecretString,
}

impl InvitationAccessGrant {
    pub fn access_token(&self) -> &str {
        self.access_token.expose_secret()
    }
}

impl std::fmt::Debug for InvitationAccessGrant {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InvitationAccessGrant")
            .field("gateway_id", &self.gateway_id)
            .field("principal_id", &self.principal_id)
            .field("device_id", &self.device_id)
            .field("session_id", &self.session_id)
            .field("access_expires_at_unix", &self.access_expires_at_unix)
            .field("access_token", &"[redacted]")
            .finish()
    }
}

pub struct InvitationSessionCleanup {
    gateway_base_url: GatewayBaseUrl,
    session_id: AuthSessionId,
    access_token: AuthSecretString,
}

impl InvitationSessionCleanup {
    pub fn gateway_base_url(&self) -> &GatewayBaseUrl {
        &self.gateway_base_url
    }

    pub fn session_id(&self) -> &AuthSessionId {
        &self.session_id
    }

    pub fn access_token(&self) -> &str {
        self.access_token.expose_secret()
    }
}

impl std::fmt::Debug for InvitationSessionCleanup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InvitationSessionCleanup")
            .field("gateway_base_url", &self.gateway_base_url)
            .field("session_id", &self.session_id)
            .field("access_token", &"[redacted]")
            .finish()
    }
}

/// Ordering guard for the secret-bearing result of invitation acceptance.
///
/// The access token cannot be obtained until the caller confirms both durable
/// refresh storage and registry persistence. A secure-storage failure yields
/// only a best-effort cleanup capability, never a connected-state grant.
pub struct InvitationSessionCommit {
    state: InvitationSessionCommitState,
    gateway_base_url: GatewayBaseUrl,
    gateway_id: GatewayId,
    principal_id: PrincipalId,
    device_id: DeviceId,
    session_id: AuthSessionId,
    token_family_id: TokenFamilyId,
    installation_id: String,
    client_kind: ClientKind,
    refresh_generation: u64,
    refresh_expires_at_unix: u64,
    refresh_token: Option<AuthSecretString>,
    access_expires_at_unix: u64,
    access_token: Option<AuthSecretString>,
    member: MemberSummary,
    workspace_ids: Vec<WorkspaceId>,
}

impl InvitationSessionCommit {
    pub fn new(
        invitation: &InvitationQrPresentation,
        accepted: InvitationAcceptResponse,
        expected_installation_id: &str,
    ) -> ClientResult<Self> {
        let grant = accepted.grant;
        let workspace_ids = accepted.workspace_ids;
        let workspaces_are_canonical = !workspace_ids.is_empty()
            && workspace_ids.len() <= pioneer_protocol::INVITATION_MAX_WORKSPACE_GRANTS
            && workspace_ids.windows(2).all(|pair| pair[0] < pair[1]);
        if grant.gateway.id != *invitation.gateway_id()
            || grant.auth_protocol_version != pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION
            || grant.principal.kind != PrincipalKind::User
            || accepted.member.principal_id != grant.principal.id
            || accepted.member.kind != PrincipalKind::User
            || accepted
                .member
                .role_key
                .as_ref()
                .is_none_or(|role| role.as_str() != pioneer_protocol::MEMBER_ROLE_KEY)
            || accepted.member.status != PrincipalStatus::Active
            || accepted.member.display_name != grant.principal.display_name
            || accepted.member.nickname != grant.principal.nickname
            || grant.device.installation_id != expected_installation_id
            || grant.device.status != DeviceStatus::Active
            || grant.session.status != pioneer_protocol::AuthSessionStatus::Active
            || grant.session.device_id != grant.device.id
            || grant.session.refresh_generation != grant.refresh_generation
            || grant.session.refresh_expires_at_unix != grant.refresh_expires_at_unix
            || grant.refresh_generation != 0
            || grant.access_token.expose_secret().is_empty()
            || grant.refresh_token.expose_secret().is_empty()
            || grant.access_expires_at_unix == 0
            || grant.refresh_expires_at_unix == 0
            || grant.credential_storage_order
                != CredentialStorageOrder::PersistRefreshBeforeActivatingAccess
            || !workspaces_are_canonical
        {
            return Err(ClientError::protocol(
                "inconsistent invitation session grant",
            ));
        }
        Ok(Self {
            state: InvitationSessionCommitState::RefreshReady,
            gateway_base_url: invitation.gateway_base_url().clone(),
            gateway_id: grant.gateway.id,
            principal_id: grant.principal.id,
            device_id: grant.device.id,
            session_id: grant.session.id,
            token_family_id: grant.session.token_family_id,
            installation_id: grant.device.installation_id,
            client_kind: grant.device.client_kind,
            refresh_generation: grant.refresh_generation,
            refresh_expires_at_unix: grant.refresh_expires_at_unix,
            refresh_token: Some(grant.refresh_token),
            access_expires_at_unix: grant.access_expires_at_unix,
            access_token: Some(grant.access_token),
            member: accepted.member,
            workspace_ids,
        })
    }

    pub fn state(&self) -> InvitationSessionCommitState {
        self.state
    }

    pub fn take_refresh_for_secure_storage(&mut self) -> ClientResult<InvitationRefreshEnvelope> {
        if self.state != InvitationSessionCommitState::RefreshReady {
            return Err(ClientError::invalid_state(
                "invitation refresh credential already consumed",
            ));
        }
        let refresh_token = self
            .refresh_token
            .take()
            .ok_or_else(|| ClientError::invalid_state("missing invitation refresh credential"))?;
        self.state = InvitationSessionCommitState::AwaitingSecureStorage;
        Ok(InvitationRefreshEnvelope {
            gateway_id: self.gateway_id.clone(),
            principal_id: self.principal_id.clone(),
            device_id: self.device_id.clone(),
            session_id: self.session_id.clone(),
            token_family_id: self.token_family_id.clone(),
            installation_id: self.installation_id.clone(),
            client_kind: self.client_kind,
            refresh_generation: self.refresh_generation,
            refresh_expires_at_unix: self.refresh_expires_at_unix,
            refresh_token,
        })
    }

    pub fn secure_storage_committed(&mut self) -> ClientResult<InvitationRegistryBinding> {
        if self.state != InvitationSessionCommitState::AwaitingSecureStorage {
            return Err(ClientError::invalid_state(
                "invitation secure storage commit is out of order",
            ));
        }
        self.state = InvitationSessionCommitState::AwaitingRegistry;
        Ok(self.registry_binding())
    }

    pub fn registry_committed(&mut self) -> ClientResult<InvitationAccessGrant> {
        if self.state != InvitationSessionCommitState::AwaitingRegistry {
            return Err(ClientError::invalid_state(
                "invitation registry commit is out of order",
            ));
        }
        let access_token = self
            .access_token
            .take()
            .ok_or_else(|| ClientError::invalid_state("missing invitation access credential"))?;
        self.state = InvitationSessionCommitState::ReadyToConnect;
        Ok(InvitationAccessGrant {
            gateway_id: self.gateway_id.clone(),
            principal_id: self.principal_id.clone(),
            device_id: self.device_id.clone(),
            session_id: self.session_id.clone(),
            access_expires_at_unix: self.access_expires_at_unix,
            access_token,
        })
    }

    pub fn registry_failed(&mut self) -> ClientResult<()> {
        if self.state != InvitationSessionCommitState::AwaitingRegistry {
            return Err(ClientError::invalid_state(
                "invitation registry failure is out of order",
            ));
        }
        self.state = InvitationSessionCommitState::DurableSessionUnbound;
        self.access_token.take();
        Ok(())
    }

    pub fn secure_storage_failed(mut self) -> ClientResult<InvitationSessionCleanup> {
        if self.state != InvitationSessionCommitState::AwaitingSecureStorage {
            return Err(ClientError::invalid_state(
                "invitation secure storage failure is out of order",
            ));
        }
        let access_token = self
            .access_token
            .take()
            .ok_or_else(|| ClientError::invalid_state("missing invitation access credential"))?;
        Ok(InvitationSessionCleanup {
            gateway_base_url: self.gateway_base_url,
            session_id: self.session_id,
            access_token,
        })
    }

    fn registry_binding(&self) -> InvitationRegistryBinding {
        InvitationRegistryBinding {
            gateway_id: self.gateway_id.clone(),
            principal_id: self.principal_id.clone(),
            device_id: self.device_id.clone(),
            session_id: self.session_id.clone(),
            member: self.member.clone(),
            workspace_ids: self.workspace_ids.clone(),
        }
    }
}

impl std::fmt::Debug for InvitationSessionCommit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InvitationSessionCommit")
            .field("state", &self.state)
            .field("gateway_base_url", &self.gateway_base_url)
            .field("gateway_id", &self.gateway_id)
            .field("principal_id", &self.principal_id)
            .field("device_id", &self.device_id)
            .field("session_id", &self.session_id)
            .field("refresh_token", &"[redacted]")
            .field("access_token", &"[redacted]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use pioneer_protocol::{
        AuthDeviceSnapshot, AuthGatewaySnapshot, AuthPrincipalSnapshot, AuthSessionSnapshot,
        AuthSessionStatus, ClientKind, CredentialStorageOrder, DeviceStatus, InvitationCredential,
        InvitationErrorReason, InvitationPresentation, RoleKey,
    };

    use super::*;

    fn presentation() -> InvitationPresentation {
        InvitationPresentation::new(
            GatewayBaseUrl::parse_presentation("91.224.86.172:17878").unwrap(),
            GatewayId::new("G00000000000000000001").unwrap(),
            InvitationCredential::parse(format!(
                "{}{}",
                pioneer_protocol::INVITATION_CREDENTIAL_PREFIX,
                "A".repeat(pioneer_protocol::INVITATION_CREDENTIAL_BODY_LEN)
            ))
            .unwrap(),
        )
        .unwrap()
    }

    fn accepted() -> InvitationAcceptResponse {
        let principal_id = PrincipalId::new("P00000000000000000001").unwrap();
        let device_id = DeviceId::new("D00000000000000000001").unwrap();
        let session_id = AuthSessionId::new("S00000000000000000001").unwrap();
        let workspace_ids = vec![
            WorkspaceId::new("W00000000000000000001").unwrap(),
            WorkspaceId::new("W00000000000000000002").unwrap(),
        ];
        InvitationAcceptResponse {
            grant: pioneer_protocol::AuthSessionGrant {
                gateway: AuthGatewaySnapshot {
                    id: GatewayId::new("G00000000000000000001").unwrap(),
                },
                principal: AuthPrincipalSnapshot {
                    id: principal_id.clone(),
                    kind: PrincipalKind::User,
                    display_name: "Member".to_owned(),
                    nickname: "member".to_owned(),
                },
                device: AuthDeviceSnapshot {
                    id: device_id.clone(),
                    installation_id: "installation-1".to_owned(),
                    display_name: "Pioneer App".to_owned(),
                    client_kind: ClientKind::Mobile,
                    status: DeviceStatus::Active,
                },
                session: AuthSessionSnapshot {
                    id: session_id,
                    device_id,
                    token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
                    status: AuthSessionStatus::Active,
                    refresh_generation: 0,
                    refresh_expires_at_unix: 200,
                },
                access_token: AuthSecretString::new("access-secret"),
                access_expires_at_unix: 100,
                refresh_token: AuthSecretString::new("refresh-secret"),
                refresh_expires_at_unix: 200,
                refresh_generation: 0,
                auth_protocol_version: pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION,
                credential_storage_order:
                    CredentialStorageOrder::PersistRefreshBeforeActivatingAccess,
            },
            member: MemberSummary {
                principal_id,
                kind: PrincipalKind::User,
                display_name: "Member".to_owned(),
                nickname: "member".to_owned(),
                role_key: Some(RoleKey::member()),
                status: PrincipalStatus::Active,
                avatar_revision: None,
            },
            workspace_ids,
        }
    }

    #[test]
    fn parser_and_qr_payload_use_the_one_canonical_secret_uri() {
        let canonical = presentation();
        let deep_link = canonical.deep_link().to_owned();
        let expected_gateway = canonical.gateway_id.clone();
        let presented = InvitationQrPresentation::parse(deep_link.as_str()).unwrap();

        assert_eq!(presented.deep_link(), deep_link);
        assert_eq!(presented.qr_payload(), deep_link.as_bytes());
        assert_eq!(
            presented.gateway_base_url().as_str(),
            "http://91.224.86.172:17878/"
        );
        assert_eq!(
            presented.transport_security(),
            InvitationTransportSecurity::InsecureWs
        );
        assert!(presented.verify_gateway_id(&expected_gateway).is_ok());
        assert!(
            presented
                .verify_gateway_id(&GatewayId::new("G00000000000000000002").unwrap())
                .is_err()
        );

        let rendered = format!("{presented:?}");
        assert!(!rendered.contains(presented.credential()));
        assert!(!rendered.contains("pioneer://invite"));
        assert!(rendered.contains("[redacted]"));
    }

    #[test]
    fn malformed_duplicate_and_query_secret_forms_are_rejected() {
        let canonical = presentation();
        let credential = canonical.token().to_owned();
        let gateway_id = canonical.gateway_id;
        for invalid in [
            format!(
                "pioneer://invite?gateway_base_url=http%3A%2F%2Flocalhost%3A17878%2F&gateway_id={gateway_id}&token={credential}#token={credential}"
            ),
            format!(
                "pioneer://invite?gateway_base_url=http%3A%2F%2Flocalhost%3A17878%2F&gateway_base_url=http%3A%2F%2Fother%3A17878%2F&gateway_id={gateway_id}#token={credential}"
            ),
            format!(
                "pioneer://invite?gateway_base_url=http%3A%2F%2Flocalhost%3A17878%2F&gateway_id={gateway_id}&unknown=1#token={credential}"
            ),
        ] {
            assert!(InvitationQrPresentation::parse(&invalid).is_err());
        }
    }

    #[test]
    fn gateway_mismatch_is_the_generic_corrective_error() {
        assert_eq!(
            presentation().verify_gateway_id(&GatewayId::new("G00000000000000000002").unwrap()),
            Err(InvitationErrorReason::InvitationUnavailable)
        );
    }

    #[test]
    fn accepted_grant_exposes_access_only_after_refresh_and_registry_commits() {
        let invitation = InvitationQrPresentation::from_presentation(presentation());
        let mut commit =
            InvitationSessionCommit::new(&invitation, accepted(), "installation-1").unwrap();
        assert_eq!(commit.state(), InvitationSessionCommitState::RefreshReady);
        assert!(commit.registry_committed().is_err());

        let refresh = commit.take_refresh_for_secure_storage().unwrap();
        assert_eq!(refresh.refresh_token(), "refresh-secret");
        assert!(!format!("{refresh:?}").contains("refresh-secret"));
        assert_eq!(
            commit.state(),
            InvitationSessionCommitState::AwaitingSecureStorage
        );
        assert!(commit.registry_committed().is_err());

        let binding = commit.secure_storage_committed().unwrap();
        assert_eq!(binding.principal_id, accepted().member.principal_id);
        assert_eq!(
            commit.state(),
            InvitationSessionCommitState::AwaitingRegistry
        );
        let access = commit.registry_committed().unwrap();
        assert_eq!(access.access_token(), "access-secret");
        assert!(!format!("{access:?}").contains("access-secret"));
        assert_eq!(commit.state(), InvitationSessionCommitState::ReadyToConnect);
    }

    #[test]
    fn secure_storage_failure_yields_only_redacted_best_effort_cleanup() {
        let invitation = InvitationQrPresentation::from_presentation(presentation());
        let mut commit =
            InvitationSessionCommit::new(&invitation, accepted(), "installation-1").unwrap();
        let refresh = commit.take_refresh_for_secure_storage().unwrap();
        drop(refresh);
        let cleanup = commit.secure_storage_failed().unwrap();
        assert_eq!(cleanup.session_id().as_str(), "S00000000000000000001");
        assert_eq!(cleanup.access_token(), "access-secret");
        assert!(!format!("{cleanup:?}").contains("access-secret"));
        assert!(!format!("{cleanup:?}").contains(invitation.credential()));
    }

    #[test]
    fn malformed_or_mismatched_accept_grants_never_enter_commit_workflow() {
        let invitation = InvitationQrPresentation::from_presentation(presentation());
        let mut mismatch = accepted();
        mismatch.grant.gateway.id = GatewayId::new("G00000000000000000002").unwrap();
        assert!(InvitationSessionCommit::new(&invitation, mismatch, "installation-1").is_err());

        let mut wrong_installation = accepted();
        wrong_installation.grant.device.installation_id = "other".to_owned();
        assert!(
            InvitationSessionCommit::new(&invitation, wrong_installation, "installation-1")
                .is_err()
        );

        let mut non_initial_generation = accepted();
        non_initial_generation.grant.refresh_generation = 1;
        non_initial_generation.grant.session.refresh_generation = 1;
        assert!(
            InvitationSessionCommit::new(&invitation, non_initial_generation, "installation-1",)
                .is_err()
        );

        let mut excessive_grants = accepted();
        excessive_grants.workspace_ids = (0..=pioneer_protocol::INVITATION_MAX_WORKSPACE_GRANTS)
            .map(|index| {
                WorkspaceId::new(format!("W{index:020}"))
                    .expect("generated workspace id must remain canonical")
            })
            .collect();
        assert!(
            InvitationSessionCommit::new(&invitation, excessive_grants, "installation-1").is_err()
        );
    }
}
