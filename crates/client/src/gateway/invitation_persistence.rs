//! Invitation credential/registry commit and retry policy, shared by native adapters.
use super::{
    invitation::{InvitationQrPresentation, InvitationSessionCleanup, InvitationSessionCommit},
    registry::commit_registry_v3_binding,
    runtime as client_gateway_runtime,
    session_envelope::{GATEWAY_SESSION_SCHEMA_VERSION, GatewaySessionEnvelope},
    session_refresh::GatewaySessionStorage,
    setup as client_gateway_setup,
    types::{GatewayEndpoint, GatewayRegistry},
};
use anyhow::Result;
use pioneer_protocol::{AuthSecretString, ClientKind, InvitationAcceptResponse};
use std::fmt;
#[derive(Clone)]
pub struct InvitationRegistryRecovery {
    installation_id: String,
    previous_endpoint: Option<GatewayEndpoint>,
    endpoint: GatewayEndpoint,
}

impl InvitationRegistryRecovery {
    pub fn endpoint(&self) -> &GatewayEndpoint {
        &self.endpoint
    }
}
impl fmt::Debug for InvitationRegistryRecovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InvitationRegistryRecovery")
            .field("endpoint_id", &self.endpoint.id)
            .finish_non_exhaustive()
    }
}

pub enum InvitationCommitError {
    SecureStorage(InvitationSessionCleanup),
    Registry(InvitationRegistryRecovery),
    Invalid { _source: anyhow::Error },
}

impl InvitationCommitError {
    pub fn invalid(source: anyhow::Error) -> Self {
        Self::Invalid { _source: source }
    }
}

impl fmt::Debug for InvitationCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SecureStorage(_) => formatter.write_str("SecureStorage([redacted])"),
            Self::Registry(recovery) => formatter.debug_tuple("Registry").field(recovery).finish(),
            Self::Invalid { .. } => formatter.write_str("Invalid([redacted])"),
        }
    }
}

pub fn commit_accepted_invitation(
    registry: &mut GatewayRegistry,
    storage: &dyn GatewaySessionStorage,
    expected_kind: ClientKind,
    default_remote_name: String,
    save: impl FnMut(&GatewayRegistry) -> Result<()>,
    invitation: &InvitationQrPresentation,
    accepted: InvitationAcceptResponse,
    gateway_name: &str,
) -> std::result::Result<GatewayEndpoint, InvitationCommitError> {
    let installation_id = registry.installation_id.as_deref().ok_or_else(|| {
        InvitationCommitError::invalid(anyhow::anyhow!("Gateway installation unavailable"))
    })?;
    let commit = InvitationSessionCommit::new(invitation, accepted, installation_id)
        .map_err(anyhow::Error::new)
        .map_err(InvitationCommitError::invalid)?;
    commit_invitation_session(
        registry,
        storage,
        expected_kind,
        default_remote_name,
        save,
        invitation,
        commit,
        gateway_name,
    )
}

pub fn commit_invitation_session(
    registry: &mut GatewayRegistry,
    storage: &dyn GatewaySessionStorage,
    expected_kind: ClientKind,
    default_remote_name: String,
    mut save: impl FnMut(&GatewayRegistry) -> Result<()>,
    invitation: &InvitationQrPresentation,
    mut commit: InvitationSessionCommit,
    gateway_name: &str,
) -> std::result::Result<GatewayEndpoint, InvitationCommitError> {
    if registry.remotes.iter().any(|endpoint| {
        endpoint.server_gateway_id.as_ref() == Some(invitation.gateway_id())
            && endpoint.session_ref.is_some()
    }) {
        return Err(InvitationCommitError::invalid(anyhow::anyhow!(
            "Gateway already has a durable session"
        )));
    }

    let installation_id = registry
        .installation_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| {
            InvitationCommitError::invalid(anyhow::anyhow!("Gateway installation unavailable"))
        })?
        .to_owned();
    let change = client_gateway_setup::plan_add_remote_gateway(
        &registry,
        client_gateway_setup::AddRemoteGatewayInput {
            name: gateway_name,
            gateway_base_url: invitation.gateway_base_url().as_str(),
            new_endpoint_id: client_gateway_setup::generated_remote_gateway_endpoint_id(),
            default_remote_name,
        },
    )
    .map_err(anyhow::Error::new)
    .map_err(InvitationCommitError::invalid)?;
    let mut staged_registry = registry.clone();
    let staged = change
        .apply_to_registry(
            &mut staged_registry,
            client_gateway_setup::AddRemoteGatewayApplyMode::ProfileOnly,
        )
        .map_err(anyhow::Error::new)
        .map_err(InvitationCommitError::invalid)?;

    let refresh = commit
        .take_refresh_for_secure_storage()
        .map_err(anyhow::Error::new)
        .map_err(InvitationCommitError::invalid)?;
    if refresh.client_kind != expected_kind || refresh.installation_id != installation_id {
        let cleanup = commit
            .secure_storage_failed()
            .map_err(anyhow::Error::new)
            .map_err(InvitationCommitError::invalid)?;
        return Err(InvitationCommitError::SecureStorage(cleanup));
    }

    let session_ref = staged.endpoint.id.clone();
    let session = GatewaySessionEnvelope {
        schema_version: GATEWAY_SESSION_SCHEMA_VERSION,
        gateway_id: refresh.gateway_id.clone(),
        principal_id: refresh.principal_id.clone(),
        device_id: refresh.device_id.clone(),
        session_id: refresh.session_id.clone(),
        token_family_id: refresh.token_family_id.clone(),
        installation_id: refresh.installation_id.clone(),
        refresh_generation: refresh.refresh_generation,
        refresh_expires_at_unix: refresh.refresh_expires_at_unix,
        refresh_token: AuthSecretString::new(refresh.refresh_token()),
        pending_refresh_request_id: None,
    };
    let mut storage_endpoint = staged.endpoint.clone();
    storage_endpoint.session_ref = Some(session_ref.clone());
    if storage.persist(&storage_endpoint, &session).is_err() {
        let cleanup = commit
            .secure_storage_failed()
            .map_err(anyhow::Error::new)
            .map_err(InvitationCommitError::invalid)?;
        return Err(InvitationCommitError::SecureStorage(cleanup));
    }

    let binding = commit
        .secure_storage_committed()
        .map_err(anyhow::Error::new)
        .map_err(InvitationCommitError::invalid)?;
    if binding.gateway_id != refresh.gateway_id
        || binding.principal_id != refresh.principal_id
        || binding.device_id != refresh.device_id
        || binding.session_id != refresh.session_id
    {
        let _ = storage.delete(&storage_endpoint);
        commit
            .registry_failed()
            .map_err(anyhow::Error::new)
            .map_err(InvitationCommitError::invalid)?;
        return Err(InvitationCommitError::invalid(anyhow::anyhow!(
            "inconsistent invitation registry binding"
        )));
    }
    commit_registry_v3_binding(
        &mut staged_registry,
        staged.endpoint.id.as_str(),
        session_ref.as_str(),
        &binding.gateway_id,
    )
    .map_err(anyhow::Error::new)
    .map_err(InvitationCommitError::invalid)?;
    if let Some(endpoint) = client_gateway_runtime::endpoint_by_id_mut(
        &mut staged_registry,
        staged.endpoint.id.as_str(),
    ) {
        endpoint.workspace_id = binding
            .workspace_ids
            .first()
            .map(|workspace_id| workspace_id.as_str().to_owned());
    }
    let active_plan = client_gateway_setup::plan_activate_gateway_registry(
        &staged_registry,
        staged.endpoint.id.as_str(),
    )
    .map_err(anyhow::Error::new)
    .map_err(InvitationCommitError::invalid)?;
    let recovery = InvitationRegistryRecovery {
        installation_id: installation_id.to_owned(),
        previous_endpoint: client_gateway_runtime::endpoint_by_id(
            registry,
            &active_plan.endpoint.id,
        )
        .cloned(),
        endpoint: active_plan.endpoint.clone(),
    };

    if save(&staged_registry).is_err() {
        commit
            .registry_failed()
            .map_err(anyhow::Error::new)
            .map_err(InvitationCommitError::invalid)?;
        return Err(InvitationCommitError::Registry(recovery));
    }
    let access = commit
        .registry_committed()
        .map_err(anyhow::Error::new)
        .map_err(InvitationCommitError::invalid)?;
    if access.gateway_id != refresh.gateway_id
        || access.principal_id != refresh.principal_id
        || access.device_id != refresh.device_id
        || access.session_id != refresh.session_id
    {
        return Err(InvitationCommitError::Registry(recovery));
    }
    if save(&active_plan.registry).is_err() {
        return Err(InvitationCommitError::Registry(recovery));
    }
    *registry = active_plan.registry;
    Ok(active_plan.endpoint)
}

pub fn recover_invitation_registry(
    registry: &mut GatewayRegistry,
    recovery: &InvitationRegistryRecovery,
    mut save: impl FnMut(&GatewayRegistry) -> Result<()>,
) -> Result<GatewayEndpoint> {
    anyhow::ensure!(
        registry.installation_id.as_deref() == Some(recovery.installation_id.as_str()),
        "invitation recovery installation changed"
    );
    let current = client_gateway_runtime::endpoint_by_id(registry, &recovery.endpoint.id);
    let mut endpoint = recovery.endpoint.clone();
    if let Some(current) = current {
        let same_binding = |other: &GatewayEndpoint| {
            current.gateway_base_url == other.gateway_base_url
                && current.kind == other.kind
                && current.session_ref == other.session_ref
                && current.server_gateway_id == other.server_gateway_id
        };
        anyhow::ensure!(
            same_binding(&recovery.endpoint)
                || recovery
                    .previous_endpoint
                    .as_ref()
                    .is_some_and(same_binding),
            "invitation recovery binding changed"
        );
        if let Some(previous) = &recovery.previous_endpoint {
            if current.name != previous.name {
                endpoint.name = current.name.clone();
            }
            if current.workspace_id != previous.workspace_id {
                endpoint.workspace_id = current.workspace_id.clone();
            }
        }
    } else {
        anyhow::ensure!(
            recovery.previous_endpoint.is_none(),
            "invitation recovery endpoint removed"
        );
    }
    anyhow::ensure!(
        !registry
            .endpoints()
            .iter()
            .any(|other| other.id != endpoint.id
                && (other.gateway_base_url == endpoint.gateway_base_url
                    || endpoint.server_gateway_id.is_some()
                        && other.server_gateway_id == endpoint.server_gateway_id
                    || endpoint.session_ref.is_some()
                        && other.session_ref == endpoint.session_ref)),
        "invitation recovery endpoint identity occupied"
    );
    let mut staged = registry.clone();
    if let Some(current) = client_gateway_runtime::endpoint_by_id_mut(&mut staged, &endpoint.id) {
        *current = endpoint.clone();
    } else {
        staged.remotes.push(endpoint.clone());
    }
    let plan = client_gateway_setup::plan_activate_gateway_registry(&staged, &endpoint.id)?;
    save(&staged)?;
    save(&plan.registry)?;
    *registry = plan.registry;
    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use super::super::provisioning::tests::{MemoryStorage, registry};
    use super::*;
    use pioneer_protocol::*;
    fn invitation_fixture(
        installation_id: &str,
    ) -> (InvitationQrPresentation, InvitationAcceptResponse) {
        let gateway_id = GatewayId::new("G00000000000000000001").unwrap();
        let principal_id = PrincipalId::new("P00000000000000000001").unwrap();
        let device_id = DeviceId::new("D00000000000000000001").unwrap();
        let session_id = AuthSessionId::new("S00000000000000000001").unwrap();
        let presentation = InvitationQrPresentation::from_presentation(
            InvitationPresentation::new(
                GatewayBaseUrl::parse_presentation("https://gateway.example/").unwrap(),
                gateway_id.clone(),
                InvitationCredential::parse(format!(
                    "{}{}",
                    pioneer_protocol::INVITATION_CREDENTIAL_PREFIX,
                    "A".repeat(pioneer_protocol::INVITATION_CREDENTIAL_BODY_LEN)
                ))
                .unwrap(),
            )
            .unwrap(),
        );
        let response = InvitationAcceptResponse {
            grant: AuthSessionGrant {
                gateway: AuthGatewaySnapshot { id: gateway_id },
                principal: AuthPrincipalSnapshot {
                    id: principal_id.clone(),
                    kind: PrincipalKind::User,
                    display_name: "Member".to_owned(),
                    nickname: "member".to_owned(),
                    avatar_revision: None,
                },
                device: AuthDeviceSnapshot {
                    id: device_id.clone(),
                    installation_id: installation_id.to_owned(),
                    display_name: "Pioneer Desktop".to_owned(),
                    client_kind: ClientKind::Desktop,
                    status: DeviceStatus::Active,
                },
                session: AuthSessionSnapshot {
                    id: session_id,
                    device_id,
                    token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
                    status: AuthSessionStatus::Active,
                    refresh_generation: 0,
                    refresh_expires_at_unix: 2_000_000_000,
                },
                access_token: AuthSecretString::new("access-secret"),
                access_expires_at_unix: 1_900_000_000,
                refresh_token: AuthSecretString::new(format!(
                    "{}{}",
                    pioneer_protocol::REFRESH_CREDENTIAL_PREFIX,
                    "r".repeat(pioneer_protocol::REFRESH_CREDENTIAL_BODY_LEN)
                )),
                refresh_expires_at_unix: 2_000_000_000,
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
                role: pioneer_protocol::AuthorizationRolePresentation {
                    key: "member".to_owned(),
                    display_name: "Member".to_owned(),
                    description: "Workspace collaborator".to_owned(),
                    built_in: true,
                },
                lifecycle_managed: true,
                status: PrincipalStatus::Active,
                avatar_revision: None,
            },
            workspace_ids: vec![WorkspaceId::new("W00000000000000000001").unwrap()],
        };
        (presentation, response)
    }

    #[test]
    fn refresh_precedes_binding_and_selected_endpoint_for_both_client_kinds() {
        for kind in [ClientKind::Desktop, ClientKind::Mobile] {
            let mut registry = registry();
            let storage = MemoryStorage::default();
            let (uri, mut response) =
                invitation_fixture(registry.installation_id.as_deref().unwrap());
            response.grant.device.client_kind = kind;
            let saves = std::cell::RefCell::new(vec![]);
            let endpoint = commit_accepted_invitation(
                &mut registry,
                &storage,
                kind,
                "Remote".into(),
                |next| {
                    let endpoint = next.remotes.last().unwrap();
                    assert!(storage.load(endpoint)?.is_some());
                    saves.borrow_mut().push(next.active_gateway_id.clone());
                    Ok(())
                },
                &uri,
                response,
                "Invited",
            )
            .unwrap();
            assert_eq!(saves.borrow().len(), 2);
            assert_eq!(saves.borrow()[0], None);
            assert_eq!(saves.borrow()[1].as_deref(), Some(endpoint.id.as_str()));
            assert_eq!(
                registry.active_gateway_id.as_deref(),
                Some(endpoint.id.as_str())
            );
            assert!(
                !serde_json::to_string(&registry)
                    .unwrap()
                    .contains("access-secret")
            );
        }
    }
    #[test]
    fn storage_failure_retains_cleanup_without_publishing_or_saving_registry() {
        let mut registry = registry();
        let before = registry.clone();
        let storage = MemoryStorage::default();
        storage.fail_write.set(true);
        let (uri, response) = invitation_fixture(registry.installation_id.as_deref().unwrap());
        let result = commit_accepted_invitation(
            &mut registry,
            &storage,
            ClientKind::Desktop,
            "Remote".into(),
            |_| panic!("failed credential cannot publish binding"),
            &uri,
            response,
            "Invited",
        );
        assert!(matches!(
            result,
            Err(InvitationCommitError::SecureStorage(_))
        ));
        assert_eq!(registry, before);
    }
    #[test]
    fn registry_retry_uses_durable_recovery_without_reaccepting_one_use_invitation() {
        for fail_at in [1, 2] {
            let mut registry = registry();
            let storage = MemoryStorage::default();
            let (uri, response) = invitation_fixture(registry.installation_id.as_deref().unwrap());
            let count = std::cell::Cell::new(0);
            let result = commit_accepted_invitation(
                &mut registry,
                &storage,
                ClientKind::Desktop,
                "Remote".into(),
                |_| {
                    count.set(count.get() + 1);
                    if count.get() == fail_at {
                        anyhow::bail!("synthetic registry failure");
                    }
                    Ok(())
                },
                &uri,
                response,
                "Invited",
            );
            let Err(InvitationCommitError::Registry(recovery)) = result else {
                panic!("must preserve recovery")
            };
            assert!(storage.load(recovery.endpoint()).unwrap().is_some());
            registry.local.as_mut().unwrap().workspace_id = Some("newer-workspace".into());
            let mut replaced = registry.clone();
            let mut other = recovery.endpoint().clone();
            other.session_ref = Some("newer-credential".into());
            replaced.remotes.push(other);
            let before = replaced.clone();
            assert!(
                recover_invitation_registry(&mut replaced, &recovery, |_| panic!(
                    "newer binding must not be overwritten"
                ))
                .is_err()
            );
            assert_eq!(replaced, before);
            let endpoint =
                recover_invitation_registry(&mut registry, &recovery, |_| Ok(())).unwrap();
            assert_eq!(
                registry.active_gateway_id.as_deref(),
                Some(endpoint.id.as_str())
            );
            assert_eq!(
                registry.local.as_ref().unwrap().workspace_id.as_deref(),
                Some("newer-workspace")
            );
        }
    }
}
