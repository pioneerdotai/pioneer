//! Desktop application effects for the shared invitation commit state machine.

use std::fmt;

use anyhow::{Context, Result};
use pioneer_client::gateway::{
    invitation::{InvitationQrPresentation, InvitationSessionCleanup, InvitationSessionCommit},
    registry::commit_registry_v3_binding,
    runtime as client_gateway_runtime, setup as client_gateway_setup,
    types::{GatewayEndpoint, GatewayRegistry},
};
use pioneer_protocol::{AuthSecretString, ClientKind, InvitationAcceptResponse};

use crate::gateway::{
    registry::save_registry,
    secrets::{DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION, DesktopGatewaySessionSecret},
};

use super::{GatewayRuntime, map_gateway_profile_error};

#[derive(Clone)]
pub(crate) struct DesktopInvitationRegistryRecovery {
    staged_registry: GatewayRegistry,
    active_registry: GatewayRegistry,
    endpoint: GatewayEndpoint,
}

impl fmt::Debug for DesktopInvitationRegistryRecovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DesktopInvitationRegistryRecovery")
            .field("endpoint_id", &self.endpoint.id)
            .finish_non_exhaustive()
    }
}

pub(crate) enum DesktopInvitationCommitError {
    SecureStorage(InvitationSessionCleanup),
    Registry(DesktopInvitationRegistryRecovery),
    Invalid { _source: anyhow::Error },
}

impl DesktopInvitationCommitError {
    fn invalid(source: anyhow::Error) -> Self {
        Self::Invalid { _source: source }
    }
}

impl fmt::Debug for DesktopInvitationCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SecureStorage(_) => formatter.write_str("SecureStorage([redacted])"),
            Self::Registry(recovery) => formatter.debug_tuple("Registry").field(recovery).finish(),
            Self::Invalid { .. } => formatter.write_str("Invalid([redacted])"),
        }
    }
}

impl GatewayRuntime {
    pub(crate) fn invitation_installation_id(&self) -> Result<String> {
        self.registry
            .installation_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .context("desktop Gateway registry has no installation id")
    }

    pub(crate) fn commit_accepted_invitation(
        &mut self,
        invitation: &InvitationQrPresentation,
        accepted: InvitationAcceptResponse,
        gateway_name: &str,
    ) -> std::result::Result<GatewayEndpoint, DesktopInvitationCommitError> {
        if self.registry.remotes.iter().any(|endpoint| {
            endpoint.server_gateway_id.as_ref() == Some(invitation.gateway_id())
                && endpoint.session_ref.is_some()
        }) {
            return Err(DesktopInvitationCommitError::invalid(anyhow::anyhow!(
                "desktop Gateway already has a durable session"
            )));
        }

        let installation_id = self
            .invitation_installation_id()
            .map_err(DesktopInvitationCommitError::invalid)?;
        let change = client_gateway_setup::plan_add_remote_gateway(
            &self.registry,
            client_gateway_setup::AddRemoteGatewayInput {
                name: gateway_name,
                gateway_base_url: invitation.gateway_base_url().as_str(),
                new_endpoint_id: client_gateway_setup::generated_remote_gateway_endpoint_id(),
                default_remote_name: t!(
                    "gateway.endpoint.remote_name",
                    index = self.registry.remotes.len() + 1
                )
                .to_string(),
            },
        )
        .map_err(map_gateway_profile_error)
        .map_err(DesktopInvitationCommitError::invalid)?;
        let mut staged_registry = self.registry.clone();
        let staged = change
            .apply_to_registry(
                &mut staged_registry,
                client_gateway_setup::AddRemoteGatewayApplyMode::ProfileOnly,
            )
            .map_err(map_gateway_profile_error)
            .map_err(DesktopInvitationCommitError::invalid)?;

        let mut commit = InvitationSessionCommit::new(invitation, accepted, &installation_id)
            .map_err(anyhow::Error::new)
            .map_err(DesktopInvitationCommitError::invalid)?;
        let refresh = commit
            .take_refresh_for_secure_storage()
            .map_err(anyhow::Error::new)
            .map_err(DesktopInvitationCommitError::invalid)?;
        if refresh.client_kind != ClientKind::Desktop {
            let cleanup = commit
                .secure_storage_failed()
                .map_err(anyhow::Error::new)
                .map_err(DesktopInvitationCommitError::invalid)?;
            return Err(DesktopInvitationCommitError::SecureStorage(cleanup));
        }

        let session_ref = staged.endpoint.id.clone();
        let session = DesktopGatewaySessionSecret {
            schema_version: DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION,
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
        if self
            .secrets
            .put_gateway_session(
                session_ref.as_str(),
                &session,
                Some(format!("{} session", staged.endpoint.name)),
            )
            .is_err()
        {
            let cleanup = commit
                .secure_storage_failed()
                .map_err(anyhow::Error::new)
                .map_err(DesktopInvitationCommitError::invalid)?;
            return Err(DesktopInvitationCommitError::SecureStorage(cleanup));
        }

        let binding = commit
            .secure_storage_committed()
            .map_err(anyhow::Error::new)
            .map_err(DesktopInvitationCommitError::invalid)?;
        if binding.gateway_id != refresh.gateway_id
            || binding.principal_id != refresh.principal_id
            || binding.device_id != refresh.device_id
            || binding.session_id != refresh.session_id
        {
            let _ = self.secrets.delete_gateway_session(session_ref.as_str());
            commit
                .registry_failed()
                .map_err(anyhow::Error::new)
                .map_err(DesktopInvitationCommitError::invalid)?;
            return Err(DesktopInvitationCommitError::invalid(anyhow::anyhow!(
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
        .map_err(DesktopInvitationCommitError::invalid)?;
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
        .map_err(map_gateway_profile_error)
        .map_err(DesktopInvitationCommitError::invalid)?;
        let recovery = DesktopInvitationRegistryRecovery {
            staged_registry: staged_registry.clone(),
            active_registry: active_plan.registry.clone(),
            endpoint: active_plan.endpoint.clone(),
        };

        if save_registry(&self.registry_path, &staged_registry).is_err() {
            commit
                .registry_failed()
                .map_err(anyhow::Error::new)
                .map_err(DesktopInvitationCommitError::invalid)?;
            return Err(DesktopInvitationCommitError::Registry(recovery));
        }
        let access = commit
            .registry_committed()
            .map_err(anyhow::Error::new)
            .map_err(DesktopInvitationCommitError::invalid)?;
        if access.gateway_id != refresh.gateway_id
            || access.principal_id != refresh.principal_id
            || access.device_id != refresh.device_id
            || access.session_id != refresh.session_id
        {
            return Err(DesktopInvitationCommitError::Registry(recovery));
        }
        if save_registry(&self.registry_path, &active_plan.registry).is_err() {
            return Err(DesktopInvitationCommitError::Registry(recovery));
        }
        self.registry = active_plan.registry;
        Ok(active_plan.endpoint)
    }

    pub(crate) fn recover_invitation_registry(
        &mut self,
        recovery: &DesktopInvitationRegistryRecovery,
    ) -> Result<GatewayEndpoint> {
        save_registry(&self.registry_path, &recovery.staged_registry)?;
        save_registry(&self.registry_path, &recovery.active_registry)?;
        self.registry = recovery.active_registry.clone();
        Ok(recovery.endpoint.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use pioneer_client::gateway::invitation::InvitationQrPresentation;
    use pioneer_protocol::{
        AuthDeviceSnapshot, AuthGatewaySnapshot, AuthPrincipalSnapshot, AuthSecretString,
        AuthSessionGrant, AuthSessionId, AuthSessionSnapshot, AuthSessionStatus, ClientKind,
        CredentialStorageOrder, DeviceId, DeviceStatus, GatewayBaseUrl, GatewayId,
        InvitationAcceptResponse, InvitationCredential, InvitationPresentation, MemberSummary,
        PrincipalId, PrincipalKind, PrincipalStatus, RoleKey, TokenFamilyId, WorkspaceId,
    };

    use crate::gateway::{
        registry::{load_registry, save_registry},
        secrets::DesktopSecrets,
        test_support::FailingDesktopSecretStore,
        tests::unique_temp_dir,
    };

    use super::*;

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
    fn commit_persists_refresh_then_binding_then_activation() {
        let mut runtime = GatewayRuntime::for_ws_spec_tests();
        let runtime_dir = unique_temp_dir();
        fs::create_dir_all(&runtime_dir).unwrap();
        runtime.registry_path = runtime_dir.join("gateway-registry.toml");
        save_registry(&runtime.registry_path, &runtime.registry).unwrap();
        let previous_active = runtime.registry.active_gateway_id.clone();
        let installation_id = runtime.invitation_installation_id().unwrap();
        let (presentation, accepted) = invitation_fixture(installation_id.as_str());

        let endpoint = runtime
            .commit_accepted_invitation(&presentation, accepted, "Joined Gateway")
            .expect("durable invitation commit");

        assert_eq!(runtime.active_gateway_id(), Some(endpoint.id.as_str()));
        assert_ne!(runtime.active_gateway_id(), previous_active.as_deref());
        assert!(
            runtime
                .secrets
                .has_gateway_session(endpoint.id.as_str())
                .unwrap()
        );
        let durable = load_registry(&runtime.registry_path, &runtime.config).unwrap();
        let durable_endpoint = durable
            .remotes
            .iter()
            .find(|candidate| candidate.id == endpoint.id)
            .unwrap();
        assert_eq!(
            durable.active_gateway_id.as_deref(),
            Some(endpoint.id.as_str())
        );
        assert_eq!(
            durable_endpoint.session_ref.as_deref(),
            Some(endpoint.id.as_str())
        );
        assert_eq!(
            durable_endpoint.workspace_id.as_deref(),
            Some("W00000000000000000001")
        );
        let _ = fs::remove_dir_all(runtime_dir);
    }

    #[test]
    fn secure_storage_failure_never_persists_or_activates_endpoint() {
        let mut runtime = GatewayRuntime::for_ws_spec_tests();
        let runtime_dir = unique_temp_dir();
        fs::create_dir_all(&runtime_dir).unwrap();
        runtime.registry_path = runtime_dir.join("gateway-registry.toml");
        save_registry(&runtime.registry_path, &runtime.registry).unwrap();
        let installation_id = runtime.invitation_installation_id().unwrap();
        let (presentation, accepted) = invitation_fixture(installation_id.as_str());
        let store = FailingDesktopSecretStore::new();
        store.fail_next_write();
        runtime.secrets = DesktopSecrets::new(store);

        let error = runtime
            .commit_accepted_invitation(&presentation, accepted, "Joined Gateway")
            .expect_err("injected secure storage failure");

        assert!(matches!(
            error,
            DesktopInvitationCommitError::SecureStorage(_)
        ));
        assert!(runtime.registry.remotes.is_empty());
        let durable = load_registry(&runtime.registry_path, &runtime.config).unwrap();
        assert!(durable.remotes.is_empty());
        assert_eq!(
            durable.active_gateway_id,
            runtime.registry.active_gateway_id
        );
        let _ = fs::remove_dir_all(runtime_dir);
    }

    #[test]
    fn registry_failure_retries_without_a_second_accept() {
        let mut runtime = GatewayRuntime::for_ws_spec_tests();
        let runtime_dir = unique_temp_dir();
        fs::create_dir_all(&runtime_dir).unwrap();
        runtime.registry_path = runtime_dir.clone();
        let installation_id = runtime.invitation_installation_id().unwrap();
        let (presentation, accepted) = invitation_fixture(installation_id.as_str());

        let recovery = match runtime
            .commit_accepted_invitation(&presentation, accepted, "Joined Gateway")
            .expect_err("directory path must reject registry write")
        {
            DesktopInvitationCommitError::Registry(recovery) => recovery,
            other => panic!("unexpected commit failure: {other:?}"),
        };
        assert!(
            runtime
                .secrets
                .has_gateway_session(recovery.endpoint.id.as_str())
                .unwrap()
        );
        assert!(runtime.registry.remotes.is_empty());

        runtime.registry_path = runtime_dir.join("gateway-registry.toml");
        let endpoint = runtime
            .recover_invitation_registry(&recovery)
            .expect("retry only durable registry writes");
        assert_eq!(runtime.active_gateway_id(), Some(endpoint.id.as_str()));
        assert!(
            runtime
                .secrets
                .has_gateway_session(endpoint.id.as_str())
                .unwrap()
        );
        let _ = fs::remove_dir_all(runtime_dir);
    }
}
