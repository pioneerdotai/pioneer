use pioneer_client::authorization::{
    CurrentPrincipalPresentation, PrincipalPresentationCapabilities, SessionListRowPresentation,
    ThreadPresentationCapabilities, current_principal_presentation,
    principal_presentation_capabilities, session_list_row_presentation,
    thread_presentation_capabilities,
};
use pioneer_protocol::{
    AuthMeResponse, AuthSessionListItem, AuthorizationCapabilitySnapshot,
    AuthorizationThreadCapabilities, AuthorizationWorkspaceCapabilities, MemberSummary,
};
use serde::Deserialize;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientArtifactPresentationPolicyRequest {
    pub can_read_artifacts: bool,
    pub can_attach_artifacts: bool,
    pub connected: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientMemberPresentationRequest {
    pub auth: AuthMeResponse,
    pub capability_snapshot: AuthorizationCapabilitySnapshot,
    pub member: MemberSummary,
    pub is_workspace_member: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientCurrentPrincipalPresentationRequest {
    pub auth: AuthMeResponse,
    pub capability_snapshot: AuthorizationCapabilitySnapshot,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadCreateVisibilityRequest {
    pub capabilities: AuthorizationWorkspaceCapabilities,
    pub origin_kind: pioneer_protocol::ThreadOriginKind,
}

pub fn principal_capabilities(
    snapshot: AuthorizationCapabilitySnapshot,
) -> PrincipalPresentationCapabilities {
    principal_presentation_capabilities(&snapshot)
}

/// Mobile boundary adapter for the same shell-neutral thread projection used
/// by Desktop. Kept typed so Proposal 63's matrix can compare both shells
/// without duplicating capability inference in test code.
pub fn thread_capabilities(
    capabilities: AuthorizationThreadCapabilities,
) -> ThreadPresentationCapabilities {
    thread_presentation_capabilities(Some(&capabilities))
}

pub fn artifact_presentation_policy(
    request: ClientArtifactPresentationPolicyRequest,
) -> pioneer_client::artifacts::presentation::ArtifactPresentationPolicy {
    pioneer_client::artifacts::presentation::artifact_presentation_policy(
        request.can_read_artifacts,
        request.can_attach_artifacts,
        request.connected,
    )
}

pub fn current_principal(
    request: ClientCurrentPrincipalPresentationRequest,
) -> Result<CurrentPrincipalPresentation, String> {
    if request.capability_snapshot.principal_id != request.auth.principal.id {
        return Err("authorization capability principal mismatch".to_owned());
    }
    let capabilities = capabilities_for_auth(&request.auth, &request.capability_snapshot);
    Ok(current_principal_presentation(
        &request.auth,
        capabilities,
        &request.capability_snapshot.role,
    ))
}

pub fn session_list_row(item: AuthSessionListItem) -> SessionListRowPresentation {
    session_list_row_presentation(&item)
}

pub fn thread_create_visibility(
    request: ClientThreadCreateVisibilityRequest,
) -> pioneer_client::threads::scope::ThreadCreateVisibilityPlan {
    pioneer_client::threads::scope::thread_create_visibility_plan(
        Some(&request.capabilities),
        request.origin_kind,
    )
}

pub fn member_presentation(
    request: ClientMemberPresentationRequest,
) -> pioneer_client::administration::MemberListRow {
    let capabilities = capabilities_for_auth(&request.auth, &request.capability_snapshot);
    pioneer_client::administration::member_list_row(
        &request.member,
        Some(&request.auth.principal.id),
        capabilities,
        request.is_workspace_member,
    )
}

fn capabilities_for_auth(
    auth: &AuthMeResponse,
    snapshot: &AuthorizationCapabilitySnapshot,
) -> PrincipalPresentationCapabilities {
    if snapshot.principal_id != auth.principal.id {
        PrincipalPresentationCapabilities::default()
    } else {
        principal_presentation_capabilities(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION, AuthDeviceSnapshot, AuthGatewaySnapshot,
        AuthPrincipalSnapshot, AuthSessionSnapshot, AuthSessionStatus,
        AuthorizationGlobalCapabilities, AuthorizationRolePresentation,
        AuthorizationWorkspaceCapabilitySnapshot, ClientKind, DeviceId, DeviceStatus, GatewayId,
        PrincipalId, PrincipalKind, RoleKey, ThreadVisibility, TokenFamilyId,
    };

    fn auth(kind: PrincipalKind, role_key: Option<RoleKey>) -> AuthMeResponse {
        let device_id = DeviceId::new("DAAAAAAAAAAAAAAAAAAAA").expect("device id");
        AuthMeResponse {
            gateway: AuthGatewaySnapshot {
                id: GatewayId::new("GAAAAAAAAAAAAAAAAAAAA").expect("gateway id"),
            },
            principal: AuthPrincipalSnapshot {
                id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").expect("principal id"),
                kind,
                display_name: "Alice".to_owned(),
                nickname: "alice".to_owned(),
                avatar_revision: None,
            },
            device: AuthDeviceSnapshot {
                id: device_id.clone(),
                installation_id: "installation".to_owned(),
                display_name: "Phone".to_owned(),
                client_kind: ClientKind::Mobile,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: pioneer_protocol::AuthSessionId::new("SAAAAAAAAAAAAAAAAAAAA")
                    .expect("session id"),
                device_id,
                token_family_id: TokenFamilyId::new("FAAAAAAAAAAAAAAAAAAAA").expect("family id"),
                status: AuthSessionStatus::Active,
                refresh_generation: 1,
                refresh_expires_at_unix: 2,
            },
            role_key,
        }
    }

    fn capability_snapshot(
        auth: &AuthMeResponse,
        elevated: bool,
    ) -> AuthorizationCapabilitySnapshot {
        AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision: 7,
            principal_id: auth.principal.id.clone(),
            role_key: auth
                .role_key
                .as_ref()
                .map_or_else(|| "superuser".to_owned(), ToString::to_string),
            role: AuthorizationRolePresentation {
                key: auth
                    .role_key
                    .as_ref()
                    .map_or_else(|| "superuser".to_owned(), ToString::to_string),
                display_name: if elevated { "Superuser" } else { "Member" }.to_owned(),
                description: "Test role".to_owned(),
                built_in: true,
            },
            global: AuthorizationGlobalCapabilities {
                can_create_workspace: elevated,
                can_manage_gateway_settings: elevated,
                can_manage_capabilities: elevated,
                can_manage_providers: elevated,
                can_manage_mcp: elevated,
                can_manage_skills: elevated,
                can_manage_cli_runtimes: elevated,
                can_manage_all_threads: elevated,
                can_view_invitations: true,
                can_create_invitation: true,
                invitation_role_options: Vec::new(),
                can_view_member_directory: true,
                can_manage_member_lifecycle: elevated,
                can_manage_own_sessions: true,
            },
            workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                workspace_id: "workspace-a".to_owned(),
                operational_resources:
                    pioneer_protocol::AuthorizationOperationalResourceProjection {
                        fingerprint: "fixture-policy".to_owned(),
                        ..Default::default()
                    },
                capabilities: AuthorizationWorkspaceCapabilities {
                    can_read: true,
                    can_create_thread: true,
                    can_manage: elevated,
                    can_read_own_notifications: true,
                    can_acknowledge_own_notifications: true,
                    can_use_providers: true,
                    can_use_cli_runtimes: true,
                    can_use_skills: true,
                    can_use_mcp: true,
                    can_run_tasks: true,
                    can_read_artifacts: true,
                    can_write_artifacts: true,
                    execution_limits: Default::default(),
                    agent_permission_options: Vec::new(),
                    can_list_members: true,
                    can_add_member: true,
                    can_remove_member: elevated,
                    thread_visibility_options: vec![
                        ThreadVisibility::Private,
                        ThreadVisibility::Workspace,
                    ],
                },
                execution_draft_policy:
                    pioneer_protocol::AuthorizationExecutionDraftPolicyProjection {
                        fingerprint: "fixture-policy".to_owned(),
                        resources: pioneer_protocol::AuthorizationOperationalResourceProjection {
                            fingerprint: "fixture-policy".to_owned(),
                            ..Default::default()
                        },
                        permission_options: Vec::new(),
                        can_attach_artifacts: true,
                        mcp_invocation_limits: Default::default(),
                    },
            }),
            thread: None,
        }
    }

    #[test]
    fn bridge_delegates_presentation_policy_to_shared_client() {
        let auth = auth(PrincipalKind::User, Some(RoleKey::member()));
        let snapshot = capability_snapshot(&auth, false);
        assert!(principal_capabilities(snapshot.clone()).can_create_invitation);
        assert!(principal_capabilities(snapshot).can_add_workspace_member);
    }

    #[test]
    fn member_bridge_delegates_action_policy_to_shared_client() {
        let auth = auth(PrincipalKind::Superuser, None);
        let member = MemberSummary {
            principal_id: PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").unwrap(),
            kind: PrincipalKind::User,
            display_name: "Bob".to_owned(),
            nickname: "bob".to_owned(),
            role_key: Some(RoleKey::member()),
            role: pioneer_protocol::AuthorizationRolePresentation {
                key: "member".to_owned(),
                display_name: "Member".to_owned(),
                description: "Workspace collaborator".to_owned(),
                built_in: true,
            },
            lifecycle_managed: true,
            status: pioneer_protocol::PrincipalStatus::Active,
            avatar_revision: None,
        };
        let row = member_presentation(ClientMemberPresentationRequest {
            capability_snapshot: capability_snapshot(&auth, true),
            auth,
            member,
            is_workspace_member: true,
        });
        assert!(row.actions.can_suspend);
        assert!(row.actions.can_remove_from_workspace);
        assert!(!row.actions.can_add_to_workspace);
    }

    #[test]
    fn thread_create_visibility_bridge_uses_shared_fail_closed_plan() {
        let capabilities = AuthorizationWorkspaceCapabilities {
            can_read: true,
            can_create_thread: true,
            can_manage: false,
            can_read_own_notifications: true,
            can_acknowledge_own_notifications: true,
            can_use_providers: true,
            can_use_cli_runtimes: true,
            can_use_skills: true,
            can_use_mcp: true,
            can_run_tasks: true,
            can_read_artifacts: true,
            can_write_artifacts: true,
            execution_limits: Default::default(),
            agent_permission_options: Vec::new(),
            can_list_members: true,
            can_add_member: true,
            can_remove_member: false,
            thread_visibility_options: vec![
                pioneer_protocol::ThreadVisibility::Private,
                pioneer_protocol::ThreadVisibility::Workspace,
            ],
        };
        let member = thread_create_visibility(ClientThreadCreateVisibilityRequest {
            capabilities: capabilities.clone(),
            origin_kind: pioneer_protocol::ThreadOriginKind::Collaborative,
        });
        assert_eq!(
            member.options,
            vec![
                pioneer_protocol::ThreadVisibility::Private,
                pioneer_protocol::ThreadVisibility::Workspace,
            ]
        );

        let superuser = thread_create_visibility(ClientThreadCreateVisibilityRequest {
            capabilities,
            origin_kind: pioneer_protocol::ThreadOriginKind::Collaborative,
        });
        assert_eq!(
            superuser.options,
            vec![
                pioneer_protocol::ThreadVisibility::Private,
                pioneer_protocol::ThreadVisibility::Workspace,
            ]
        );
    }

    #[test]
    fn current_principal_uses_only_the_coherent_server_manifest() {
        let mut auth = auth(PrincipalKind::User, Some(RoleKey::member()));
        auth.principal.avatar_revision = Some("avatar-2".to_owned());
        let result = current_principal(ClientCurrentPrincipalPresentationRequest {
            capability_snapshot: capability_snapshot(&auth, false),
            auth: auth.clone(),
        })
        .expect("matching manifest");
        assert_eq!(result.principal_id, auth.principal.id);
        assert_eq!(result.display_name, "Alice");
        assert_eq!(result.nickname, "alice");
        assert_eq!(result.avatar_revision.as_deref(), Some("avatar-2"));
        assert_eq!(result.role.key, "member");
        assert!(!result.read_only);
        assert!(result.capabilities.can_manage_own_sessions);
    }

    #[test]
    fn current_principal_rejects_a_mismatched_capability_principal() {
        let auth = auth(PrincipalKind::Superuser, None);
        let mut capability_snapshot = capability_snapshot(&auth, true);
        capability_snapshot.principal_id = PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").unwrap();
        let error = current_principal(ClientCurrentPrincipalPresentationRequest {
            capability_snapshot,
            auth,
        })
        .expect_err("mismatched manifest must fail closed");
        assert_eq!(error, "authorization capability principal mismatch");
    }
}
