use pioneer_client::authorization::{
    CurrentPrincipalPresentation, PrincipalPresentationCapabilities, SessionListRowPresentation,
    current_principal_presentation, principal_presentation_capabilities,
    session_list_row_presentation, thread_presentation_capabilities,
};
#[cfg(test)]
use pioneer_protocol::TurnPermissionMode;
use pioneer_protocol::{
    AuthMeResponse, AuthSessionListItem, AuthorizationCapabilitySnapshot,
    AuthorizationThreadCapabilities, AuthorizationWorkspaceCapabilities, InvitationSummary,
    MemberSummary, WorkspaceId,
};
use serde::Deserialize;

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
pub struct ClientInvitationListRowRequest {
    pub auth: AuthMeResponse,
    pub capability_snapshot: AuthorizationCapabilitySnapshot,
    pub invitation: InvitationSummary,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientCurrentPrincipalPresentationRequest {
    pub auth: AuthMeResponse,
    pub capability_snapshot: AuthorizationCapabilitySnapshot,
    #[serde(default)]
    pub visible_member: Option<MemberSummary>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadScopePresentationRequest {
    pub auth: AuthMeResponse,
    pub thread: pioneer_protocol::Thread,
    pub capabilities: AuthorizationThreadCapabilities,
    pub participants: pioneer_protocol::ThreadParticipantsResponse,
    pub workspace_members: pioneer_protocol::WorkspaceMemberListResponse,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadCreateVisibilityRequest {
    pub capabilities: AuthorizationWorkspaceCapabilities,
    pub origin_kind: pioneer_protocol::ThreadOriginKind,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadScopeMutationPlanRequest {
    pub workspace_id: WorkspaceId,
    pub thread_id: String,
    pub action: pioneer_client::threads::scope::ThreadScopeAction,
}

pub fn principal_capabilities(
    snapshot: AuthorizationCapabilitySnapshot,
) -> PrincipalPresentationCapabilities {
    principal_presentation_capabilities(&snapshot)
}

pub fn current_principal(
    request: ClientCurrentPrincipalPresentationRequest,
) -> CurrentPrincipalPresentation {
    let capabilities = capabilities_for_auth(&request.auth, &request.capability_snapshot);
    current_principal_presentation(&request.auth, request.visible_member.as_ref(), capabilities)
}

pub fn session_list_row(item: AuthSessionListItem) -> SessionListRowPresentation {
    session_list_row_presentation(&item)
}

pub fn thread_scope(
    request: ClientThreadScopePresentationRequest,
) -> pioneer_client::threads::scope::ThreadScopePresentation {
    let participants = if request.participants.participants.is_empty() {
        request
            .participants
            .participant_ids
            .into_iter()
            .map(|principal_id| pioneer_protocol::ThreadParticipantSummary { principal_id })
            .collect::<Vec<_>>()
    } else {
        request.participants.participants
    };
    pioneer_client::threads::scope::thread_scope_presentation(
        &request.thread,
        Some(&request.auth.principal.id),
        thread_presentation_capabilities(Some(&request.capabilities)),
        &participants,
        &request.workspace_members.members,
    )
}

pub fn thread_create_visibility(
    request: ClientThreadCreateVisibilityRequest,
) -> pioneer_client::threads::scope::ThreadCreateVisibilityPlan {
    pioneer_client::threads::scope::thread_create_visibility_plan(
        Some(&request.capabilities),
        request.origin_kind,
    )
}

pub fn thread_scope_mutation_plan(
    request: ClientThreadScopeMutationPlanRequest,
) -> pioneer_client::threads::scope::ThreadScopeMutationPlan {
    pioneer_client::threads::scope::plan_thread_scope_action(
        request.workspace_id,
        request.thread_id,
        request.action,
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

pub fn invitation_list_row(
    request: ClientInvitationListRowRequest,
) -> pioneer_client::administration::InvitationListRow {
    let capabilities = capabilities_for_auth(&request.auth, &request.capability_snapshot);
    pioneer_client::administration::invitation_list_row(&request.invitation, capabilities)
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
        AuthDeviceSnapshot, AuthGatewaySnapshot, AuthPrincipalSnapshot, AuthSessionSnapshot,
        AuthSessionStatus, AuthorizationGlobalCapabilities,
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
            schema_version: 1,
            authorization_revision: 7,
            principal_id: auth.principal.id.clone(),
            role_key: auth
                .role_key
                .as_ref()
                .map_or_else(|| "superuser".to_owned(), ToString::to_string),
            global: AuthorizationGlobalCapabilities {
                can_create_workspace: elevated,
                can_manage_gateway_settings: elevated,
                can_manage_capabilities: elevated,
                can_manage_all_threads: elevated,
                can_view_invitations: true,
                can_create_invitation: true,
                can_view_member_directory: true,
                can_manage_member_lifecycle: elevated,
                can_manage_own_sessions: true,
            },
            workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                workspace_id: "workspace-a".to_owned(),
                capabilities: AuthorizationWorkspaceCapabilities {
                    can_read: true,
                    can_create_thread: true,
                    can_manage: elevated,
                    can_use_providers: true,
                    can_use_cli_runtimes: true,
                    can_use_skills: true,
                    can_use_mcp: true,
                    can_run_tasks: true,
                    turn_permission_modes: vec![
                        TurnPermissionMode::FullAccess,
                        TurnPermissionMode::AutoAcceptEdits,
                        TurnPermissionMode::Supervised,
                    ],
                    can_list_members: true,
                    can_add_member: true,
                    can_remove_member: elevated,
                    thread_visibility_options: vec![
                        ThreadVisibility::Private,
                        ThreadVisibility::Workspace,
                    ],
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
    fn invitation_row_bridge_delegates_status_and_capability_policy() {
        let auth = auth(PrincipalKind::User, Some(RoleKey::member()));
        let row = invitation_list_row(ClientInvitationListRowRequest {
            capability_snapshot: capability_snapshot(&auth, false),
            auth,
            invitation: InvitationSummary {
                invitation_id: pioneer_protocol::InvitationId::new("IAAAAAAAAAAAAAAAAAAAA")
                    .unwrap(),
                status: pioneer_protocol::InvitationStatus::Pending,
                revoke_reason: None,
                inviter: pioneer_protocol::InvitationInviterSummary {
                    principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                    kind: PrincipalKind::User,
                    display_name: "Alice".to_owned(),
                    nickname: "alice".to_owned(),
                },
                workspaces: Vec::new(),
                created_at_unix: 1,
                expires_at_unix: 2,
                terminal_at_unix: None,
            },
        });
        assert_eq!(
            row.status,
            pioneer_client::administration::InvitationPresentationStatus::Pending
        );
        assert!(row.can_revoke);
    }

    #[test]
    fn thread_create_visibility_bridge_uses_shared_fail_closed_plan() {
        let capabilities = AuthorizationWorkspaceCapabilities {
            can_read: true,
            can_create_thread: true,
            can_manage: false,
            can_use_providers: true,
            can_use_cli_runtimes: true,
            can_use_skills: true,
            can_use_mcp: true,
            can_run_tasks: true,
            turn_permission_modes: vec![TurnPermissionMode::Supervised],
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
    fn thread_scope_mutation_bridge_preserves_exact_shared_refetch_plan() {
        let plan = thread_scope_mutation_plan(ClientThreadScopeMutationPlanRequest {
            workspace_id: pioneer_protocol::WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").unwrap(),
            thread_id: "thread-a".to_owned(),
            action: pioneer_client::threads::scope::ThreadScopeAction::AddParticipant {
                principal_id: PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").unwrap(),
            },
        });
        assert_eq!(
            plan.refetch,
            vec![
                pioneer_client::threads::scope::ThreadScopeRefetch::Participants,
                pioneer_client::threads::scope::ThreadScopeRefetch::Thread,
            ]
        );
    }

    #[test]
    fn current_principal_uses_auth_identity_and_matching_visible_avatar_only() {
        let auth = auth(PrincipalKind::User, Some(RoleKey::member()));
        let visible_member = MemberSummary {
            principal_id: auth.principal.id.clone(),
            kind: PrincipalKind::User,
            display_name: "Stale directory name".to_owned(),
            nickname: "stale".to_owned(),
            role_key: Some(RoleKey::member()),
            status: pioneer_protocol::PrincipalStatus::Active,
            avatar_revision: Some("avatar-2".to_owned()),
        };
        let result = current_principal(ClientCurrentPrincipalPresentationRequest {
            capability_snapshot: capability_snapshot(&auth, false),
            auth: auth.clone(),
            visible_member: Some(visible_member),
        });
        assert_eq!(result.principal_id, auth.principal.id);
        assert_eq!(result.display_name, "Alice");
        assert_eq!(result.nickname, "alice");
        assert_eq!(result.avatar_revision.as_deref(), Some("avatar-2"));
        assert!(!result.read_only);
        assert!(result.capabilities.can_manage_own_sessions);
    }

    #[test]
    fn current_principal_ignores_another_visible_member() {
        let auth = auth(PrincipalKind::Superuser, None);
        let visible_member = MemberSummary {
            principal_id: PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").unwrap(),
            kind: PrincipalKind::User,
            display_name: "Bob".to_owned(),
            nickname: "bob".to_owned(),
            role_key: Some(RoleKey::member()),
            status: pioneer_protocol::PrincipalStatus::Active,
            avatar_revision: Some("avatar-b".to_owned()),
        };
        let result = current_principal(ClientCurrentPrincipalPresentationRequest {
            capability_snapshot: capability_snapshot(&auth, true),
            auth,
            visible_member: Some(visible_member),
        });
        assert_eq!(
            result.kind,
            pioneer_client::authorization::CurrentPrincipalKindPresentation::Superuser
        );
        assert_eq!(result.avatar_revision, None);
    }
}
