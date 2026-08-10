use pioneer_client::authorization::{
    CurrentPrincipalPresentation, PrincipalPresentationCapabilities, SessionListRowPresentation,
    current_principal_presentation, principal_presentation_capabilities_from_auth,
    session_list_row_presentation,
};
use pioneer_protocol::{
    AuthMeResponse, AuthSessionListItem, InvitationSummary, MemberSummary, WorkspaceId,
};
use serde::Deserialize;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientMemberPresentationRequest {
    pub auth: AuthMeResponse,
    pub member: MemberSummary,
    pub is_workspace_member: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientInvitationListRowRequest {
    pub auth: AuthMeResponse,
    pub invitation: InvitationSummary,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientCurrentPrincipalPresentationRequest {
    pub auth: AuthMeResponse,
    #[serde(default)]
    pub visible_member: Option<MemberSummary>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadScopePresentationRequest {
    pub auth: AuthMeResponse,
    pub thread: pioneer_protocol::Thread,
    pub current_principal_is_creator: bool,
    pub participants: pioneer_protocol::ThreadParticipantsResponse,
    pub workspace_members: pioneer_protocol::WorkspaceMemberListResponse,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadCreateVisibilityRequest {
    pub auth: AuthMeResponse,
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

pub fn principal_capabilities(auth: AuthMeResponse) -> PrincipalPresentationCapabilities {
    principal_presentation_capabilities_from_auth(&auth)
}

pub fn current_principal(
    request: ClientCurrentPrincipalPresentationRequest,
) -> CurrentPrincipalPresentation {
    current_principal_presentation(&request.auth, request.visible_member.as_ref())
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
        Some(request.auth.principal.kind),
        request.auth.role_key.as_ref(),
        Some(&request.auth.principal.id),
        request.current_principal_is_creator,
        &participants,
        &request.workspace_members.members,
    )
}

pub fn thread_create_visibility(
    request: ClientThreadCreateVisibilityRequest,
) -> pioneer_client::threads::scope::ThreadCreateVisibilityPlan {
    pioneer_client::threads::scope::thread_create_visibility_plan(
        Some(request.auth.principal.kind),
        request.auth.role_key.as_ref(),
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
    let capabilities = principal_presentation_capabilities_from_auth(&request.auth);
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
    let capabilities = principal_presentation_capabilities_from_auth(&request.auth);
    pioneer_client::administration::invitation_list_row(&request.invitation, capabilities)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        AuthDeviceSnapshot, AuthGatewaySnapshot, AuthPrincipalSnapshot, AuthSessionSnapshot,
        AuthSessionStatus, ClientKind, DeviceId, DeviceStatus, GatewayId, PrincipalId,
        PrincipalKind, RoleKey, TokenFamilyId,
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

    #[test]
    fn bridge_delegates_presentation_policy_to_shared_client() {
        let auth = auth(PrincipalKind::User, Some(RoleKey::member()));
        assert!(principal_capabilities(auth.clone()).can_create_invitation);
        assert!(principal_capabilities(auth).can_add_workspace_member);
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
        let member = thread_create_visibility(ClientThreadCreateVisibilityRequest {
            auth: auth(PrincipalKind::User, Some(RoleKey::member())),
            origin_kind: pioneer_protocol::ThreadOriginKind::Collaborative,
        });
        assert_eq!(
            member.options,
            vec![pioneer_protocol::ThreadVisibility::Private]
        );

        let superuser = thread_create_visibility(ClientThreadCreateVisibilityRequest {
            auth: auth(PrincipalKind::Superuser, None),
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
