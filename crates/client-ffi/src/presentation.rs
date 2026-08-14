use pioneer_client::authorization::{
    CurrentPrincipalPresentation, PrincipalPresentationCapabilities, SessionListRowPresentation,
    ThreadPresentationCapabilities, current_principal_presentation,
    principal_presentation_capabilities, session_list_row_presentation,
    thread_presentation_capabilities,
};
use pioneer_protocol::{
    AuthMeResponse, AuthSessionListItem, AuthorizationCapabilitySnapshot,
    AuthorizationThreadCapabilities, AuthorizationWorkspaceCapabilities, InvitationSummary,
    MemberSummary, WorkspaceId,
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
pub struct ClientAuthorizationProjectionAcceptRequest {
    /// Exact native transport epoch that produced `snapshot`. Delayed
    /// responses from a replaced Gateway connection must never advance the
    /// projection store for the new connection.
    pub gateway_id: String,
    pub connection_id: u64,
    pub expected_principal_id: pioneer_protocol::PrincipalId,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub thread_id: Option<String>,
    pub snapshot: AuthorizationCapabilitySnapshot,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientAuthorizationProjectionAcceptResult {
    pub acceptance: pioneer_client::authorization::AuthorizationProjectionAcceptance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<AuthorizationCapabilitySnapshot>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientExecutionDraftReconcileRequest {
    pub draft: pioneer_client::composer::reconciliation::ExecutionDraftSelection,
    pub policy: pioneer_protocol::AuthorizationExecutionDraftPolicyProjection,
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

pub fn accept_authorization_projection(
    store: &mut pioneer_client::authorization::AuthorizationProjectionStore,
    active_gateway_id: Option<&str>,
    active_connection_id: Option<u64>,
    request: ClientAuthorizationProjectionAcceptRequest,
) -> ClientAuthorizationProjectionAcceptResult {
    if request.gateway_id.trim().is_empty()
        || active_gateway_id != Some(request.gateway_id.as_str())
        || active_connection_id != Some(request.connection_id)
    {
        return ClientAuthorizationProjectionAcceptResult {
            acceptance:
                pioneer_client::authorization::AuthorizationProjectionAcceptance::Incompatible,
            snapshot: None,
        };
    }
    if !pioneer_client::authorization::authorization_capability_snapshot_is_compatible(
        &request.snapshot,
        &request.expected_principal_id,
        request.workspace_id.as_deref(),
        request.thread_id.as_deref(),
    ) {
        return ClientAuthorizationProjectionAcceptResult {
            acceptance:
                pioneer_client::authorization::AuthorizationProjectionAcceptance::Incompatible,
            snapshot: None,
        };
    }
    let acceptance = store.accept(request.snapshot);
    let snapshot = (acceptance
        == pioneer_client::authorization::AuthorizationProjectionAcceptance::Accepted)
        .then(|| {
            store
                .snapshot(
                    request.workspace_id.as_deref(),
                    request.thread_id.as_deref(),
                )
                // An absent requested scope is an authoritative negative
                // projection, not a malformed response. Preserve the narrowest
                // confirmed parent scope so Mobile can fail closed immediately
                // instead of retaining an older positive cache entry.
                .or_else(|| store.snapshot(request.workspace_id.as_deref(), None))
                .or_else(|| store.snapshot(None, None))
        })
        .flatten();
    ClientAuthorizationProjectionAcceptResult {
        acceptance,
        snapshot,
    }
}

pub fn reconcile_execution_draft(
    request: ClientExecutionDraftReconcileRequest,
) -> pioneer_client::composer::reconciliation::ExecutionDraftReconciliation {
    pioneer_client::composer::reconciliation::reconcile_execution_draft(
        &request.draft,
        &request.policy,
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
    fn invitation_row_bridge_delegates_status_and_capability_policy() {
        let auth = auth(PrincipalKind::User, Some(RoleKey::member()));
        let row = invitation_list_row(ClientInvitationListRowRequest {
            capability_snapshot: capability_snapshot(&auth, false),
            auth,
            invitation: InvitationSummary {
                invitation_id: pioneer_protocol::InvitationId::new("IAAAAAAAAAAAAAAAAAAAA")
                    .unwrap(),
                role_key: RoleKey::member(),
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

    #[test]
    fn authorization_projection_rejects_a_delayed_previous_connection_epoch() {
        let auth = auth(PrincipalKind::User, Some(RoleKey::member()));
        let mut store = pioneer_client::authorization::AuthorizationProjectionStore::default();
        let result = accept_authorization_projection(
            &mut store,
            Some("gateway-new"),
            Some(12),
            ClientAuthorizationProjectionAcceptRequest {
                gateway_id: "gateway-old".to_owned(),
                connection_id: 11,
                expected_principal_id: auth.principal.id.clone(),
                workspace_id: Some("workspace-a".to_owned()),
                thread_id: None,
                snapshot: capability_snapshot(&auth, false),
            },
        );

        assert_eq!(
            result.acceptance,
            pioneer_client::authorization::AuthorizationProjectionAcceptance::Incompatible
        );
        assert!(result.snapshot.is_none());
        assert_eq!(store.accepted_revision(), None);
    }

    #[test]
    fn authorization_projection_accepts_only_the_exact_active_connection_epoch() {
        let auth = auth(PrincipalKind::User, Some(RoleKey::member()));
        let mut store = pioneer_client::authorization::AuthorizationProjectionStore::default();
        let result = accept_authorization_projection(
            &mut store,
            Some("gateway-a"),
            Some(12),
            ClientAuthorizationProjectionAcceptRequest {
                gateway_id: "gateway-a".to_owned(),
                connection_id: 12,
                expected_principal_id: auth.principal.id.clone(),
                workspace_id: Some("workspace-a".to_owned()),
                thread_id: None,
                snapshot: capability_snapshot(&auth, false),
            },
        );

        assert_eq!(
            result.acceptance,
            pioneer_client::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        assert!(result.snapshot.is_some());
        assert_eq!(store.accepted_revision(), Some(7));
    }

    #[test]
    fn authorization_projection_accepts_a_missing_thread_as_an_authoritative_deny() {
        let auth = auth(PrincipalKind::User, Some(RoleKey::member()));
        let mut store = pioneer_client::authorization::AuthorizationProjectionStore::default();
        let result = accept_authorization_projection(
            &mut store,
            Some("gateway-a"),
            Some(12),
            ClientAuthorizationProjectionAcceptRequest {
                gateway_id: "gateway-a".to_owned(),
                connection_id: 12,
                expected_principal_id: auth.principal.id.clone(),
                workspace_id: Some("workspace-a".to_owned()),
                thread_id: Some("thread-a".to_owned()),
                // The Gateway response is workspace-scoped only. It must not
                // satisfy a thread-scoped Mobile query.
                snapshot: capability_snapshot(&auth, false),
            },
        );

        assert_eq!(
            result.acceptance,
            pioneer_client::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        let snapshot = result.snapshot.expect("negative thread projection");
        assert!(snapshot.workspace.is_some());
        assert!(snapshot.thread.is_none());
        assert_eq!(store.accepted_revision(), Some(7));
    }
}
