use anyhow::Result;
#[cfg(test)]
use pioneer_protocol::TurnPermissionMode;
use pioneer_protocol::{
    AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION, AuthorizationCapabilitiesParams,
    AuthorizationCapabilitySnapshot, AuthorizationGlobalCapabilities,
    AuthorizationThreadCapabilities, AuthorizationThreadCapabilitySnapshot,
    AuthorizationWorkspaceCapabilities, AuthorizationWorkspaceCapabilitySnapshot, ThreadVisibility,
};

use crate::auth::AuthenticatedSessionPrincipal;

use super::{
    AuthorizationResolver, AuthorizationService, ResolvedResourceAccess, ResourceAction,
    ThreadAccessFacts, WorkspaceAccessFacts,
};

/// Builds requester-scoped UI capabilities from the same policy service that
/// authorizes RPC operations. A snapshot is never an authorization proof;
/// every operation is still independently admitted by the Gateway.
#[derive(Clone)]
pub(crate) struct AuthorizationCapabilitySnapshotService {
    resolver: AuthorizationResolver,
    policy: AuthorizationService,
}

impl AuthorizationCapabilitySnapshotService {
    pub(crate) fn new(resolver: AuthorizationResolver) -> Self {
        Self {
            resolver,
            policy: AuthorizationService::new(),
        }
    }

    pub(crate) async fn snapshot(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        params: AuthorizationCapabilitiesParams,
        authorization_revision: u64,
    ) -> Result<AuthorizationCapabilitySnapshot> {
        let workspace_id = params
            .workspace_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let thread_id = params
            .thread_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let workspace = match workspace_id {
            Some(workspace_id) => self.workspace_snapshot(principal, workspace_id).await?,
            None => None,
        };
        let thread = match thread_id {
            Some(thread_id) => {
                self.thread_snapshot(principal, workspace_id, thread_id)
                    .await?
            }
            None => None,
        };

        Ok(AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision,
            principal_id: principal.principal_id.clone(),
            role_key: self
                .policy
                .built_in_role_key(principal.kind, principal.role_key.as_ref())
                .unwrap_or("unknown")
                .to_owned(),
            global: self.global_capabilities(principal),
            workspace,
            thread,
        })
    }

    fn global_capabilities(
        &self,
        principal: &AuthenticatedSessionPrincipal,
    ) -> AuthorizationGlobalCapabilities {
        AuthorizationGlobalCapabilities {
            can_create_workspace: self.allows(
                principal,
                ResourceAction::WorkspaceCreate,
                ResolvedResourceAccess::Gateway,
            ),
            can_manage_gateway_settings: self.allows(
                principal,
                ResourceAction::GatewayManage,
                ResolvedResourceAccess::Gateway,
            ),
            can_manage_capabilities: [
                ResourceAction::ProviderManage,
                ResourceAction::McpManage,
                ResourceAction::SkillManage,
                ResourceAction::CliRuntimeManage,
            ]
            .into_iter()
            .all(|action| {
                self.allows(
                    principal,
                    action,
                    ResolvedResourceAccess::Capability {
                        workspace: WorkspaceAccessFacts {
                            workspace_active: true,
                            workspace_member: true,
                        },
                        enabled: true,
                    },
                )
            }),
            can_manage_all_threads: self
                .policy
                .authorize_action(
                    principal.kind,
                    principal.role_key.as_ref(),
                    ResourceAction::ThreadManage,
                )
                .is_final_allow(),
            can_view_invitations: self.allows(
                principal,
                ResourceAction::InvitationList,
                ResolvedResourceAccess::InvitationCollection,
            ),
            can_create_invitation: self.allows(
                principal,
                ResourceAction::InvitationCreate,
                ResolvedResourceAccess::InvitationGrantSet {
                    all_active_and_authorized: true,
                },
            ),
            can_view_member_directory: self.allows(
                principal,
                ResourceAction::MemberDirectoryList,
                ResolvedResourceAccess::MemberDirectory,
            ),
            can_manage_member_lifecycle: [
                ResourceAction::MemberSuspend,
                ResourceAction::MemberRestore,
                ResourceAction::MemberDeviceCreate,
                ResourceAction::MemberRemove,
            ]
            .into_iter()
            .all(|action| self.allows(principal, action, ResolvedResourceAccess::MemberPrincipal)),
            can_manage_own_sessions: [
                ResourceAction::SessionReadOwn,
                ResourceAction::SessionRevokeOwn,
                ResourceAction::ProfileUpdateOwn,
            ]
            .into_iter()
            .all(|action| {
                self.allows(
                    principal,
                    action,
                    ResolvedResourceAccess::Session { owns_session: true },
                )
            }),
        }
    }

    async fn workspace_snapshot(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        workspace_id: &str,
    ) -> Result<Option<AuthorizationWorkspaceCapabilitySnapshot>> {
        let Some(facts) = self
            .resolver
            .capability_workspace_facts(principal, workspace_id)
            .await?
        else {
            return Ok(None);
        };
        let can_read = self.allows(
            principal,
            ResourceAction::WorkspaceRead,
            ResolvedResourceAccess::Workspace(facts),
        );
        if !can_read {
            return Ok(None);
        }
        let can_create_thread = self.allows(
            principal,
            ResourceAction::ThreadCreate,
            ResolvedResourceAccess::Workspace(facts),
        );
        Ok(Some(AuthorizationWorkspaceCapabilitySnapshot {
            workspace_id: workspace_id.to_owned(),
            capabilities: AuthorizationWorkspaceCapabilities {
                can_read,
                can_create_thread,
                can_manage: self.allows(
                    principal,
                    ResourceAction::WorkspaceManage,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_use_providers: self.allows(
                    principal,
                    ResourceAction::ProviderUse,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_use_cli_runtimes: self.allows(
                    principal,
                    ResourceAction::CliRuntimeUse,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_use_skills: self.allows(
                    principal,
                    ResourceAction::SkillUse,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_use_mcp: self.allows(
                    principal,
                    ResourceAction::McpUse,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_run_tasks: self.allows(
                    principal,
                    ResourceAction::TaskRun,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                turn_permission_modes: self
                    .policy
                    .allowed_turn_permission_modes(principal.kind, principal.role_key.as_ref()),
                can_list_members: self.allows(
                    principal,
                    ResourceAction::WorkspaceMemberList,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_add_member: self.allows(
                    principal,
                    ResourceAction::WorkspaceMemberAdd,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_remove_member: self.allows(
                    principal,
                    ResourceAction::WorkspaceMemberRemove,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                thread_visibility_options: if can_create_thread {
                    vec![ThreadVisibility::Private, ThreadVisibility::Workspace]
                } else {
                    Vec::new()
                },
            },
        }))
    }

    async fn thread_snapshot(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        expected_workspace_id: Option<&str>,
        thread_id: &str,
    ) -> Result<Option<AuthorizationThreadCapabilitySnapshot>> {
        let Some((workspace_id, facts)) = self
            .resolver
            .capability_thread_facts(principal, thread_id, expected_workspace_id)
            .await?
        else {
            return Ok(None);
        };
        let can_read = self.thread_allows(principal, ResourceAction::ThreadRead, facts);
        if !can_read {
            return Ok(None);
        }
        Ok(Some(AuthorizationThreadCapabilitySnapshot {
            workspace_id,
            thread_id: thread_id.to_owned(),
            capabilities: AuthorizationThreadCapabilities {
                can_read,
                can_write: self.thread_allows(principal, ResourceAction::ThreadWrite, facts),
                can_start_turn: self.thread_allows(principal, ResourceAction::ThreadWrite, facts),
                can_respond_to_agent_requests: self.thread_allows(
                    principal,
                    ResourceAction::ThreadWrite,
                    facts,
                ),
                can_control_cli_runtime: self.thread_allows(
                    principal,
                    ResourceAction::CliRuntimeUse,
                    facts,
                ),
                can_create_task: self.thread_allows(principal, ResourceAction::TaskRun, facts),
                can_read_artifacts: self.thread_allows(
                    principal,
                    ResourceAction::ArtifactRead,
                    facts,
                ),
                can_write_artifacts: self.thread_allows(
                    principal,
                    ResourceAction::ArtifactWrite,
                    facts,
                ),
                can_manage: self.thread_allows(principal, ResourceAction::ThreadManage, facts),
                can_manage_private_participants: self.thread_allows(
                    principal,
                    ResourceAction::ThreadParticipantsManage,
                    facts,
                ),
                can_move: self.thread_allows(principal, ResourceAction::ThreadMove, facts),
            },
        }))
    }

    fn thread_allows(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action: ResourceAction,
        facts: ThreadAccessFacts,
    ) -> bool {
        self.allows(principal, action, ResolvedResourceAccess::Thread(facts))
    }

    fn allows(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action: ResourceAction,
        access: ResolvedResourceAccess,
    ) -> bool {
        let gate =
            self.policy
                .authorize_action(principal.kind, principal.role_key.as_ref(), action);
        self.policy
            .authorize_resource(&gate, action, access)
            .is_allowed()
    }
}

#[cfg(test)]
mod tests {
    use pioneer_crud::CrudStore;
    use pioneer_protocol::{
        AuthSessionId, DeviceId, GatewayId, PrincipalId, PrincipalKind, RoleKey,
    };

    use super::*;
    use crate::tests::authorization::{
        IsolatedEpic4Harness, MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, THREAD_RED_PRIVATE_B_ID,
        WORKSPACE_RED_ID,
    };

    fn principal(id: &str, kind: PrincipalKind) -> AuthenticatedSessionPrincipal {
        AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new("G00000000000000000001").expect("gateway"),
            principal_id: PrincipalId::new(id).expect("principal"),
            kind,
            role_key: (kind == PrincipalKind::User).then(RoleKey::member),
            device_id: DeviceId::new("D0000000000000000000Z").expect("device"),
            session_id: AuthSessionId::new("S0000000000000000000Z").expect("session"),
            access_jti: "J0000000000000000000Z".to_owned(),
            access_expires_at_unix: u64::MAX,
        }
    }

    #[tokio::test]
    async fn member_snapshot_projects_creator_and_workspace_capabilities() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("authorization fixture");
        let service = AuthorizationCapabilitySnapshotService::new(AuthorizationResolver::new(
            CrudStore::new(harness.database.clone()),
        ));
        let snapshot = service
            .snapshot(
                &principal(MEMBER_A_ID, PrincipalKind::User),
                AuthorizationCapabilitiesParams {
                    workspace_id: Some(WORKSPACE_RED_ID.to_owned()),
                    thread_id: Some(THREAD_RED_PRIVATE_A_ID.to_owned()),
                },
                17,
            )
            .await
            .expect("member snapshot");

        assert_eq!(snapshot.schema_version, 1);
        assert_eq!(snapshot.authorization_revision, 17);
        assert_eq!(snapshot.role_key, "member");
        assert!(!snapshot.global.can_create_workspace);
        assert!(!snapshot.global.can_manage_gateway_settings);
        assert!(snapshot.global.can_create_invitation);
        assert!(snapshot.global.can_manage_own_sessions);
        let workspace = snapshot.workspace.expect("workspace capabilities");
        assert!(workspace.capabilities.can_create_thread);
        assert!(!workspace.capabilities.can_manage);
        assert!(workspace.capabilities.can_use_providers);
        assert!(workspace.capabilities.can_use_cli_runtimes);
        assert!(workspace.capabilities.can_use_skills);
        assert!(workspace.capabilities.can_use_mcp);
        assert!(workspace.capabilities.can_run_tasks);
        assert_eq!(
            workspace.capabilities.turn_permission_modes,
            vec![TurnPermissionMode::Supervised]
        );
        assert_eq!(
            workspace.capabilities.thread_visibility_options,
            vec![ThreadVisibility::Private, ThreadVisibility::Workspace]
        );
        let thread = snapshot.thread.expect("thread capabilities");
        assert!(thread.capabilities.can_read);
        assert!(thread.capabilities.can_write);
        assert!(thread.capabilities.can_start_turn);
        assert!(thread.capabilities.can_respond_to_agent_requests);
        assert!(thread.capabilities.can_control_cli_runtime);
        assert!(thread.capabilities.can_create_task);
        assert!(thread.capabilities.can_read_artifacts);
        assert!(thread.capabilities.can_write_artifacts);
        assert!(thread.capabilities.can_manage);
        assert!(thread.capabilities.can_manage_private_participants);
        assert!(!thread.capabilities.can_move);
    }

    #[tokio::test]
    async fn snapshot_omits_an_inaccessible_private_thread_without_leaking_it() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("authorization fixture");
        let service = AuthorizationCapabilitySnapshotService::new(AuthorizationResolver::new(
            CrudStore::new(harness.database.clone()),
        ));
        let snapshot = service
            .snapshot(
                &principal(MEMBER_A_ID, PrincipalKind::User),
                AuthorizationCapabilitiesParams {
                    workspace_id: Some(WORKSPACE_RED_ID.to_owned()),
                    thread_id: Some(THREAD_RED_PRIVATE_B_ID.to_owned()),
                },
                18,
            )
            .await
            .expect("bounded snapshot");

        assert!(snapshot.workspace.is_some());
        assert!(snapshot.thread.is_none());
    }

    #[tokio::test]
    async fn superuser_snapshot_projects_gateway_wide_management() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("authorization fixture");
        let service = AuthorizationCapabilitySnapshotService::new(AuthorizationResolver::new(
            CrudStore::new(harness.database.clone()),
        ));
        let snapshot = service
            .snapshot(
                &principal("P00000000000000000001", PrincipalKind::Superuser),
                AuthorizationCapabilitiesParams {
                    workspace_id: Some(WORKSPACE_RED_ID.to_owned()),
                    thread_id: Some(THREAD_RED_PRIVATE_B_ID.to_owned()),
                },
                19,
            )
            .await
            .expect("superuser snapshot");

        assert_eq!(snapshot.role_key, "superuser");
        assert!(snapshot.global.can_create_workspace);
        assert!(snapshot.global.can_manage_gateway_settings);
        assert!(snapshot.global.can_manage_capabilities);
        assert!(snapshot.global.can_manage_all_threads);
        assert!(snapshot.global.can_manage_member_lifecycle);
        let workspace = snapshot.workspace.expect("workspace capabilities");
        assert!(workspace.capabilities.can_manage);
        assert!(workspace.capabilities.can_remove_member);
        assert_eq!(
            workspace.capabilities.turn_permission_modes,
            vec![
                TurnPermissionMode::FullAccess,
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionMode::Supervised,
            ]
        );
        let thread = snapshot.thread.expect("thread capabilities");
        assert!(thread.capabilities.can_manage);
        assert!(thread.capabilities.can_manage_private_participants);
        assert!(thread.capabilities.can_move);
    }
}
