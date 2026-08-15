use anyhow::{Context, Result};
#[cfg(test)]
use pioneer_protocol::TurnPermissionMode;
use pioneer_protocol::{
    AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION, AuthorizationCapabilitiesParams,
    AuthorizationCapabilitySnapshot, AuthorizationExecutionResourceLimits,
    AuthorizationGlobalCapabilities, AuthorizationThreadCapabilities,
    AuthorizationThreadCapabilitySnapshot, AuthorizationWorkspaceCapabilities,
    AuthorizationWorkspaceCapabilitySnapshot, ThreadVisibility,
};
use sha2::{Digest, Sha256};

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

fn execution_draft_policy_fingerprint(
    workspace_id: &str,
    resources: &pioneer_protocol::AuthorizationOperationalResourceProjection,
    permission_options: &[pioneer_protocol::AuthorizationAgentPermissionOption],
    can_attach_artifacts: bool,
    mcp_invocation_limits: &pioneer_protocol::McpInvocationResourceLimits,
) -> Result<String> {
    let semantic_policy = serde_json::json!({
        "contract": "pioneer-execution-draft-policy-v1",
        "workspace_id": workspace_id,
        "providers": &resources.providers,
        "provider_models_all": resources.provider_models_all,
        "provider_models": &resources.provider_models,
        "cli_runtimes": &resources.cli_runtimes,
        "cli_models_all": resources.cli_models_all,
        "cli_models": &resources.cli_models,
        "skills": &resources.skills,
        "mcp_servers": &resources.mcp_servers,
        "permission_options": permission_options,
        "can_attach_artifacts": can_attach_artifacts,
        "mcp_invocation_limits": mcp_invocation_limits,
    });
    let encoded = serde_json::to_vec(&semantic_policy)
        .context("failed to encode execution draft policy fingerprint")?;
    Ok(hex::encode(Sha256::digest(encoded)))
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
            Some(workspace_id) => {
                self.workspace_snapshot(principal, workspace_id, authorization_revision)
                    .await?
            }
            None => None,
        };
        let thread = match thread_id {
            Some(thread_id) => {
                self.thread_snapshot(principal, workspace_id, thread_id)
                    .await?
            }
            None => None,
        };

        let role_key = self
            .policy
            .resolved_role_key(principal.kind, principal.role_key.as_ref())
            .context("authorization snapshot principal has no registered role")?;
        let role = self
            .policy
            .role_presentation(principal.kind, principal.role_key.as_ref())
            .context("authorization snapshot role presentation is unavailable")?;

        Ok(AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision,
            principal_id: principal.principal_id.clone(),
            role_key: role_key.to_owned(),
            role,
            global: self.global_capabilities(principal),
            workspace,
            thread,
        })
    }

    fn global_capabilities(
        &self,
        principal: &AuthenticatedSessionPrincipal,
    ) -> AuthorizationGlobalCapabilities {
        let can_manage_providers = self.allows(
            principal,
            ResourceAction::ProviderManage,
            enabled_capability_access(),
        );
        let can_manage_mcp = self.allows(
            principal,
            ResourceAction::McpManage,
            enabled_capability_access(),
        );
        let can_manage_skills = self.allows(
            principal,
            ResourceAction::SkillManage,
            enabled_capability_access(),
        );
        let can_manage_cli_runtimes = self.allows(
            principal,
            ResourceAction::CliRuntimeManage,
            enabled_capability_access(),
        );
        let can_create_invitation = self.allows(
            principal,
            ResourceAction::InvitationCreate,
            ResolvedResourceAccess::InvitationGrantSet {
                all_active_and_authorized: true,
            },
        );
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
            can_manage_capabilities: can_manage_providers
                || can_manage_mcp
                || can_manage_skills
                || can_manage_cli_runtimes,
            can_manage_providers,
            can_manage_mcp,
            can_manage_skills,
            can_manage_cli_runtimes,
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
            can_create_invitation,
            invitation_role_options: if can_create_invitation {
                self.policy.invitation_role_options()
            } else {
                Vec::new()
            },
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
        authorization_revision: u64,
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
        let can_create_private_thread = self.allows(
            principal,
            ResourceAction::ThreadCreatePrivate,
            ResolvedResourceAccess::Workspace(facts),
        );
        let can_create_workspace_thread = self.allows(
            principal,
            ResourceAction::ThreadCreateWorkspace,
            ResolvedResourceAccess::Workspace(facts),
        );
        let can_create_thread = can_create_private_thread || can_create_workspace_thread;
        let can_use_providers = self.allows(
            principal,
            ResourceAction::ProviderUse,
            ResolvedResourceAccess::Workspace(facts),
        );
        let can_use_cli_runtimes = self.allows(
            principal,
            ResourceAction::CliRuntimeUse,
            ResolvedResourceAccess::Workspace(facts),
        );
        let can_use_skills = self.allows(
            principal,
            ResourceAction::SkillUse,
            ResolvedResourceAccess::Workspace(facts),
        );
        let can_use_mcp = self.allows(
            principal,
            ResourceAction::McpUse,
            ResolvedResourceAccess::Workspace(facts),
        );
        let can_write_artifacts = self.allows(
            principal,
            ResourceAction::ArtifactCreateThread,
            ResolvedResourceAccess::Workspace(facts),
        );
        let can_bind_artifacts = self.allows(
            principal,
            ResourceAction::ArtifactBindThread,
            ResolvedResourceAccess::Workspace(facts),
        );
        let operational_resources = self
            .policy
            .operational_projection(
                principal.kind,
                principal.role_key.as_ref(),
                workspace_id,
                authorization_revision,
            )
            .context("authorization role has no operational resource projection")?;
        let agent_permission_options = self
            .policy
            .agent_permission_options(principal.kind, principal.role_key.as_ref());
        let mut draft_resources = operational_resources.clone();
        if !can_use_providers {
            draft_resources.providers = Default::default();
            draft_resources.provider_models_all = false;
            draft_resources.provider_models.clear();
        }
        if !can_use_cli_runtimes {
            draft_resources.cli_runtimes = Default::default();
            draft_resources.cli_models_all = false;
            draft_resources.cli_models.clear();
        }
        if !can_use_skills {
            draft_resources.skills = Default::default();
        }
        if !can_use_mcp {
            draft_resources.mcp_servers = Default::default();
        }
        let can_attach_artifacts = can_write_artifacts && can_bind_artifacts;
        let mcp_invocation_limits = self
            .policy
            .mcp_invocation_resource_limits(principal.kind, principal.role_key.as_ref())
            .context("authorization role has no MCP invocation resource policy")?;
        let draft_policy_fingerprint = execution_draft_policy_fingerprint(
            workspace_id,
            &draft_resources,
            &agent_permission_options,
            can_attach_artifacts,
            &mcp_invocation_limits,
        )?;
        Ok(Some(AuthorizationWorkspaceCapabilitySnapshot {
            workspace_id: workspace_id.to_owned(),
            // The public operational catalog is the exact selectable/useable
            // projection, not the role's raw selector inventory. A future
            // role may carry selectors for a resource class while lacking
            // its corresponding `*Use` action; publishing those selectors
            // would disclose an unusable catalog and make discovery disagree
            // with composite admission.
            operational_resources: draft_resources.clone(),
            execution_draft_policy: pioneer_protocol::AuthorizationExecutionDraftPolicyProjection {
                fingerprint: draft_policy_fingerprint,
                resources: draft_resources,
                permission_options: agent_permission_options.clone(),
                can_attach_artifacts,
                mcp_invocation_limits,
            },
            capabilities: AuthorizationWorkspaceCapabilities {
                can_read,
                can_create_thread,
                can_manage: self.allows(
                    principal,
                    ResourceAction::WorkspaceManage,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_read_own_notifications: self.allows(
                    principal,
                    ResourceAction::NotificationReadOwn,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_acknowledge_own_notifications: self.allows(
                    principal,
                    ResourceAction::NotificationAcknowledgeOwn,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_use_providers,
                can_use_cli_runtimes,
                can_use_skills,
                can_use_mcp,
                can_run_tasks: self.allows(
                    principal,
                    ResourceAction::TaskCreate,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_read_artifacts: self.allows(
                    principal,
                    ResourceAction::ArtifactRead,
                    ResolvedResourceAccess::Workspace(facts),
                ),
                can_write_artifacts,
                execution_limits: self
                    .policy
                    .execution_resource_policy(principal.kind, principal.role_key.as_ref())
                    .map(|policy| AuthorizationExecutionResourceLimits {
                        max_active_executions: policy.active.per_principal,
                        max_queued_tasks: policy.queued.per_principal,
                        max_scheduled_tasks: policy.scheduled.per_principal,
                    })
                    .unwrap_or_default(),
                agent_permission_options,
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
                thread_visibility_options: [
                    (can_create_private_thread, ThreadVisibility::Private),
                    (can_create_workspace_thread, ThreadVisibility::Workspace),
                ]
                .into_iter()
                .filter_map(|(allowed, visibility)| allowed.then_some(visibility))
                .collect(),
            },
        }))
    }

    async fn thread_snapshot(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        expected_workspace_id: Option<&str>,
        thread_id: &str,
    ) -> Result<Option<AuthorizationThreadCapabilitySnapshot>> {
        let Some(capability_facts) = self
            .resolver
            .capability_thread_facts(principal, thread_id, expected_workspace_id)
            .await?
        else {
            return Ok(None);
        };
        let workspace_id = capability_facts.workspace_id.clone();
        let thread_allows =
            |action| self.capability_thread_allows(principal, action, &capability_facts);
        let can_read = thread_allows(ResourceAction::ThreadRead);
        if !can_read {
            return Ok(None);
        }
        let can_read_agents_document = self
            .agents_document_allows(
                principal,
                workspace_id.as_str(),
                thread_id,
                ResourceAction::AgentsDocumentRead,
            )
            .await?;
        let can_manage_agents_document = self
            .agents_document_allows(
                principal,
                workspace_id.as_str(),
                thread_id,
                ResourceAction::AgentsDocumentManage,
            )
            .await?;
        Ok(Some(AuthorizationThreadCapabilitySnapshot {
            workspace_id,
            thread_id: thread_id.to_owned(),
            capabilities: AuthorizationThreadCapabilities {
                can_read,
                can_write: thread_allows(ResourceAction::MessageCreate),
                can_edit_own_message: thread_allows(ResourceAction::MessageEditOwn),
                can_delete_own_message: thread_allows(ResourceAction::MessageDeleteOwn),
                can_start_turn: thread_allows(ResourceAction::AgentTurnStart),
                can_observe_agent_execution: thread_allows(ResourceAction::AgentExecutionObserve),
                can_cancel_agent_execution: thread_allows(ResourceAction::AgentExecutionCancel),
                can_resume_agent_execution: thread_allows(ResourceAction::AgentExecutionResume),
                can_steer_agent_execution: thread_allows(ResourceAction::AgentExecutionSteer),
                can_observe_agent_requests: thread_allows(ResourceAction::AgentRequestObserve),
                can_respond_to_agent_requests: thread_allows(ResourceAction::AgentRequestRespond),
                can_control_cli_runtime: thread_allows(ResourceAction::CliRuntimeControl),
                can_create_task: thread_allows(ResourceAction::TaskCreate),
                can_review_tasks: thread_allows(ResourceAction::TaskReview),
                can_cancel_tasks: thread_allows(ResourceAction::TaskCancel),
                can_read_artifacts: thread_allows(ResourceAction::ArtifactRead),
                can_write_artifacts: thread_allows(ResourceAction::ArtifactCreateThread),
                can_bind_artifacts: thread_allows(ResourceAction::ArtifactBindThread),
                can_read_agents_document,
                can_manage_agents_document,
                can_manage: thread_allows(ResourceAction::ThreadManage),
                can_manage_private_participants: thread_allows(
                    ResourceAction::ThreadParticipantsManage,
                ),
                can_move: thread_allows(ResourceAction::ThreadMove),
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

    fn capability_thread_allows(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action: ResourceAction,
        facts: &super::CapabilityThreadFacts,
    ) -> bool {
        if !self.thread_allows(principal, action, facts.access) {
            return false;
        }
        if !facts.internal_child {
            return true;
        }
        let Some(child_action) = super::execution_child_policy_action(action) else {
            return false;
        };
        let child_gate =
            self.policy
                .authorize_action(principal.kind, principal.role_key.as_ref(), child_action);
        if !child_gate.permits_resource_resolution() && !child_gate.is_final_allow() {
            return false;
        }
        facts
            .parent_execution_actions
            .as_ref()
            .is_some_and(|actions| {
                actions
                    .binary_search_by(|candidate| candidate.as_str().cmp(action.safe_name()))
                    .is_ok()
            })
    }

    async fn agents_document_allows(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        workspace_id: &str,
        thread_id: &str,
        action: ResourceAction,
    ) -> Result<bool> {
        let gate =
            self.policy
                .authorize_action(principal.kind, principal.role_key.as_ref(), action);
        Ok(matches!(
            self.resolver
                .authorize_agents_document_for_thread(
                    principal,
                    &gate,
                    action,
                    workspace_id,
                    thread_id,
                )
                .await?,
            super::ProofResolution::Authorized(_)
        ))
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

const fn enabled_capability_access() -> ResolvedResourceAccess {
    ResolvedResourceAccess::Capability {
        workspace: WorkspaceAccessFacts {
            workspace_active: true,
            workspace_member: true,
        },
        enabled: true,
    }
}

#[cfg(test)]
mod tests {
    use pioneer_crud::CrudStore;
    use pioneer_protocol::{
        AuthSessionId, DeviceId, GatewayId, PrincipalId, PrincipalKind, RoleKey,
    };
    use sea_orm::ConnectionTrait;

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

    fn assert_shell_projection_parity(snapshot: &AuthorizationCapabilitySnapshot) {
        let shared_principal =
            pioneer_client::authorization::principal_presentation_capabilities(snapshot);
        let mobile_principal = pioneer_client_ffi::principal_capabilities(snapshot.clone());
        assert_eq!(shared_principal, mobile_principal);

        let server_thread = snapshot
            .thread
            .as_ref()
            .expect("parity fixture must include a thread")
            .capabilities
            .clone();
        let shared_thread =
            pioneer_client::authorization::thread_presentation_capabilities(Some(&server_thread));
        let mobile_thread = pioneer_client_ffi::thread_capabilities(server_thread.clone());
        assert_eq!(shared_thread, mobile_thread);
        assert_eq!(shared_thread.can_read, server_thread.can_read);
        assert_eq!(shared_thread.can_write, server_thread.can_write);
        assert_eq!(shared_thread.can_start_turn, server_thread.can_start_turn);
        assert_eq!(
            shared_thread.can_observe_agent_execution,
            server_thread.can_observe_agent_execution
        );
        assert_eq!(
            shared_thread.can_cancel_agent_execution,
            server_thread.can_cancel_agent_execution
        );
        assert_eq!(
            shared_thread.can_resume_agent_execution,
            server_thread.can_resume_agent_execution
        );
        assert_eq!(
            shared_thread.can_steer_agent_execution,
            server_thread.can_steer_agent_execution
        );
        assert_eq!(
            shared_thread.can_respond_to_agent_requests,
            server_thread.can_respond_to_agent_requests
        );
        assert_eq!(
            shared_thread.can_control_cli_runtime,
            server_thread.can_control_cli_runtime
        );
        assert_eq!(shared_thread.can_create_task, server_thread.can_create_task);
        assert_eq!(
            shared_thread.can_review_tasks,
            server_thread.can_review_tasks
        );
        assert_eq!(
            shared_thread.can_cancel_tasks,
            server_thread.can_cancel_tasks
        );
        assert_eq!(
            shared_thread.can_read_artifacts,
            server_thread.can_read_artifacts
        );
        assert_eq!(
            shared_thread.can_write_artifacts,
            server_thread.can_write_artifacts
        );
        assert_eq!(
            shared_thread.can_bind_artifacts,
            server_thread.can_bind_artifacts
        );
        assert_eq!(shared_thread.can_manage_thread, server_thread.can_manage);
        assert_eq!(
            shared_thread.can_manage_private_participants,
            server_thread.can_manage_private_participants
        );
        assert_eq!(shared_thread.can_move, server_thread.can_move);
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

        assert_eq!(
            snapshot.schema_version,
            AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION
        );
        assert_eq!(snapshot.authorization_revision, 17);
        assert_eq!(snapshot.role_key, "member");
        assert!(!snapshot.global.can_create_workspace);
        assert!(!snapshot.global.can_manage_gateway_settings);
        assert!(snapshot.global.can_create_invitation);
        assert_eq!(
            snapshot
                .global
                .invitation_role_options
                .iter()
                .filter(|option| option.is_default)
                .count(),
            1
        );
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
            workspace
                .capabilities
                .agent_permission_options
                .iter()
                .map(|option| option.mode)
                .collect::<Vec<_>>(),
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
            .expect("member parity snapshot");
        assert_shell_projection_parity(&snapshot);
    }

    #[tokio::test]
    async fn draft_policy_fingerprint_ignores_unrelated_authorization_generation_changes() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("authorization fixture");
        let service = AuthorizationCapabilitySnapshotService::new(AuthorizationResolver::new(
            CrudStore::new(harness.database.clone()),
        ));
        let params = AuthorizationCapabilitiesParams {
            workspace_id: Some(WORKSPACE_RED_ID.to_owned()),
            thread_id: Some(THREAD_RED_PRIVATE_A_ID.to_owned()),
        };
        let first = service
            .snapshot(
                &principal(MEMBER_A_ID, PrincipalKind::User),
                params.clone(),
                17,
            )
            .await
            .expect("first member snapshot");
        let next = service
            .snapshot(&principal(MEMBER_A_ID, PrincipalKind::User), params, 18)
            .await
            .expect("next member snapshot");
        let first_workspace = first.workspace.expect("first workspace policy");
        let next_workspace = next.workspace.expect("next workspace policy");

        assert_ne!(
            first_workspace.operational_resources.fingerprint,
            next_workspace.operational_resources.fingerprint
        );
        assert_eq!(
            first_workspace.execution_draft_policy.fingerprint,
            next_workspace.execution_draft_policy.fingerprint
        );
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
    async fn snapshot_rejects_an_unregistered_role_instead_of_emitting_unknown_policy() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("authorization fixture");
        let service = AuthorizationCapabilitySnapshotService::new(AuthorizationResolver::new(
            CrudStore::new(harness.database.clone()),
        ));
        let mut unsupported = principal(MEMBER_A_ID, PrincipalKind::User);
        unsupported.role_key = Some(RoleKey::new("unregistered_role").expect("valid role key"));

        let error = service
            .snapshot(&unsupported, AuthorizationCapabilitiesParams::default(), 18)
            .await
            .expect_err("an unsupported role must fail closed");

        assert!(error.to_string().contains("no registered role"));
    }

    #[tokio::test]
    async fn artifact_attachment_projection_intersects_create_and_bind_actions() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("authorization fixture");
        harness
            .database
            .execute_unprepared(&format!(
                "UPDATE gateway_principal SET role_key='synthetic_executor' \
                 WHERE id='{MEMBER_A_ID}'"
            ))
            .await
            .expect("assign synthetic executor");
        let service = AuthorizationCapabilitySnapshotService::new(AuthorizationResolver::new(
            CrudStore::new(harness.database.clone()),
        ));
        let mut executor = principal(MEMBER_A_ID, PrincipalKind::User);
        executor.role_key = Some(RoleKey::new("synthetic_executor").expect("valid role key"));

        let snapshot = service
            .snapshot(
                &executor,
                AuthorizationCapabilitiesParams {
                    workspace_id: Some(WORKSPACE_RED_ID.to_owned()),
                    thread_id: Some(THREAD_RED_PRIVATE_A_ID.to_owned()),
                },
                19,
            )
            .await
            .expect("synthetic executor snapshot");
        let capabilities = &snapshot
            .thread
            .as_ref()
            .expect("thread projection")
            .capabilities;
        assert!(capabilities.can_write_artifacts);
        assert!(!capabilities.can_bind_artifacts);
        assert!(
            snapshot
                .workspace
                .as_ref()
                .expect("workspace projection")
                .capabilities
                .can_write_artifacts
        );
        assert!(
            !snapshot
                .workspace
                .as_ref()
                .expect("workspace projection")
                .execution_draft_policy
                .can_attach_artifacts
        );
        let workspace = snapshot.workspace.as_ref().expect("workspace projection");
        assert!(workspace.capabilities.can_use_providers);
        assert!(!workspace.capabilities.can_use_cli_runtimes);
        assert!(!workspace.capabilities.can_use_skills);
        assert!(!workspace.capabilities.can_use_mcp);
        assert_eq!(
            workspace.operational_resources.providers.ids,
            ["allowed-provider"]
        );
        assert!(workspace.operational_resources.cli_runtimes.ids.is_empty());
        assert!(!workspace.operational_resources.cli_runtimes.all);
        assert!(workspace.operational_resources.cli_models.is_empty());
        assert!(!workspace.operational_resources.cli_models_all);
        assert!(workspace.operational_resources.skills.ids.is_empty());
        assert!(!workspace.operational_resources.skills.all);
        assert!(workspace.operational_resources.mcp_servers.ids.is_empty());
        assert!(!workspace.operational_resources.mcp_servers.all);
        assert_eq!(
            workspace.execution_draft_policy.resources,
            workspace.operational_resources
        );
        let presentation = pioneer_client::artifacts::presentation::artifact_presentation_policy(
            capabilities.can_read_artifacts,
            capabilities.can_write_artifacts && capabilities.can_bind_artifacts,
            true,
        );
        assert!(!presentation.can_attach);
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
            workspace
                .capabilities
                .agent_permission_options
                .iter()
                .map(|option| option.mode)
                .collect::<Vec<_>>(),
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
            .expect("superuser parity snapshot");
        assert_shell_projection_parity(&snapshot);
    }
}
