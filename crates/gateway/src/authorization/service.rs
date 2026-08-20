use std::collections::BTreeSet;

use pioneer_protocol::{
    AuthorizationAgentPermissionOption, AuthorizationInvitationRoleOption,
    AuthorizationPermissionLock, AuthorizationRolePresentation, PrincipalKind, RoleKey,
    ToolPermissionPolicySnapshot, TurnPermissionMode, TurnPermissionProfileCap,
};
use sha2::{Digest, Sha256};

use super::{
    ActionGateDecision, AllowReason, AuthorizationDecision, DenyReason, DisclosurePolicy,
    ObservationResourcePolicy, ResourceAction, RoleDefinitionRegistry, RoleDisclosurePolicy,
    RoleResourcePolicy, RuntimePrincipalPolicy, ThreadAccessClass,
};
use crate::human_interaction::HumanInteractionBudget;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceAccessFacts {
    pub(crate) workspace_active: bool,
    pub(crate) workspace_member: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ThreadAccessFacts {
    pub(crate) workspace: WorkspaceAccessFacts,
    pub(crate) access_class: ThreadAccessClass,
    pub(crate) resource_class: ThreadResourceClass,
    pub(crate) thread_member: bool,
    pub(crate) thread_creator: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThreadResourceClass {
    Root,
    InternalChild,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CliThreadForkExportProjection {
    /// Provider-native state clone. It is only safe when the collaboration
    /// audience is unchanged.
    OpaqueNativeState,
    /// A future server-built allow-listed projection may cross audiences once
    /// its source and destination grants are independently proven.
    #[allow(dead_code)]
    RedactedTimeline,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CliThreadForkExportFacts<'a> {
    pub(crate) source_access_class: ThreadAccessClass,
    pub(crate) source_principals: &'a BTreeSet<pioneer_protocol::PrincipalId>,
    pub(crate) destination_access_class: ThreadAccessClass,
    pub(crate) destination_principals: &'a BTreeSet<pioneer_protocol::PrincipalId>,
    pub(crate) projection: CliThreadForkExportProjection,
}

/// Typed operational selector shared by list projection and exact admission.
/// Parent/child identity (provider/model and runtime/model) cannot be flattened
/// into an ambiguous string at the policy boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OperationalResourceRef<'a> {
    Provider(&'a str),
    ProviderModel { provider: &'a str, model: &'a str },
    CliRuntime(&'a str),
    CliModel { runtime_id: &'a str, model: &'a str },
    Skill(&'a str),
    McpServer(&'a str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedResourceAccess {
    Gateway,
    WorkspaceCollection,
    Workspace(WorkspaceAccessFacts),
    Thread(ThreadAccessFacts),
    Turn(ThreadAccessFacts),
    Artifact {
        workspace: WorkspaceAccessFacts,
        thread: Option<ThreadAccessFacts>,
    },
    Task {
        workspace: WorkspaceAccessFacts,
        root_thread: Option<ThreadAccessFacts>,
        initiating_principal: bool,
    },
    Session {
        owns_session: bool,
    },
    Capability {
        workspace: WorkspaceAccessFacts,
        enabled: bool,
    },
    AgentsDocument {
        workspace: WorkspaceAccessFacts,
        scope_exists: bool,
    },
    InvitationGrantSet {
        all_active_and_authorized: bool,
    },
    InvitationCollection,
    Invitation {
        actor_created: bool,
    },
    MemberDirectory,
    DirectoryPrincipal {
        visible: bool,
    },
    MemberPrincipal,
}

/// The single role-to-policy boundary for normal Gateway actions.
///
/// This service performs only the first authorization level. A scoped-role result
/// of `RequireResource` must be followed by an exact server-owned resource
/// lookup and resource decision before any handler side effect. Superuser is
/// the only principal that receives an action-level final allow; resource
/// resolvers must still reject malformed or inconsistent resource identity.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AuthorizationService {
    roles: RoleDefinitionRegistry,
}

impl AuthorizationService {
    pub(crate) const fn new() -> Self {
        Self {
            roles: RoleDefinitionRegistry::new(),
        }
    }

    /// Authorize an agent action through the same server-owned role registry
    /// used by the rest of Gateway. Agent envelopes still carry the narrowed
    /// parent intersection; this check prevents a forged role/action pair from
    /// bypassing the registry before that envelope is evaluated.
    pub(crate) fn agent_action_allowed(&self, role_key: &str, action: ResourceAction) -> bool {
        self.roles.agent_policy_allows(role_key, action)
    }

    pub(crate) fn authorize_action(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        action: ResourceAction,
    ) -> ActionGateDecision {
        let Some(role) = self.roles.resolve(principal_kind, role_key) else {
            return deny_unsupported_role();
        };
        if !role.actions.allows(action) {
            return ActionGateDecision::Deny {
                reason: DenyReason::ManagementDenied,
                disclosure: DisclosurePolicy::Forbidden,
            };
        }
        match role.resources {
            RoleResourcePolicy::Absolute => ActionGateDecision::AllowAbsolute,
            RoleResourcePolicy::ScopedCollaboration => ActionGateDecision::RequireResource {
                role: RoleKey::new(role.key).expect("registered user role key must be valid"),
            },
        }
    }

    pub(crate) fn cli_thread_fork_export_allowed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        facts: CliThreadForkExportFacts<'_>,
    ) -> bool {
        let Some(role) = self.roles.resolve(principal_kind, role_key) else {
            return false;
        };
        if !role.actions.allows(ResourceAction::CliThreadFork) {
            return false;
        }
        match role.resources {
            RoleResourcePolicy::Absolute => true,
            RoleResourcePolicy::ScopedCollaboration => match facts.projection {
                CliThreadForkExportProjection::OpaqueNativeState => {
                    facts.source_access_class == ThreadAccessClass::Private
                        && facts.destination_access_class == ThreadAccessClass::Private
                        && !facts.source_principals.is_empty()
                        && facts.source_principals == facts.destination_principals
                }
                CliThreadForkExportProjection::RedactedTimeline => false,
            },
        }
    }

    /// Stable identifier for a role implemented by this Gateway binary.
    /// This is metadata only; clients must use capability bits, not this key,
    /// for presentation or authorization decisions.
    pub(crate) fn resolved_role_key(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<&'static str> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| definition.key)
    }

    pub(crate) fn role_presentation(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<AuthorizationRolePresentation> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| AuthorizationRolePresentation {
                key: definition.key.to_owned(),
                display_name: definition.presentation.display_name.to_owned(),
                description: definition.presentation.description.to_owned(),
                built_in: definition.presentation.built_in,
            })
    }

    pub(crate) fn execution_resource_policy(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<pioneer_crud::ExecutionAdmissionQuotaPolicy> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| definition.execution_resources)
    }

    pub(crate) fn observation_resource_policy(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<ObservationResourcePolicy> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| definition.observation_resources)
    }

    pub(crate) fn task_resource_budget(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<pioneer_protocol::TaskResourceBudget> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| definition.task_resources)
    }

    pub(crate) fn mcp_invocation_resource_limits(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<pioneer_protocol::McpInvocationResourceLimits> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| definition.mcp_invocation_resources)
    }

    pub(crate) fn native_event_resource_budget(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<pioneer_cli_agent_runtime::NativeEventBudget> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| definition.native_event_resources)
    }

    /// Maximum agent permission profile implemented by this code-defined
    /// role. It is shared by capability projection and execution admission so
    /// the UI can never advertise a mode the runtime would silently widen or
    /// reject.
    pub(crate) fn turn_permission_profile_cap(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<TurnPermissionProfileCap> {
        let definition = self.roles.resolve(principal_kind, role_key)?;
        Some((definition.permission_cap)())
    }

    pub(crate) fn human_interaction_budget(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<HumanInteractionBudget> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| definition.human_interaction_budget)
    }

    /// Role-owned maximum for consent scope. It is intentionally independent
    /// from the role's own execution permission profile: an approver may not
    /// be a runner, and a full-access runner may still approve another
    /// collaborator's supervised execution.
    pub(crate) fn approval_scope_cap(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<pioneer_protocol::TurnApprovalScopePolicySnapshot> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| (definition.approval_scope_cap)())
    }

    pub(crate) fn runtime_principal_policy(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<RuntimePrincipalPolicy> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| definition.runtime_principal)
    }

    pub(crate) fn role_is_lifecycle_managed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> bool {
        self.roles
            .resolve(principal_kind, role_key)
            .is_some_and(|definition| definition.lifecycle_managed)
    }

    pub(crate) fn role_disclosure_policy(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<RoleDisclosurePolicy> {
        self.roles
            .resolve(principal_kind, role_key)
            .map(|definition| definition.disclosure)
    }

    pub(crate) fn role_is_invitation_assignable(&self, role_key: &RoleKey) -> bool {
        self.roles
            .resolve_user_role(role_key)
            .is_some_and(|definition| definition.invitation_assignable)
    }

    pub(crate) fn invitation_role_options(&self) -> Vec<AuthorizationInvitationRoleOption> {
        self.roles.invitation_role_options()
    }

    pub(crate) fn provider_allowed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        provider: &str,
    ) -> bool {
        self.operational_resource_allowed(
            principal_kind,
            role_key,
            OperationalResourceRef::Provider(provider),
        )
    }

    pub(crate) fn provider_model_allowed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        provider: &str,
        model: &str,
    ) -> bool {
        self.operational_resource_allowed(
            principal_kind,
            role_key,
            OperationalResourceRef::ProviderModel { provider, model },
        )
    }

    pub(crate) fn cli_runtime_allowed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        runtime_id: &str,
    ) -> bool {
        self.operational_resource_allowed(
            principal_kind,
            role_key,
            OperationalResourceRef::CliRuntime(runtime_id),
        )
    }

    pub(crate) fn cli_management_details_allowed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        runtime_id: &str,
    ) -> bool {
        matches!(
            self.authorize_action(principal_kind, role_key, ResourceAction::CliRuntimeManage,),
            ActionGateDecision::AllowAbsolute
        ) && self.cli_runtime_allowed(principal_kind, role_key, runtime_id)
    }

    pub(crate) fn cli_model_allowed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        runtime_id: &str,
        model: &str,
    ) -> bool {
        self.operational_resource_allowed(
            principal_kind,
            role_key,
            OperationalResourceRef::CliModel { runtime_id, model },
        )
    }

    pub(crate) fn skill_allowed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        skill_id: &str,
    ) -> bool {
        self.operational_resource_allowed(
            principal_kind,
            role_key,
            OperationalResourceRef::Skill(skill_id),
        )
    }

    pub(crate) fn mcp_server_allowed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        server_id: &str,
    ) -> bool {
        self.operational_resource_allowed(
            principal_kind,
            role_key,
            OperationalResourceRef::McpServer(server_id),
        )
    }

    pub(crate) fn operational_resource_allowed(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        resource: OperationalResourceRef<'_>,
    ) -> bool {
        let Some(role) = self.roles.resolve(principal_kind, role_key) else {
            return false;
        };
        match resource {
            OperationalResourceRef::Provider(provider) => {
                role.operational_resources.provider_allowed(provider)
            }
            OperationalResourceRef::ProviderModel { provider, model } => role
                .operational_resources
                .provider_model_allowed(provider, model),
            OperationalResourceRef::CliRuntime(runtime_id) => {
                role.operational_resources.cli_runtime_allowed(runtime_id)
            }
            OperationalResourceRef::CliModel { runtime_id, model } => role
                .operational_resources
                .cli_model_allowed(runtime_id, model),
            OperationalResourceRef::Skill(skill_id) => {
                role.operational_resources.skill_allowed(skill_id)
            }
            OperationalResourceRef::McpServer(server_id) => {
                role.operational_resources.mcp_server_allowed(server_id)
            }
        }
    }

    pub(crate) fn operational_projection(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        workspace_id: &str,
        authorization_revision: u64,
    ) -> Option<pioneer_protocol::AuthorizationOperationalResourceProjection> {
        let role = self.roles.resolve(principal_kind, role_key)?;
        let policy = role.operational_resources;
        let selector = |ids: Option<&'static [&'static str]>| {
            pioneer_protocol::AuthorizationResourceSelector {
                all: ids.is_none(),
                ids: ids
                    .unwrap_or_default()
                    .iter()
                    .map(|id| (*id).to_owned())
                    .collect(),
            }
        };
        let mut digest = Sha256::new();
        digest.update(b"pioneer-operational-projection-v1");
        digest.update(role.key.as_bytes());
        digest.update(workspace_id.as_bytes());
        digest.update(authorization_revision.to_le_bytes());
        digest.update(
            RoleDefinitionRegistry::new()
                .policy_fingerprint()
                .as_bytes(),
        );
        Some(
            pioneer_protocol::AuthorizationOperationalResourceProjection {
                fingerprint: hex::encode(digest.finalize()),
                providers: selector(policy.providers),
                provider_models_all: policy.provider_models.is_none(),
                provider_models: policy
                    .provider_models
                    .unwrap_or_default()
                    .iter()
                    .map(
                        |(provider, model)| pioneer_protocol::AuthorizationProviderModelGrant {
                            provider: (*provider).to_owned(),
                            model: (*model).to_owned(),
                        },
                    )
                    .collect(),
                cli_runtimes: selector(policy.cli_runtimes),
                cli_models_all: policy.cli_models.is_none(),
                cli_models: policy
                    .cli_models
                    .unwrap_or_default()
                    .iter()
                    .map(
                        |(runtime_id, model)| pioneer_protocol::AuthorizationCliModelGrant {
                            runtime_id: (*runtime_id).to_owned(),
                            model: (*model).to_owned(),
                        },
                    )
                    .collect(),
                skills: selector(policy.skills),
                mcp_servers: selector(policy.mcp_servers),
            },
        )
    }

    pub(crate) fn agent_permission_options(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Vec<AuthorizationAgentPermissionOption> {
        let Some(role) = self.roles.resolve(principal_kind, role_key) else {
            return Vec::new();
        };
        let role_cap = pioneer_protocol::task_permission_cap_snapshot(&(role.permission_cap)());
        role.permission_presets
            .iter()
            .copied()
            .map(|mode| {
                let requested = pioneer_protocol::compile_turn_permission_profile(
                    mode,
                    pioneer_protocol::TurnPermissionProfileSource::Composer,
                );
                let effective = pioneer_protocol::intersect_turn_permission_profiles(
                    &requested,
                    &role_cap,
                    pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
                );
                AuthorizationAgentPermissionOption {
                    id: mode.as_str().to_owned(),
                    label: match mode {
                        TurnPermissionMode::FullAccess => "Full access",
                        TurnPermissionMode::AutoAcceptEdits => "Auto-accept edits",
                        TurnPermissionMode::Supervised => "Supervised",
                    }
                    .to_owned(),
                    description: match mode {
                        TurnPermissionMode::FullAccess => {
                            "Allow commands and edits within the role policy ceiling."
                        }
                        TurnPermissionMode::AutoAcceptEdits => {
                            "Auto-approve permitted edits and ask before other permitted actions."
                        }
                        TurnPermissionMode::Supervised => {
                            "Ask before commands and file changes allowed by the role policy."
                        }
                    }
                    .to_owned(),
                    mode,
                    locked: permission_policy_locks(
                        &requested.effective_policy,
                        &effective.effective_policy,
                    ),
                    effective_policy: effective.effective_policy,
                }
            })
            .collect()
    }

    /// Completes authorization after a server-owned exact resource lookup.
    ///
    /// Callers must not synthesize `ResolvedResourceAccess` from client
    /// fields. The resolver layer is the only production constructor.
    pub(crate) fn authorize_resource(
        &self,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        access: ResolvedResourceAccess,
    ) -> AuthorizationDecision {
        match action_gate {
            ActionGateDecision::AllowAbsolute => AuthorizationDecision::AllowAbsolute,
            ActionGateDecision::Deny { reason, disclosure } => AuthorizationDecision::Deny {
                reason: *reason,
                disclosure: *disclosure,
            },
            ActionGateDecision::RequireResource { role } => {
                match self
                    .roles
                    .resolve_user_role(role)
                    .map(|role| role.resources)
                {
                    Some(RoleResourcePolicy::ScopedCollaboration) => {
                        authorize_scoped_resource(role, action, access)
                    }
                    Some(RoleResourcePolicy::Absolute) | None => AuthorizationDecision::Deny {
                        reason: DenyReason::UnsupportedRole,
                        disclosure: DisclosurePolicy::AuthenticationTerminal,
                    },
                }
            }
        }
    }
}

fn permission_policy_locks(
    requested: &ToolPermissionPolicySnapshot,
    effective: &ToolPermissionPolicySnapshot,
) -> Vec<AuthorizationPermissionLock> {
    let mut locked = Vec::new();
    for (field, requested, effective) in [
        (
            "default_behavior",
            requested.default_behavior,
            effective.default_behavior,
        ),
        ("file_read", requested.file_read, effective.file_read),
        ("file_write", requested.file_write, effective.file_write),
        (
            "shell_command",
            requested.shell_command,
            effective.shell_command,
        ),
        ("network", requested.network, effective.network),
        ("mcp_read", requested.mcp_read, effective.mcp_read),
        (
            "mcp_write_or_unknown",
            requested.mcp_write_or_unknown,
            effective.mcp_write_or_unknown,
        ),
        (
            "dynamic_skill_tool",
            requested.dynamic_skill_tool,
            effective.dynamic_skill_tool,
        ),
        (
            "computer_use",
            requested.computer_use,
            effective.computer_use,
        ),
        (
            "task_subagent",
            requested.task_subagent,
            effective.task_subagent,
        ),
    ] {
        if requested != effective {
            locked.push(AuthorizationPermissionLock {
                field: field.to_owned(),
                reason: "role_policy_maximum".to_owned(),
            });
        }
    }
    for (field, changed) in [
        (
            "allowed_tools",
            requested.allowed_tools != effective.allowed_tools,
        ),
        (
            "denied_tools",
            requested.denied_tools != effective.denied_tools,
        ),
        (
            "allowed_paths",
            requested.allowed_paths != effective.allowed_paths,
        ),
    ] {
        if changed {
            locked.push(AuthorizationPermissionLock {
                field: field.to_owned(),
                reason: "role_policy_maximum".to_owned(),
            });
        }
    }
    locked
}

fn deny_unsupported_role() -> ActionGateDecision {
    ActionGateDecision::Deny {
        reason: DenyReason::UnsupportedRole,
        disclosure: DisclosurePolicy::AuthenticationTerminal,
    }
}

fn authorize_scoped_resource(
    role: &RoleKey,
    action: ResourceAction,
    access: ResolvedResourceAccess,
) -> AuthorizationDecision {
    if !resource_supports_action(access, action) {
        return deny_not_found(DenyReason::ResourceScopeMismatch);
    }
    match access {
        ResolvedResourceAccess::Gateway => deny_forbidden(DenyReason::ManagementDenied),
        ResolvedResourceAccess::WorkspaceCollection => allow(role, AllowReason::ScopedCollection),
        ResolvedResourceAccess::Workspace(workspace) => {
            authorize_workspace(role, workspace, AllowReason::ActiveWorkspaceMember)
        }
        ResolvedResourceAccess::Thread(thread) | ResolvedResourceAccess::Turn(thread) => {
            authorize_thread(role, action, thread)
        }
        ResolvedResourceAccess::Artifact { workspace, thread } => match thread {
            Some(thread) => authorize_thread(role, action, thread),
            None => {
                let workspace_decision =
                    authorize_workspace(role, workspace, AllowReason::ActiveWorkspaceMember);
                if workspace_decision.is_allowed() {
                    deny_not_found(DenyReason::MissingAuthoritativeResource)
                } else {
                    workspace_decision
                }
            }
        },
        ResolvedResourceAccess::Task {
            workspace,
            root_thread,
            initiating_principal: _,
        } => {
            let inherited = match root_thread {
                Some(thread) => authorize_thread(role, action, thread),
                None => {
                    let workspace_decision =
                        authorize_workspace(role, workspace, AllowReason::ActiveWorkspaceMember);
                    if workspace_decision.is_allowed() {
                        deny_not_found(DenyReason::MissingAuthoritativeResource)
                    } else {
                        workspace_decision
                    }
                }
            };
            if !inherited.is_allowed() {
                return inherited;
            }
            inherited
        }
        ResolvedResourceAccess::Session { owns_session } => {
            if owns_session {
                allow(role, AllowReason::OwnSession)
            } else {
                deny_not_found(DenyReason::ResourceScopeMismatch)
            }
        }
        ResolvedResourceAccess::Capability { workspace, enabled } => {
            let workspace_decision =
                authorize_workspace(role, workspace, AllowReason::CapabilityProjected);
            if !workspace_decision.is_allowed() {
                workspace_decision
            } else if enabled {
                allow(role, AllowReason::CapabilityProjected)
            } else {
                deny_forbidden(DenyReason::CapabilityDisabled)
            }
        }
        ResolvedResourceAccess::AgentsDocument {
            workspace,
            scope_exists,
        } => {
            let workspace_decision =
                authorize_workspace(role, workspace, AllowReason::ActiveWorkspaceMember);
            if !workspace_decision.is_allowed() {
                workspace_decision
            } else if scope_exists {
                workspace_decision
            } else {
                deny_not_found(DenyReason::MissingAuthoritativeResource)
            }
        }
        ResolvedResourceAccess::InvitationGrantSet {
            all_active_and_authorized,
        } => {
            if all_active_and_authorized {
                allow(role, AllowReason::InvitationGrantSet)
            } else {
                deny_not_found(DenyReason::NoWorkspaceMembership)
            }
        }
        ResolvedResourceAccess::InvitationCollection => allow(role, AllowReason::ScopedCollection),
        ResolvedResourceAccess::Invitation { actor_created } => {
            if actor_created {
                allow(role, AllowReason::InvitationCreator)
            } else {
                deny_not_found(DenyReason::ManagementDenied)
            }
        }
        ResolvedResourceAccess::MemberDirectory => allow(role, AllowReason::ScopedCollection),
        ResolvedResourceAccess::DirectoryPrincipal { visible } => {
            if visible {
                allow(role, AllowReason::DirectoryVisible)
            } else {
                deny_not_found(DenyReason::MissingAuthoritativeResource)
            }
        }
        ResolvedResourceAccess::MemberPrincipal => deny_forbidden(DenyReason::ManagementDenied),
    }
}

fn authorize_workspace(
    role: &RoleKey,
    workspace: WorkspaceAccessFacts,
    reason: AllowReason,
) -> AuthorizationDecision {
    if !workspace.workspace_active {
        deny_not_found(DenyReason::MissingAuthoritativeResource)
    } else if !workspace.workspace_member {
        deny_not_found(DenyReason::NoWorkspaceMembership)
    } else {
        allow(role, reason)
    }
}

fn authorize_thread(
    role: &RoleKey,
    action: ResourceAction,
    thread: ThreadAccessFacts,
) -> AuthorizationDecision {
    let workspace = authorize_workspace(role, thread.workspace, AllowReason::ActiveWorkspaceMember);
    if !workspace.is_allowed() {
        return workspace;
    }
    if thread.access_class == ThreadAccessClass::Internal {
        return deny_not_found(DenyReason::MissingAuthoritativeResource);
    }
    if thread.resource_class == ThreadResourceClass::InternalChild
        && matches!(
            action,
            ResourceAction::ThreadManage
                | ResourceAction::ThreadMove
                | ResourceAction::ThreadParticipantsManage
                | ResourceAction::AgentsDocumentRead
                | ResourceAction::AgentsDocumentManage
                | ResourceAction::MessageEditOwn
                | ResourceAction::MessageDeleteOwn
        )
    {
        return deny_not_found(DenyReason::ResourceScopeMismatch);
    }
    if thread.access_class == ThreadAccessClass::Private && !thread.thread_member {
        return deny_not_found(DenyReason::NoPrivateThreadMembership);
    }
    if action == ResourceAction::ThreadParticipantsManage
        && (thread.access_class != ThreadAccessClass::Private || !thread.thread_creator)
    {
        return deny_forbidden(DenyReason::ManagementDenied);
    }
    if action == ResourceAction::ThreadManage && !thread.thread_creator {
        return deny_forbidden(DenyReason::NotThreadCreator);
    }
    let reason = if thread.thread_creator
        && matches!(
            action,
            ResourceAction::ThreadManage | ResourceAction::ThreadParticipantsManage
        ) {
        AllowReason::ThreadCreator
    } else if thread.access_class == ThreadAccessClass::Private {
        AllowReason::PrivateThreadParticipant
    } else {
        AllowReason::WorkspaceThreadMember
    };
    allow(role, reason)
}

const fn resource_supports_action(access: ResolvedResourceAccess, action: ResourceAction) -> bool {
    match access {
        ResolvedResourceAccess::Gateway => matches!(
            action,
            ResourceAction::GatewayManage | ResourceAction::WorkspaceCreate
        ),
        ResolvedResourceAccess::WorkspaceCollection => {
            matches!(
                action,
                ResourceAction::WorkspaceList | ResourceAction::WorkspaceRead
            )
        }
        ResolvedResourceAccess::Workspace(_) => matches!(
            action,
            ResourceAction::WorkspaceList
                | ResourceAction::WorkspaceRead
                | ResourceAction::WorkspaceManage
                | ResourceAction::ThreadCreatePrivate
                | ResourceAction::ThreadCreateWorkspace
                | ResourceAction::ArtifactRead
                | ResourceAction::ArtifactCreateThread
                | ResourceAction::ArtifactBindThread
                | ResourceAction::MemoryRead
                | ResourceAction::MemoryCreateThread
                | ResourceAction::MemoryUpdateThread
                | ResourceAction::MemoryForgetThread
                | ResourceAction::MemoryForgetWorkspace
                | ResourceAction::MemoryModerateWorkspace
                | ResourceAction::TaskRead
                | ResourceAction::TaskCreate
                | ResourceAction::ProviderDiscover
                | ResourceAction::ProviderUse
                | ResourceAction::McpDiscover
                | ResourceAction::McpUse
                | ResourceAction::SkillDiscover
                | ResourceAction::SkillUse
                | ResourceAction::CliRuntimeDiscover
                | ResourceAction::CliRuntimeUse
                | ResourceAction::CliRuntimeControl
                | ResourceAction::CliThreadFork
                | ResourceAction::WorkspaceMemberList
                | ResourceAction::WorkspaceMemberAdd
                | ResourceAction::WorkspaceMemberRemove
                | ResourceAction::NotificationReadOwn
                | ResourceAction::NotificationAcknowledgeOwn
        ),
        ResolvedResourceAccess::Thread(_) => matches!(
            action,
            ResourceAction::ThreadRead
                | ResourceAction::MessageCreate
                | ResourceAction::MessageEditOwn
                | ResourceAction::MessageDeleteOwn
                | ResourceAction::AgentTurnStart
                | ResourceAction::AgentExecutionObserve
                | ResourceAction::AgentExecutionCancel
                | ResourceAction::AgentExecutionResume
                | ResourceAction::AgentExecutionSteer
                | ResourceAction::AgentRequestObserve
                | ResourceAction::AgentRequestRespond
                | ResourceAction::ThreadManage
                | ResourceAction::ThreadMove
                | ResourceAction::ThreadParticipantsManage
                | ResourceAction::ArtifactRead
                | ResourceAction::ArtifactCreateThread
                | ResourceAction::ArtifactBindThread
                | ResourceAction::MemoryRead
                | ResourceAction::MemoryCreateThread
                | ResourceAction::MemoryUpdateThread
                | ResourceAction::MemoryForgetThread
                | ResourceAction::TaskCreate
                | ResourceAction::TaskReview
                | ResourceAction::TaskCancel
                | ResourceAction::ProviderUse
                | ResourceAction::McpUse
                | ResourceAction::SkillUse
                | ResourceAction::CliRuntimeDiscover
                | ResourceAction::CliRuntimeUse
                | ResourceAction::CliRuntimeControl
                | ResourceAction::CliThreadFork
        ),
        ResolvedResourceAccess::Turn(_) => matches!(
            action,
            ResourceAction::ThreadRead
                | ResourceAction::MessageEditOwn
                | ResourceAction::MessageDeleteOwn
                | ResourceAction::AgentExecutionObserve
                | ResourceAction::AgentExecutionCancel
                | ResourceAction::AgentExecutionResume
                | ResourceAction::AgentExecutionSteer
                | ResourceAction::AgentRequestObserve
                | ResourceAction::AgentRequestRespond
                | ResourceAction::ArtifactRead
                | ResourceAction::ArtifactCreateThread
                | ResourceAction::ArtifactBindThread
                | ResourceAction::MemoryRead
                | ResourceAction::MemoryCreateThread
                | ResourceAction::MemoryUpdateThread
                | ResourceAction::MemoryForgetThread
                | ResourceAction::TaskCreate
                | ResourceAction::ProviderUse
                | ResourceAction::McpUse
                | ResourceAction::SkillUse
                | ResourceAction::CliRuntimeUse
                | ResourceAction::CliRuntimeReadOperator
                | ResourceAction::CliThreadFork
        ),
        ResolvedResourceAccess::Artifact { .. } => matches!(
            action,
            ResourceAction::ArtifactRead
                | ResourceAction::ArtifactCreateThread
                | ResourceAction::ArtifactBindThread
                | ResourceAction::ArtifactDeleteThread
                | ResourceAction::ArtifactDeleteOwn
                | ResourceAction::ArtifactManageWorkspace
                | ResourceAction::ArtifactDelete
        ),
        ResolvedResourceAccess::Task { .. } => matches!(
            action,
            ResourceAction::TaskRead
                | ResourceAction::TaskReadOperator
                | ResourceAction::TaskCreate
                | ResourceAction::TaskReview
                | ResourceAction::TaskCancel
                | ResourceAction::TaskScheduleManage
                | ResourceAction::TaskDetach
        ),
        ResolvedResourceAccess::Session { .. } => matches!(
            action,
            ResourceAction::SessionReadOwn
                | ResourceAction::SessionRevokeOwn
                | ResourceAction::ProfileUpdateOwn
        ),
        ResolvedResourceAccess::Capability { .. } => matches!(
            action,
            ResourceAction::ProviderDiscover
                | ResourceAction::ProviderUse
                | ResourceAction::ProviderManage
                | ResourceAction::McpDiscover
                | ResourceAction::McpUse
                | ResourceAction::McpReadOperator
                | ResourceAction::McpManage
                | ResourceAction::SkillDiscover
                | ResourceAction::SkillUse
                | ResourceAction::SkillManage
                | ResourceAction::CliRuntimeDiscover
                | ResourceAction::CliRuntimeUse
                | ResourceAction::CliRuntimeReadOperator
                | ResourceAction::CliRuntimeControl
                | ResourceAction::CliThreadFork
                | ResourceAction::CliRuntimeManage
        ),
        ResolvedResourceAccess::AgentsDocument { .. } => matches!(
            action,
            ResourceAction::AgentsDocumentRead | ResourceAction::AgentsDocumentManage
        ),
        ResolvedResourceAccess::InvitationGrantSet { .. } => {
            matches!(action, ResourceAction::InvitationCreate)
        }
        ResolvedResourceAccess::InvitationCollection => {
            matches!(action, ResourceAction::InvitationList)
        }
        ResolvedResourceAccess::Invitation { .. } => {
            matches!(action, ResourceAction::InvitationRevoke)
        }
        ResolvedResourceAccess::MemberDirectory => {
            matches!(action, ResourceAction::MemberDirectoryList)
        }
        ResolvedResourceAccess::DirectoryPrincipal { .. } => {
            matches!(action, ResourceAction::MemberAvatarRead)
        }
        ResolvedResourceAccess::MemberPrincipal => {
            matches!(
                action,
                ResourceAction::MemberSuspend
                    | ResourceAction::MemberRestore
                    | ResourceAction::MemberDeviceCreate
                    | ResourceAction::MemberRemove
            )
        }
    }
}

fn allow(role: &RoleKey, reason: AllowReason) -> AuthorizationDecision {
    AuthorizationDecision::AllowPolicy {
        role: role.clone(),
        reason,
    }
}

const fn deny_not_found(reason: DenyReason) -> AuthorizationDecision {
    AuthorizationDecision::Deny {
        reason,
        disclosure: DisclosurePolicy::NotFound,
    }
}

const fn deny_forbidden(reason: DenyReason) -> AuthorizationDecision {
    AuthorizationDecision::Deny {
        reason,
        disclosure: DisclosurePolicy::Forbidden,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use pioneer_protocol::{PrincipalId, PrincipalKind, RoleKey};

    use super::AuthorizationService;
    use crate::authorization::{
        ActionGateDecision, AuthorizationDecision, CliThreadForkExportFacts,
        CliThreadForkExportProjection, DenyReason, DisclosurePolicy, ResolvedResourceAccess,
        ResourceAction, ThreadAccessClass, ThreadAccessFacts, ThreadResourceClass,
        WorkspaceAccessFacts,
    };

    const MEMBER_RESOURCE_ACTIONS: [ResourceAction; 65] = [
        ResourceAction::WorkspaceList,
        ResourceAction::WorkspaceRead,
        ResourceAction::ThreadCreatePrivate,
        ResourceAction::ThreadCreateWorkspace,
        ResourceAction::ThreadRead,
        ResourceAction::MessageCreate,
        ResourceAction::MessageEditOwn,
        ResourceAction::MessageDeleteOwn,
        ResourceAction::AgentTurnStart,
        ResourceAction::AgentExecutionObserve,
        ResourceAction::AgentExecutionCancel,
        ResourceAction::AgentExecutionResume,
        ResourceAction::AgentExecutionSteer,
        ResourceAction::AgentRequestObserve,
        ResourceAction::AgentRequestRespond,
        ResourceAction::ChildObserve,
        ResourceAction::ChildWrite,
        ResourceAction::ChildStart,
        ResourceAction::ChildControl,
        ResourceAction::ChildRespond,
        ResourceAction::ChildTaskCreate,
        ResourceAction::ChildArtifactRead,
        ResourceAction::ChildArtifactWrite,
        ResourceAction::AgentSourceExport,
        ResourceAction::AgentRouteCreate,
        ResourceAction::AgentRouteRevoke,
        ResourceAction::AgentsDocumentRead,
        ResourceAction::ThreadManage,
        ResourceAction::ThreadParticipantsManage,
        ResourceAction::ArtifactRead,
        ResourceAction::ArtifactCreateThread,
        ResourceAction::ArtifactBindThread,
        ResourceAction::ArtifactDeleteThread,
        ResourceAction::MemoryRead,
        ResourceAction::MemoryCreateThread,
        ResourceAction::MemoryUpdateThread,
        ResourceAction::MemoryForgetThread,
        ResourceAction::TaskRead,
        ResourceAction::TaskCreate,
        ResourceAction::TaskReview,
        ResourceAction::TaskCancel,
        ResourceAction::TaskScheduleManage,
        ResourceAction::TaskDetach,
        ResourceAction::ProviderDiscover,
        ResourceAction::ProviderUse,
        ResourceAction::McpDiscover,
        ResourceAction::McpUse,
        ResourceAction::SkillDiscover,
        ResourceAction::SkillUse,
        ResourceAction::CliRuntimeDiscover,
        ResourceAction::CliRuntimeUse,
        ResourceAction::CliRuntimeControl,
        ResourceAction::CliThreadFork,
        ResourceAction::SessionReadOwn,
        ResourceAction::SessionRevokeOwn,
        ResourceAction::ProfileUpdateOwn,
        ResourceAction::NotificationReadOwn,
        ResourceAction::NotificationAcknowledgeOwn,
        ResourceAction::InvitationCreate,
        ResourceAction::InvitationList,
        ResourceAction::InvitationRevoke,
        ResourceAction::MemberDirectoryList,
        ResourceAction::MemberAvatarRead,
        ResourceAction::WorkspaceMemberList,
        ResourceAction::WorkspaceMemberAdd,
    ];

    #[test]
    fn synthetic_role_projects_its_granular_permission_ceiling_as_data() {
        let service = AuthorizationService::new();
        let role = RoleKey::new("synthetic_observer").expect("synthetic role key");
        let options = service.agent_permission_options(PrincipalKind::User, Some(&role));

        assert_eq!(options.len(), 1);
        let option = &options[0];
        assert_eq!(
            option.mode,
            pioneer_protocol::TurnPermissionMode::Supervised
        );
        assert_eq!(
            option.effective_policy.file_read,
            pioneer_protocol::PermissionBehavior::Allow
        );
        assert_eq!(
            option.effective_policy.shell_command,
            pioneer_protocol::PermissionBehavior::Deny
        );
        assert_eq!(
            option.effective_policy.network,
            pioneer_protocol::PermissionBehavior::Deny
        );
        assert_eq!(
            option.effective_policy.allowed_tools,
            vec!["review_read".to_owned()]
        );
        assert!(
            option.locked.iter().any(|lock| {
                lock.field == "shell_command" && lock.reason == "role_policy_maximum"
            })
        );
    }

    const MEMBER_DENIED_ACTIONS: [ResourceAction; 22] = [
        ResourceAction::GatewayManage,
        ResourceAction::WorkspaceCreate,
        ResourceAction::WorkspaceManage,
        ResourceAction::AgentsDocumentManage,
        ResourceAction::ThreadMove,
        ResourceAction::ArtifactDelete,
        ResourceAction::ArtifactDeleteOwn,
        ResourceAction::ArtifactManageWorkspace,
        ResourceAction::MemoryForgetWorkspace,
        ResourceAction::MemoryModerateWorkspace,
        ResourceAction::TaskReadOperator,
        ResourceAction::ProviderManage,
        ResourceAction::McpReadOperator,
        ResourceAction::McpManage,
        ResourceAction::SkillManage,
        ResourceAction::CliRuntimeReadOperator,
        ResourceAction::CliRuntimeManage,
        ResourceAction::WorkspaceMemberRemove,
        ResourceAction::MemberSuspend,
        ResourceAction::MemberRestore,
        ResourceAction::MemberRemove,
        ResourceAction::MemberDeviceCreate,
    ];

    #[test]
    fn member_action_matrix_is_an_exact_partition_of_the_canonical_vocabulary() {
        let allowed = MEMBER_RESOURCE_ACTIONS.into_iter().collect::<HashSet<_>>();
        let denied = MEMBER_DENIED_ACTIONS.into_iter().collect::<HashSet<_>>();
        let all = ResourceAction::ALL.into_iter().collect::<HashSet<_>>();

        assert!(allowed.is_disjoint(&denied));
        assert_eq!(allowed.union(&denied).copied().collect::<HashSet<_>>(), all);
        let service = AuthorizationService::new();
        let role = RoleKey::member();
        for action in ResourceAction::ALL {
            assert_eq!(
                service
                    .authorize_action(PrincipalKind::User, Some(&role), action)
                    .permits_resource_resolution(),
                allowed.contains(&action)
            );
        }
    }

    #[test]
    fn member_policy_never_turns_the_action_gate_into_a_final_grant() {
        let service = AuthorizationService::new();
        let role = RoleKey::member();

        for action in MEMBER_RESOURCE_ACTIONS {
            let decision = service.authorize_action(PrincipalKind::User, Some(&role), action);
            assert_eq!(
                decision,
                ActionGateDecision::RequireResource { role: role.clone() }
            );
            assert!(decision.permits_resource_resolution());
            assert!(!decision.is_final_allow());
        }
    }

    #[test]
    fn member_management_actions_are_denied_before_resource_resolution() {
        let service = AuthorizationService::new();
        let role = RoleKey::member();

        for action in MEMBER_DENIED_ACTIONS {
            let decision = service.authorize_action(PrincipalKind::User, Some(&role), action);
            assert_eq!(
                decision,
                ActionGateDecision::Deny {
                    reason: DenyReason::ManagementDenied,
                    disclosure: DisclosurePolicy::Forbidden,
                }
            );
            assert!(!decision.permits_resource_resolution());
            assert!(!decision.is_final_allow());
        }
    }

    #[test]
    fn superuser_is_absolute_without_memberships_but_requires_a_valid_shape() {
        let service = AuthorizationService::new();

        for action in ResourceAction::ALL {
            let decision = service.authorize_action(PrincipalKind::Superuser, None, action);
            assert_eq!(decision, ActionGateDecision::AllowAbsolute);
            assert!(decision.permits_resource_resolution());
            assert!(decision.is_final_allow());
        }

        assert_eq!(
            service.authorize_action(
                PrincipalKind::Superuser,
                Some(&RoleKey::member()),
                ResourceAction::WorkspaceRead,
            ),
            ActionGateDecision::Deny {
                reason: DenyReason::UnsupportedRole,
                disclosure: DisclosurePolicy::AuthenticationTerminal,
            }
        );
    }

    #[test]
    fn roleless_and_unknown_users_fail_closed_with_bounded_reasons() {
        let service = AuthorizationService::new();
        let future_role = RoleKey::new("future").expect("syntactically valid role");

        assert_eq!(
            service.resolved_role_key(PrincipalKind::Superuser, None),
            Some("superuser")
        );
        assert_eq!(
            service.resolved_role_key(PrincipalKind::User, Some(&RoleKey::member())),
            Some("member")
        );
        assert_eq!(
            service.resolved_role_key(PrincipalKind::User, Some(&future_role)),
            None
        );

        for role in [None, Some(&future_role)] {
            for action in ResourceAction::ALL {
                let decision = service.authorize_action(PrincipalKind::User, role, action);
                assert_eq!(
                    decision,
                    ActionGateDecision::Deny {
                        reason: DenyReason::UnsupportedRole,
                        disclosure: DisclosurePolicy::AuthenticationTerminal,
                    }
                );
                assert_eq!(decision.safe_name(), "deny");
            }
        }
    }

    #[test]
    fn two_user_roles_receive_distinct_registry_decisions_without_kind_fallback() {
        let service = AuthorizationService::new();
        let observer = RoleKey::new("synthetic_observer").unwrap();
        let executor = RoleKey::new("synthetic_executor").unwrap();

        assert!(
            service
                .authorize_action(
                    PrincipalKind::User,
                    Some(&observer),
                    ResourceAction::ThreadRead,
                )
                .permits_resource_resolution()
        );
        assert!(
            !service
                .authorize_action(
                    PrincipalKind::User,
                    Some(&observer),
                    ResourceAction::MessageCreate,
                )
                .permits_resource_resolution()
        );
        assert!(
            service
                .authorize_action(
                    PrincipalKind::User,
                    Some(&executor),
                    ResourceAction::AgentTurnStart,
                )
                .permits_resource_resolution()
        );
        assert_eq!(
            service.authorize_action(PrincipalKind::User, None, ResourceAction::ThreadRead,),
            ActionGateDecision::Deny {
                reason: DenyReason::UnsupportedRole,
                disclosure: DisclosurePolicy::AuthenticationTerminal,
            }
        );
    }

    #[test]
    fn opaque_cli_fork_requires_export_action_and_exact_private_audience() {
        let service = AuthorizationService::new();
        let member = RoleKey::member();
        let member_a = PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap();
        let member_b = PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").unwrap();
        let source = BTreeSet::from([member_a.clone(), member_b]);
        let same_destination = source.clone();
        let narrower_destination = BTreeSet::from([member_a]);
        assert!(service.cli_thread_fork_export_allowed(
            PrincipalKind::User,
            Some(&member),
            CliThreadForkExportFacts {
                source_access_class: ThreadAccessClass::Private,
                source_principals: &source,
                destination_access_class: ThreadAccessClass::Private,
                destination_principals: &same_destination,
                projection: CliThreadForkExportProjection::OpaqueNativeState,
            },
        ));
        assert!(!service.cli_thread_fork_export_allowed(
            PrincipalKind::User,
            Some(&member),
            CliThreadForkExportFacts {
                source_access_class: ThreadAccessClass::Private,
                source_principals: &source,
                destination_access_class: ThreadAccessClass::Private,
                destination_principals: &narrower_destination,
                projection: CliThreadForkExportProjection::OpaqueNativeState,
            },
        ));
        assert!(!service.cli_thread_fork_export_allowed(
            PrincipalKind::User,
            Some(&RoleKey::new("synthetic_observer").unwrap()),
            CliThreadForkExportFacts {
                source_access_class: ThreadAccessClass::Private,
                source_principals: &source,
                destination_access_class: ThreadAccessClass::Private,
                destination_principals: &same_destination,
                projection: CliThreadForkExportProjection::OpaqueNativeState,
            },
        ));
        assert!(!service.cli_thread_fork_export_allowed(
            PrincipalKind::User,
            Some(&member),
            CliThreadForkExportFacts {
                source_access_class: ThreadAccessClass::Workspace,
                source_principals: &source,
                destination_access_class: ThreadAccessClass::Private,
                destination_principals: &same_destination,
                projection: CliThreadForkExportProjection::OpaqueNativeState,
            },
        ));
    }

    #[test]
    fn operational_projection_uses_exact_role_allow_entries_and_revision_receipt() {
        let service = AuthorizationService::new();
        let role = RoleKey::new("synthetic_executor").unwrap();

        assert!(service.provider_allowed(PrincipalKind::User, Some(&role), "allowed-provider"));
        assert!(!service.provider_allowed(PrincipalKind::User, Some(&role), "hidden-provider"));
        assert!(service.provider_model_allowed(
            PrincipalKind::User,
            Some(&role),
            "allowed-provider",
            "allowed-model"
        ));
        assert!(!service.provider_model_allowed(
            PrincipalKind::User,
            Some(&role),
            "allowed-provider",
            "hidden-model"
        ));
        assert!(service.skill_allowed(PrincipalKind::User, Some(&role), "allowed-skill"));
        assert!(!service.mcp_server_allowed(PrincipalKind::User, Some(&role), "hidden-mcp"));

        let first = service
            .operational_projection(PrincipalKind::User, Some(&role), "workspace-a", 7)
            .unwrap();
        let next = service
            .operational_projection(PrincipalKind::User, Some(&role), "workspace-a", 8)
            .unwrap();
        assert!(!first.providers.all);
        assert_eq!(first.providers.ids, ["allowed-provider"]);
        assert!(!first.provider_models_all);
        assert_eq!(
            first.provider_models,
            [pioneer_protocol::AuthorizationProviderModelGrant {
                provider: "allowed-provider".to_owned(),
                model: "allowed-model".to_owned(),
            }]
        );
        assert!(!first.cli_models_all);
        assert_eq!(
            first.cli_models,
            [pioneer_protocol::AuthorizationCliModelGrant {
                runtime_id: "allowed-cli".to_owned(),
                model: "allowed-cli-model".to_owned(),
            }]
        );
        assert_ne!(first.fingerprint, next.fingerprint);

        let member = service
            .operational_projection(
                PrincipalKind::User,
                Some(&RoleKey::member()),
                "workspace-a",
                7,
            )
            .unwrap();
        assert!(member.provider_models_all);
        assert!(member.provider_models.is_empty());
        assert!(member.cli_models_all);
        assert!(member.cli_models.is_empty());
    }

    #[test]
    fn member_thread_policy_requires_exact_workspace_and_visibility_facts() {
        let service = AuthorizationService::new();
        let role = RoleKey::member();
        let gate =
            service.authorize_action(PrincipalKind::User, Some(&role), ResourceAction::ThreadRead);
        let workspace = WorkspaceAccessFacts {
            workspace_active: true,
            workspace_member: true,
        };

        assert!(
            service
                .authorize_resource(
                    &gate,
                    ResourceAction::ThreadRead,
                    ResolvedResourceAccess::Thread(ThreadAccessFacts {
                        workspace,
                        access_class: ThreadAccessClass::Workspace,
                        resource_class: ThreadResourceClass::Root,
                        thread_member: false,
                        thread_creator: false,
                    }),
                )
                .is_allowed()
        );
        assert_eq!(
            service.authorize_resource(
                &gate,
                ResourceAction::ThreadRead,
                ResolvedResourceAccess::Thread(ThreadAccessFacts {
                    workspace,
                    access_class: ThreadAccessClass::Private,
                    resource_class: ThreadResourceClass::Root,
                    thread_member: false,
                    thread_creator: false,
                }),
            ),
            AuthorizationDecision::Deny {
                reason: DenyReason::NoPrivateThreadMembership,
                disclosure: DisclosurePolicy::NotFound,
            }
        );
        assert_eq!(
            service.authorize_resource(
                &gate,
                ResourceAction::ThreadRead,
                ResolvedResourceAccess::Thread(ThreadAccessFacts {
                    workspace,
                    access_class: ThreadAccessClass::Internal,
                    resource_class: ThreadResourceClass::Root,
                    thread_member: true,
                    thread_creator: true,
                }),
            ),
            AuthorizationDecision::Deny {
                reason: DenyReason::MissingAuthoritativeResource,
                disclosure: DisclosurePolicy::NotFound,
            }
        );
    }

    #[test]
    fn creator_session_task_and_capability_facts_cannot_be_reused() {
        let service = AuthorizationService::new();
        let role = RoleKey::member();
        let workspace = WorkspaceAccessFacts {
            workspace_active: true,
            workspace_member: true,
        };
        let thread = ThreadAccessFacts {
            workspace,
            access_class: ThreadAccessClass::Private,
            resource_class: ThreadResourceClass::Root,
            thread_member: true,
            thread_creator: false,
        };

        let manage_gate = service.authorize_action(
            PrincipalKind::User,
            Some(&role),
            ResourceAction::ThreadManage,
        );
        assert_eq!(
            service.authorize_resource(
                &manage_gate,
                ResourceAction::ThreadManage,
                ResolvedResourceAccess::Thread(thread),
            ),
            AuthorizationDecision::Deny {
                reason: DenyReason::NotThreadCreator,
                disclosure: DisclosurePolicy::Forbidden,
            }
        );

        let participants_gate = service.authorize_action(
            PrincipalKind::User,
            Some(&role),
            ResourceAction::ThreadParticipantsManage,
        );
        assert_eq!(
            service.authorize_resource(
                &participants_gate,
                ResourceAction::ThreadParticipantsManage,
                ResolvedResourceAccess::Thread(thread),
            ),
            AuthorizationDecision::Deny {
                reason: DenyReason::ManagementDenied,
                disclosure: DisclosurePolicy::Forbidden,
            }
        );

        let session_gate = service.authorize_action(
            PrincipalKind::User,
            Some(&role),
            ResourceAction::SessionRevokeOwn,
        );
        assert_eq!(
            service.authorize_resource(
                &session_gate,
                ResourceAction::SessionRevokeOwn,
                ResolvedResourceAccess::Session {
                    owns_session: false,
                },
            ),
            AuthorizationDecision::Deny {
                reason: DenyReason::ResourceScopeMismatch,
                disclosure: DisclosurePolicy::NotFound,
            }
        );

        let capability_gate =
            service.authorize_action(PrincipalKind::User, Some(&role), ResourceAction::SkillUse);
        assert_eq!(
            service.authorize_resource(
                &capability_gate,
                ResourceAction::SkillUse,
                ResolvedResourceAccess::Capability {
                    workspace,
                    enabled: false,
                },
            ),
            AuthorizationDecision::Deny {
                reason: DenyReason::CapabilityDisabled,
                disclosure: DisclosurePolicy::Forbidden,
            }
        );

        let task_gate =
            service.authorize_action(PrincipalKind::User, Some(&role), ResourceAction::TaskReview);
        assert!(
            service
                .authorize_resource(
                    &task_gate,
                    ResourceAction::TaskReview,
                    ResolvedResourceAccess::Task {
                        workspace,
                        root_thread: Some(thread),
                        initiating_principal: false,
                    },
                )
                .is_allowed(),
            "task actor provenance must not override shared root collaboration authority"
        );

        let task_read_gate =
            service.authorize_action(PrincipalKind::User, Some(&role), ResourceAction::TaskRead);
        assert_eq!(
            service.authorize_resource(
                &task_read_gate,
                ResourceAction::TaskRead,
                ResolvedResourceAccess::Task {
                    workspace,
                    root_thread: None,
                    initiating_principal: true,
                },
            ),
            AuthorizationDecision::Deny {
                reason: DenyReason::MissingAuthoritativeResource,
                disclosure: DisclosurePolicy::NotFound,
            },
            "workspace membership cannot turn detached task state into a workspace-global resource"
        );
    }

    #[test]
    fn superuser_resource_allow_still_requires_resolver_consistency() {
        let service = AuthorizationService::new();
        let gate =
            service.authorize_action(PrincipalKind::Superuser, None, ResourceAction::ArtifactRead);
        assert_eq!(
            service.authorize_resource(
                &gate,
                ResourceAction::ArtifactRead,
                ResolvedResourceAccess::Artifact {
                    workspace: WorkspaceAccessFacts {
                        workspace_active: false,
                        workspace_member: false,
                    },
                    thread: None,
                },
            ),
            AuthorizationDecision::AllowAbsolute
        );
        // The service trusts only facts produced by the resolver. Missing,
        // dangling, or mismatched rows never reach this call.
    }
}
