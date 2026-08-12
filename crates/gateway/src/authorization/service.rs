use pioneer_protocol::{
    MEMBER_ROLE_KEY, PrincipalKind, RoleKey, SUPERUSER_CAPABILITY_ROLE_KEY, TurnPermissionMode,
    TurnPermissionProfileCap,
};

use super::{
    ActionGateDecision, AllowReason, AuthorizationDecision, DenyReason, DisclosurePolicy,
    ResourceAction, ThreadAccessClass,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceAccessFacts {
    pub(crate) workspace_active: bool,
    pub(crate) workspace_member: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ThreadAccessFacts {
    pub(crate) workspace: WorkspaceAccessFacts,
    pub(crate) access_class: ThreadAccessClass,
    pub(crate) thread_member: bool,
    pub(crate) thread_creator: bool,
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

/// Closed registry of roles implemented by this Gateway binary.
///
/// Adding another code-defined role is intentionally an exhaustive compiler
/// change: register its wire key here and add its action/resource policy in
/// `AuthorizationService`. Capability snapshots use this same registry, so a
/// role can never be recognized by the UI projection but rejected by the RPC
/// authorization layer (or vice versa).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BuiltInAuthorizationRole {
    Superuser,
    Member,
}

impl BuiltInAuthorizationRole {
    fn resolve(principal_kind: PrincipalKind, role_key: Option<&RoleKey>) -> Option<Self> {
        match (principal_kind, role_key.map(RoleKey::as_str)) {
            (PrincipalKind::Superuser, None) => Some(Self::Superuser),
            (PrincipalKind::User, Some(MEMBER_ROLE_KEY)) => Some(Self::Member),
            _ => None,
        }
    }

    const fn key(self) -> &'static str {
        match self {
            Self::Superuser => SUPERUSER_CAPABILITY_ROLE_KEY,
            Self::Member => MEMBER_ROLE_KEY,
        }
    }
}

/// The single role-to-policy boundary for normal Gateway actions.
///
/// This service performs only the first authorization level. A Member result
/// of `RequireResource` must be followed by an exact server-owned resource
/// lookup and resource decision before any handler side effect. Superuser is
/// the only principal that receives an action-level final allow; resource
/// resolvers must still reject malformed or inconsistent resource identity.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AuthorizationService;

impl AuthorizationService {
    pub(crate) const fn new() -> Self {
        Self
    }

    pub(crate) fn authorize_action(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
        action: ResourceAction,
    ) -> ActionGateDecision {
        match BuiltInAuthorizationRole::resolve(principal_kind, role_key) {
            Some(BuiltInAuthorizationRole::Superuser) => ActionGateDecision::AllowSuperuser,
            Some(BuiltInAuthorizationRole::Member) => {
                authorize_member_action(&RoleKey::member(), action)
            }
            None => deny_unsupported_role(),
        }
    }

    /// Stable identifier for a role implemented by this Gateway binary.
    /// This is metadata only; clients must use capability bits, not this key,
    /// for presentation or authorization decisions.
    pub(crate) fn built_in_role_key(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<&'static str> {
        BuiltInAuthorizationRole::resolve(principal_kind, role_key)
            .map(BuiltInAuthorizationRole::key)
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
        let mode = match BuiltInAuthorizationRole::resolve(principal_kind, role_key)? {
            BuiltInAuthorizationRole::Superuser => TurnPermissionMode::FullAccess,
            BuiltInAuthorizationRole::Member => TurnPermissionMode::Supervised,
        };
        Some(pioneer_protocol::task_permission_cap_for_mode(mode))
    }

    pub(crate) fn allowed_turn_permission_modes(
        &self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Vec<TurnPermissionMode> {
        let Some(cap) = self.turn_permission_profile_cap(principal_kind, role_key) else {
            return Vec::new();
        };
        let cap_snapshot = pioneer_protocol::task_permission_cap_snapshot(&cap);
        [
            TurnPermissionMode::FullAccess,
            TurnPermissionMode::AutoAcceptEdits,
            TurnPermissionMode::Supervised,
        ]
        .into_iter()
        .filter(|mode| {
            let requested = pioneer_protocol::compile_turn_permission_profile(
                *mode,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            );
            pioneer_protocol::intersect_turn_permission_profiles(
                &requested,
                &cap_snapshot,
                pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
            )
            .mode
                == *mode
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
            ActionGateDecision::AllowSuperuser => AuthorizationDecision::AllowSuperuser,
            ActionGateDecision::Deny { reason, disclosure } => AuthorizationDecision::Deny {
                reason: *reason,
                disclosure: *disclosure,
            },
            ActionGateDecision::RequireResource { role } => {
                match BuiltInAuthorizationRole::resolve(PrincipalKind::User, Some(role)) {
                    Some(BuiltInAuthorizationRole::Member) => {
                        authorize_member_resource(role, action, access)
                    }
                    Some(BuiltInAuthorizationRole::Superuser) | None => {
                        AuthorizationDecision::Deny {
                            reason: DenyReason::UnsupportedRole,
                            disclosure: DisclosurePolicy::AuthenticationTerminal,
                        }
                    }
                }
            }
        }
    }
}

fn authorize_member_action(role_key: &RoleKey, action: ResourceAction) -> ActionGateDecision {
    if member_action_requires_resource(action) {
        ActionGateDecision::RequireResource {
            role: role_key.clone(),
        }
    } else {
        ActionGateDecision::Deny {
            reason: DenyReason::ManagementDenied,
            disclosure: DisclosurePolicy::Forbidden,
        }
    }
}

const fn member_action_requires_resource(action: ResourceAction) -> bool {
    matches!(
        action,
        ResourceAction::WorkspaceList
            | ResourceAction::WorkspaceRead
            | ResourceAction::ThreadCreate
            | ResourceAction::ThreadRead
            | ResourceAction::ThreadWrite
            | ResourceAction::ThreadManage
            | ResourceAction::ThreadParticipantsManage
            | ResourceAction::ArtifactRead
            | ResourceAction::ArtifactWrite
            | ResourceAction::MemoryRead
            | ResourceAction::MemoryWrite
            | ResourceAction::TaskRead
            | ResourceAction::TaskRun
            | ResourceAction::TaskManage
            | ResourceAction::ProviderUse
            | ResourceAction::McpUse
            | ResourceAction::SkillUse
            | ResourceAction::CliRuntimeUse
            | ResourceAction::SessionReadOwn
            | ResourceAction::SessionRevokeOwn
            | ResourceAction::ProfileUpdateOwn
            | ResourceAction::InvitationCreate
            | ResourceAction::InvitationList
            | ResourceAction::InvitationRevoke
            | ResourceAction::MemberDirectoryList
            | ResourceAction::MemberAvatarRead
            | ResourceAction::WorkspaceMemberList
            | ResourceAction::WorkspaceMemberAdd
    )
}

fn deny_unsupported_role() -> ActionGateDecision {
    ActionGateDecision::Deny {
        reason: DenyReason::UnsupportedRole,
        disclosure: DisclosurePolicy::AuthenticationTerminal,
    }
}

fn authorize_member_resource(
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
            initiating_principal,
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
            if action == ResourceAction::TaskManage && !initiating_principal {
                deny_not_found(DenyReason::ManagementDenied)
            } else {
                inherited
            }
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
                | ResourceAction::ThreadCreate
                | ResourceAction::ArtifactRead
                | ResourceAction::MemoryRead
                | ResourceAction::MemoryWrite
                | ResourceAction::TaskRead
                | ResourceAction::TaskRun
                | ResourceAction::ProviderUse
                | ResourceAction::McpUse
                | ResourceAction::SkillUse
                | ResourceAction::CliRuntimeUse
                | ResourceAction::WorkspaceMemberList
                | ResourceAction::WorkspaceMemberAdd
                | ResourceAction::WorkspaceMemberRemove
        ),
        ResolvedResourceAccess::Thread(_) => matches!(
            action,
            ResourceAction::ThreadRead
                | ResourceAction::ThreadWrite
                | ResourceAction::ThreadManage
                | ResourceAction::ThreadMove
                | ResourceAction::ThreadParticipantsManage
                | ResourceAction::ArtifactRead
                | ResourceAction::ArtifactWrite
                | ResourceAction::TaskRun
                | ResourceAction::CliRuntimeUse
        ),
        ResolvedResourceAccess::Turn(_) => matches!(
            action,
            ResourceAction::ThreadRead
                | ResourceAction::ThreadWrite
                | ResourceAction::CliRuntimeUse
        ),
        ResolvedResourceAccess::Artifact { .. } => matches!(
            action,
            ResourceAction::ArtifactRead
                | ResourceAction::ArtifactWrite
                | ResourceAction::ArtifactDelete
        ),
        ResolvedResourceAccess::Task { .. } => matches!(
            action,
            ResourceAction::TaskRead | ResourceAction::TaskRun | ResourceAction::TaskManage
        ),
        ResolvedResourceAccess::Session { .. } => matches!(
            action,
            ResourceAction::SessionReadOwn
                | ResourceAction::SessionRevokeOwn
                | ResourceAction::ProfileUpdateOwn
        ),
        ResolvedResourceAccess::Capability { .. } => matches!(
            action,
            ResourceAction::ProviderUse
                | ResourceAction::ProviderManage
                | ResourceAction::McpUse
                | ResourceAction::McpManage
                | ResourceAction::SkillUse
                | ResourceAction::SkillManage
                | ResourceAction::CliRuntimeUse
                | ResourceAction::CliRuntimeManage
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
    use std::collections::HashSet;

    use pioneer_protocol::{PrincipalKind, RoleKey};

    use super::{AuthorizationService, member_action_requires_resource};
    use crate::authorization::{
        ActionGateDecision, AuthorizationDecision, DenyReason, DisclosurePolicy,
        ResolvedResourceAccess, ResourceAction, ThreadAccessClass, ThreadAccessFacts,
        WorkspaceAccessFacts,
    };

    const MEMBER_RESOURCE_ACTIONS: [ResourceAction; 28] = [
        ResourceAction::WorkspaceList,
        ResourceAction::WorkspaceRead,
        ResourceAction::ThreadCreate,
        ResourceAction::ThreadRead,
        ResourceAction::ThreadWrite,
        ResourceAction::ThreadManage,
        ResourceAction::ThreadParticipantsManage,
        ResourceAction::ArtifactRead,
        ResourceAction::ArtifactWrite,
        ResourceAction::MemoryRead,
        ResourceAction::MemoryWrite,
        ResourceAction::TaskRead,
        ResourceAction::TaskRun,
        ResourceAction::TaskManage,
        ResourceAction::ProviderUse,
        ResourceAction::McpUse,
        ResourceAction::SkillUse,
        ResourceAction::CliRuntimeUse,
        ResourceAction::SessionReadOwn,
        ResourceAction::SessionRevokeOwn,
        ResourceAction::ProfileUpdateOwn,
        ResourceAction::InvitationCreate,
        ResourceAction::InvitationList,
        ResourceAction::InvitationRevoke,
        ResourceAction::MemberDirectoryList,
        ResourceAction::MemberAvatarRead,
        ResourceAction::WorkspaceMemberList,
        ResourceAction::WorkspaceMemberAdd,
    ];

    const MEMBER_DENIED_ACTIONS: [ResourceAction; 14] = [
        ResourceAction::GatewayManage,
        ResourceAction::WorkspaceCreate,
        ResourceAction::WorkspaceManage,
        ResourceAction::ThreadMove,
        ResourceAction::ArtifactDelete,
        ResourceAction::ProviderManage,
        ResourceAction::McpManage,
        ResourceAction::SkillManage,
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
        for action in ResourceAction::ALL {
            assert_eq!(
                member_action_requires_resource(action),
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
            assert_eq!(decision, ActionGateDecision::AllowSuperuser);
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
            service.built_in_role_key(PrincipalKind::Superuser, None),
            Some("superuser")
        );
        assert_eq!(
            service.built_in_role_key(PrincipalKind::User, Some(&RoleKey::member())),
            Some("member")
        );
        assert_eq!(
            service.built_in_role_key(PrincipalKind::User, Some(&future_role)),
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
            service.authorize_action(PrincipalKind::User, Some(&role), ResourceAction::TaskManage);
        assert_eq!(
            service.authorize_resource(
                &task_gate,
                ResourceAction::TaskManage,
                ResolvedResourceAccess::Task {
                    workspace,
                    root_thread: Some(thread),
                    initiating_principal: false,
                },
            ),
            AuthorizationDecision::Deny {
                reason: DenyReason::ManagementDenied,
                disclosure: DisclosurePolicy::NotFound,
            }
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
            AuthorizationDecision::AllowSuperuser
        );
        // The service trusts only facts produced by the resolver. Missing,
        // dangling, or mismatched rows never reach this call.
    }
}
