use std::fmt;
use std::marker::PhantomData;

use pioneer_protocol::{AuthSessionId, GatewayId, PrincipalId, RoleKey, ThreadVisibility};

pub(crate) const RESOURCE_ID_MAX_LEN: usize = 256;

/// A bounded authoritative resource identifier whose marker prevents
/// unrelated child IDs from being accidentally swapped.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ResourceId<K> {
    value: String,
    kind: PhantomData<fn() -> K>,
}

impl<K> ResourceId<K> {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, ResourceIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ResourceIdError::Empty);
        }
        if value.len() > RESOURCE_ID_MAX_LEN {
            return Err(ResourceIdError::TooLong {
                maximum: RESOURCE_ID_MAX_LEN,
                actual: value.len(),
            });
        }
        if let Some((index, character)) = value
            .char_indices()
            .find(|(_, character)| character.is_control())
        {
            return Err(ResourceIdError::ControlCharacter { index, character });
        }
        Ok(Self {
            value,
            kind: PhantomData,
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        self.value.as_str()
    }
}

impl<K> fmt::Display for ResourceId<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResourceIdError {
    Empty,
    TooLong { maximum: usize, actual: usize },
    ControlCharacter { index: usize, character: char },
}

impl fmt::Display for ResourceIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("authorization resource id must not be empty"),
            Self::TooLong { maximum, actual } => write!(
                formatter,
                "authorization resource id must contain at most {maximum} bytes, got {actual}"
            ),
            Self::ControlCharacter { index, character } => write!(
                formatter,
                "authorization resource id contains control character {character:?} at byte {index}"
            ),
        }
    }
}

impl std::error::Error for ResourceIdError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum WorkspaceResource {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ThreadResource {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum TurnResource {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ArtifactResource {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum TaskResource {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum CapabilityResource {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum AgentsDocumentResource {}

pub(crate) type WorkspaceResourceId = ResourceId<WorkspaceResource>;
pub(crate) type ThreadResourceId = ResourceId<ThreadResource>;
pub(crate) type TurnResourceId = ResourceId<TurnResource>;
pub(crate) type ArtifactResourceId = ResourceId<ArtifactResource>;
pub(crate) type TaskResourceId = ResourceId<TaskResource>;
pub(crate) type CapabilityResourceId = ResourceId<CapabilityResource>;
pub(crate) type AgentsDocumentResourceId = ResourceId<AgentsDocumentResource>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ResourceAction {
    GatewayManage,
    WorkspaceList,
    WorkspaceRead,
    WorkspaceCreate,
    WorkspaceManage,
    ThreadCreatePrivate,
    ThreadCreateWorkspace,
    ThreadRead,
    MessageCreate,
    MessageEditOwn,
    MessageDeleteOwn,
    AgentTurnStart,
    AgentExecutionObserve,
    AgentExecutionCancel,
    AgentExecutionResume,
    AgentExecutionSteer,
    AgentRequestObserve,
    AgentRequestRespond,
    ChildObserve,
    ChildWrite,
    ChildStart,
    ChildControl,
    ChildRespond,
    ChildTaskCreate,
    ChildArtifactRead,
    ChildArtifactWrite,
    AgentsDocumentRead,
    AgentsDocumentManage,
    ThreadManage,
    ThreadMove,
    ThreadParticipantsManage,
    ArtifactRead,
    ArtifactCreateThread,
    ArtifactBindThread,
    ArtifactDeleteThread,
    ArtifactDeleteOwn,
    ArtifactManageWorkspace,
    ArtifactDelete,
    MemoryRead,
    MemoryCreateThread,
    MemoryUpdateThread,
    MemoryForgetThread,
    MemoryForgetWorkspace,
    MemoryModerateWorkspace,
    TaskRead,
    TaskReadOperator,
    TaskCreate,
    TaskReview,
    TaskCancel,
    TaskScheduleManage,
    TaskDetach,
    ProviderDiscover,
    ProviderUse,
    ProviderManage,
    McpDiscover,
    McpUse,
    McpReadOperator,
    McpManage,
    SkillDiscover,
    SkillUse,
    SkillManage,
    CliRuntimeDiscover,
    CliRuntimeUse,
    CliRuntimeReadOperator,
    CliRuntimeControl,
    CliThreadFork,
    CliRuntimeManage,
    SessionReadOwn,
    SessionRevokeOwn,
    ProfileUpdateOwn,
    NotificationReadOwn,
    NotificationAcknowledgeOwn,
    InvitationCreate,
    InvitationList,
    InvitationRevoke,
    MemberDirectoryList,
    MemberAvatarRead,
    WorkspaceMemberList,
    WorkspaceMemberAdd,
    WorkspaceMemberRemove,
    MemberSuspend,
    MemberRestore,
    MemberDeviceCreate,
    MemberRemove,
}

impl ResourceAction {
    pub(crate) const ALL: [Self; 84] = [
        Self::GatewayManage,
        Self::WorkspaceList,
        Self::WorkspaceRead,
        Self::WorkspaceCreate,
        Self::WorkspaceManage,
        Self::ThreadCreatePrivate,
        Self::ThreadCreateWorkspace,
        Self::ThreadRead,
        Self::MessageCreate,
        Self::MessageEditOwn,
        Self::MessageDeleteOwn,
        Self::AgentTurnStart,
        Self::AgentExecutionObserve,
        Self::AgentExecutionCancel,
        Self::AgentExecutionResume,
        Self::AgentExecutionSteer,
        Self::AgentRequestObserve,
        Self::AgentRequestRespond,
        Self::ChildObserve,
        Self::ChildWrite,
        Self::ChildStart,
        Self::ChildControl,
        Self::ChildRespond,
        Self::ChildTaskCreate,
        Self::ChildArtifactRead,
        Self::ChildArtifactWrite,
        Self::AgentsDocumentRead,
        Self::AgentsDocumentManage,
        Self::ThreadManage,
        Self::ThreadMove,
        Self::ThreadParticipantsManage,
        Self::ArtifactRead,
        Self::ArtifactCreateThread,
        Self::ArtifactBindThread,
        Self::ArtifactDeleteThread,
        Self::ArtifactDeleteOwn,
        Self::ArtifactManageWorkspace,
        Self::ArtifactDelete,
        Self::MemoryRead,
        Self::MemoryCreateThread,
        Self::MemoryUpdateThread,
        Self::MemoryForgetThread,
        Self::MemoryForgetWorkspace,
        Self::MemoryModerateWorkspace,
        Self::TaskRead,
        Self::TaskReadOperator,
        Self::TaskCreate,
        Self::TaskReview,
        Self::TaskCancel,
        Self::TaskScheduleManage,
        Self::TaskDetach,
        Self::ProviderDiscover,
        Self::ProviderUse,
        Self::ProviderManage,
        Self::McpDiscover,
        Self::McpUse,
        Self::McpReadOperator,
        Self::McpManage,
        Self::SkillDiscover,
        Self::SkillUse,
        Self::SkillManage,
        Self::CliRuntimeDiscover,
        Self::CliRuntimeUse,
        Self::CliRuntimeReadOperator,
        Self::CliRuntimeControl,
        Self::CliThreadFork,
        Self::CliRuntimeManage,
        Self::SessionReadOwn,
        Self::SessionRevokeOwn,
        Self::ProfileUpdateOwn,
        Self::NotificationReadOwn,
        Self::NotificationAcknowledgeOwn,
        Self::InvitationCreate,
        Self::InvitationList,
        Self::InvitationRevoke,
        Self::MemberDirectoryList,
        Self::MemberAvatarRead,
        Self::WorkspaceMemberList,
        Self::WorkspaceMemberAdd,
        Self::WorkspaceMemberRemove,
        Self::MemberSuspend,
        Self::MemberRestore,
        Self::MemberDeviceCreate,
        Self::MemberRemove,
    ];

    pub(crate) const fn safe_name(self) -> &'static str {
        match self {
            Self::GatewayManage => "gateway_manage",
            Self::WorkspaceList => "workspace_list",
            Self::WorkspaceRead => "workspace_read",
            Self::WorkspaceCreate => "workspace_create",
            Self::WorkspaceManage => "workspace_manage",
            Self::ThreadCreatePrivate => "thread_create_private",
            Self::ThreadCreateWorkspace => "thread_create_workspace",
            Self::ThreadRead => "thread_read",
            Self::MessageCreate => "message_create",
            Self::MessageEditOwn => "message_edit_own",
            Self::MessageDeleteOwn => "message_delete_own",
            Self::AgentTurnStart => "agent_turn_start",
            Self::AgentExecutionObserve => "agent_execution_observe",
            Self::AgentExecutionCancel => "agent_execution_cancel",
            Self::AgentExecutionResume => "agent_execution_resume",
            Self::AgentExecutionSteer => "agent_execution_steer",
            Self::AgentRequestObserve => "agent_request_observe",
            Self::AgentRequestRespond => "agent_request_respond",
            Self::ChildObserve => "child_observe",
            Self::ChildWrite => "child_write",
            Self::ChildStart => "child_start",
            Self::ChildControl => "child_control",
            Self::ChildRespond => "child_respond",
            Self::ChildTaskCreate => "child_task_create",
            Self::ChildArtifactRead => "child_artifact_read",
            Self::ChildArtifactWrite => "child_artifact_write",
            Self::AgentsDocumentRead => "agents_document_read",
            Self::AgentsDocumentManage => "agents_document_manage",
            Self::ThreadManage => "thread_manage",
            Self::ThreadMove => "thread_move",
            Self::ThreadParticipantsManage => "thread_participants_manage",
            Self::ArtifactRead => "artifact_read",
            Self::ArtifactCreateThread => "artifact_create_thread",
            Self::ArtifactBindThread => "artifact_bind_thread",
            Self::ArtifactDeleteThread => "artifact_delete_thread",
            Self::ArtifactDeleteOwn => "artifact_delete_own",
            Self::ArtifactManageWorkspace => "artifact_manage_workspace",
            Self::ArtifactDelete => "artifact_delete",
            Self::MemoryRead => "memory_read",
            Self::MemoryCreateThread => "memory_create_thread",
            Self::MemoryUpdateThread => "memory_update_thread",
            Self::MemoryForgetThread => "memory_forget_thread",
            Self::MemoryForgetWorkspace => "memory_forget_workspace",
            Self::MemoryModerateWorkspace => "memory_moderate_workspace",
            Self::TaskRead => "task_read",
            Self::TaskReadOperator => "task_read_operator",
            Self::TaskCreate => "task_create",
            Self::TaskReview => "task_review",
            Self::TaskCancel => "task_cancel",
            Self::TaskScheduleManage => "task_schedule_manage",
            Self::TaskDetach => "task_detach",
            Self::ProviderDiscover => "provider_discover",
            Self::ProviderUse => "provider_use",
            Self::ProviderManage => "provider_manage",
            Self::McpDiscover => "mcp_discover",
            Self::McpUse => "mcp_use",
            Self::McpReadOperator => "mcp_read_operator",
            Self::McpManage => "mcp_manage",
            Self::SkillDiscover => "skill_discover",
            Self::SkillUse => "skill_use",
            Self::SkillManage => "skill_manage",
            Self::CliRuntimeDiscover => "cli_runtime_discover",
            Self::CliRuntimeUse => "cli_runtime_use",
            Self::CliRuntimeReadOperator => "cli_runtime_read_operator",
            Self::CliRuntimeControl => "cli_runtime_control",
            Self::CliThreadFork => "cli_thread_fork",
            Self::CliRuntimeManage => "cli_runtime_manage",
            Self::SessionReadOwn => "session_read_own",
            Self::SessionRevokeOwn => "session_revoke_own",
            Self::ProfileUpdateOwn => "profile_update_own",
            Self::NotificationReadOwn => "notification_read_own",
            Self::NotificationAcknowledgeOwn => "notification_acknowledge_own",
            Self::InvitationCreate => "invitation_create",
            Self::InvitationList => "invitation_list",
            Self::InvitationRevoke => "invitation_revoke",
            Self::MemberDirectoryList => "member_directory_list",
            Self::MemberAvatarRead => "member_avatar_read",
            Self::WorkspaceMemberList => "workspace_member_list",
            Self::WorkspaceMemberAdd => "workspace_member_add",
            Self::WorkspaceMemberRemove => "workspace_member_remove",
            Self::MemberSuspend => "member_suspend",
            Self::MemberRestore => "member_restore",
            Self::MemberDeviceCreate => "member_device_create",
            Self::MemberRemove => "member_remove",
        }
    }

    pub(crate) fn from_safe_name(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|action| action.safe_name() == value)
    }
}

/// Maps an operation on an internal execution child to the independent
/// collaboration capability that must additionally be granted by the role.
pub(crate) const fn execution_child_policy_action(
    action: ResourceAction,
) -> Option<ResourceAction> {
    match action {
        ResourceAction::ThreadRead
        | ResourceAction::AgentExecutionObserve
        | ResourceAction::AgentRequestObserve
        | ResourceAction::TaskRead
        | ResourceAction::TaskReadOperator
        | ResourceAction::CliRuntimeReadOperator => Some(ResourceAction::ChildObserve),
        ResourceAction::MessageCreate
        | ResourceAction::MemoryRead
        | ResourceAction::MemoryCreateThread
        | ResourceAction::MemoryUpdateThread
        | ResourceAction::MemoryForgetThread => Some(ResourceAction::ChildWrite),
        ResourceAction::AgentTurnStart
        | ResourceAction::ProviderUse
        | ResourceAction::McpUse
        | ResourceAction::SkillUse => Some(ResourceAction::ChildStart),
        ResourceAction::AgentExecutionCancel
        | ResourceAction::AgentExecutionResume
        | ResourceAction::AgentExecutionSteer
        | ResourceAction::CliRuntimeUse
        | ResourceAction::CliRuntimeControl
        | ResourceAction::CliThreadFork => Some(ResourceAction::ChildControl),
        ResourceAction::AgentRequestRespond => Some(ResourceAction::ChildRespond),
        ResourceAction::TaskCreate => Some(ResourceAction::ChildTaskCreate),
        ResourceAction::ArtifactRead => Some(ResourceAction::ChildArtifactRead),
        ResourceAction::ArtifactCreateThread | ResourceAction::ArtifactBindThread => {
            Some(ResourceAction::ChildArtifactWrite)
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CapabilityKind {
    McpServer,
    Skill,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AuthorizationResource {
    WorkspaceCollection(GatewayId),
    Workspace(WorkspaceResourceId),
    Thread {
        workspace_id: WorkspaceResourceId,
        thread_id: ThreadResourceId,
    },
    Turn {
        workspace_id: WorkspaceResourceId,
        thread_id: ThreadResourceId,
        turn_id: TurnResourceId,
    },
    Artifact {
        workspace_id: WorkspaceResourceId,
        thread_id: Option<ThreadResourceId>,
        artifact_id: ArtifactResourceId,
    },
    Task {
        workspace_id: WorkspaceResourceId,
        root_thread_id: Option<ThreadResourceId>,
        task_id: TaskResourceId,
    },
    Session {
        principal_id: PrincipalId,
        session_id: AuthSessionId,
    },
    Capability {
        workspace_id: WorkspaceResourceId,
        kind: CapabilityKind,
        id: CapabilityResourceId,
    },
    AgentsDocument {
        workspace_id: WorkspaceResourceId,
        folder_id: Option<AgentsDocumentResourceId>,
        revision: Option<i64>,
    },
    InvitationGrantSet(Vec<WorkspaceResourceId>),
    InvitationCollection(GatewayId),
    Invitation(pioneer_protocol::InvitationId),
    MemberDirectory(GatewayId),
    DirectoryPrincipal(PrincipalId),
    MemberPrincipal(PrincipalId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AllowReason {
    ScopedCollection,
    ActiveWorkspaceMember,
    PrivateThreadParticipant,
    WorkspaceThreadMember,
    ThreadCreator,
    OwnSession,
    CapabilityProjected,
    InvitationGrantSet,
    InvitationCreator,
    DirectoryVisible,
}

impl AllowReason {
    pub(crate) const fn safe_name(self) -> &'static str {
        match self {
            Self::ScopedCollection => "scoped_collection",
            Self::ActiveWorkspaceMember => "active_workspace_member",
            Self::PrivateThreadParticipant => "private_thread_participant",
            Self::WorkspaceThreadMember => "workspace_thread_member",
            Self::ThreadCreator => "thread_creator",
            Self::OwnSession => "own_session",
            Self::CapabilityProjected => "capability_projected",
            Self::InvitationGrantSet => "invitation_grant_set",
            Self::InvitationCreator => "invitation_creator",
            Self::DirectoryVisible => "directory_visible",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DenyReason {
    InactivePrincipal,
    UnsupportedRole,
    NoWorkspaceMembership,
    NoPrivateThreadMembership,
    NotThreadCreator,
    ManagementDenied,
    CapabilityDisabled,
    ResourceScopeMismatch,
    MissingAuthoritativeResource,
    StaleAuthorizationRevision,
}

impl DenyReason {
    pub(crate) const fn safe_name(self) -> &'static str {
        match self {
            Self::InactivePrincipal => "inactive_principal",
            Self::UnsupportedRole => "unsupported_role",
            Self::NoWorkspaceMembership => "no_workspace_membership",
            Self::NoPrivateThreadMembership => "no_private_thread_membership",
            Self::NotThreadCreator => "not_thread_creator",
            Self::ManagementDenied => "management_denied",
            Self::CapabilityDisabled => "capability_disabled",
            Self::ResourceScopeMismatch => "resource_scope_mismatch",
            Self::MissingAuthoritativeResource => "missing_authoritative_resource",
            Self::StaleAuthorizationRevision => "stale_authorization_revision",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisclosurePolicy {
    NotFound,
    Forbidden,
    AuthenticationTerminal,
    Validation,
}

impl DisclosurePolicy {
    #[cfg(test)]
    pub(crate) const fn safe_name(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::Forbidden => "forbidden",
            Self::AuthenticationTerminal => "authentication_terminal",
            Self::Validation => "validation",
        }
    }
}

/// Result of the role/action gate.
///
/// `RequireResource` is deliberately not a final authorization grant. It
/// means only that the role may attempt this action and that an exact,
/// server-resolved resource decision is still required.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ActionGateDecision {
    AllowAbsolute,
    RequireResource {
        role: RoleKey,
    },
    Deny {
        reason: DenyReason,
        disclosure: DisclosurePolicy,
    },
}

impl ActionGateDecision {
    pub(crate) const fn permits_resource_resolution(&self) -> bool {
        matches!(self, Self::AllowAbsolute | Self::RequireResource { .. })
    }

    pub(crate) const fn is_final_allow(&self) -> bool {
        matches!(self, Self::AllowAbsolute)
    }

    #[cfg(test)]
    pub(crate) const fn safe_name(&self) -> &'static str {
        match self {
            Self::AllowAbsolute => "allow_absolute",
            Self::RequireResource { .. } => "require_resource",
            Self::Deny { .. } => "deny",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AuthorizationDecision {
    AllowAbsolute,
    AllowPolicy {
        role: RoleKey,
        reason: AllowReason,
    },
    Deny {
        reason: DenyReason,
        disclosure: DisclosurePolicy,
    },
}

impl AuthorizationDecision {
    pub(crate) const fn is_allowed(&self) -> bool {
        matches!(self, Self::AllowAbsolute | Self::AllowPolicy { .. })
    }

    pub(crate) const fn is_absolute(&self) -> bool {
        matches!(self, Self::AllowAbsolute)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ThreadAccessClass {
    Private,
    Workspace,
    Internal,
}

#[cfg(test)]
impl ThreadAccessClass {
    pub(crate) const ALL: [Self; 3] = [Self::Private, Self::Workspace, Self::Internal];

    pub(crate) const fn storage_value(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Workspace => "workspace",
            Self::Internal => "internal",
        }
    }

    pub(crate) fn from_storage_value(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|access_class| access_class.storage_value() == value)
    }
}

impl From<ThreadVisibility> for ThreadAccessClass {
    fn from(value: ThreadVisibility) -> Self {
        match value {
            ThreadVisibility::Private => Self::Private,
            ThreadVisibility::Workspace => Self::Workspace,
        }
    }
}

impl TryFrom<ThreadAccessClass> for ThreadVisibility {
    type Error = UserSelectableAccessClassError;

    fn try_from(value: ThreadAccessClass) -> Result<Self, Self::Error> {
        match value {
            ThreadAccessClass::Private => Ok(Self::Private),
            ThreadAccessClass::Workspace => Ok(Self::Workspace),
            ThreadAccessClass::Internal => Err(UserSelectableAccessClassError),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UserSelectableAccessClassError;

impl fmt::Display for UserSelectableAccessClassError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("internal thread access class is not user-selectable")
    }
}

impl std::error::Error for UserSelectableAccessClassError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_resource_action_has_a_unique_round_trip_name() {
        let names = ResourceAction::ALL
            .into_iter()
            .map(ResourceAction::safe_name)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), ResourceAction::ALL.len());
        for action in ResourceAction::ALL {
            assert_eq!(
                ResourceAction::from_safe_name(action.safe_name()),
                Some(action)
            );
        }
        assert_eq!(ResourceAction::from_safe_name("unknown"), None);
    }

    #[test]
    fn authorization_reason_names_are_bounded_and_exhaustive() {
        let allow_reasons = [
            AllowReason::ScopedCollection,
            AllowReason::ActiveWorkspaceMember,
            AllowReason::PrivateThreadParticipant,
            AllowReason::WorkspaceThreadMember,
            AllowReason::ThreadCreator,
            AllowReason::OwnSession,
            AllowReason::CapabilityProjected,
        ];
        let deny_reasons = [
            DenyReason::InactivePrincipal,
            DenyReason::UnsupportedRole,
            DenyReason::NoWorkspaceMembership,
            DenyReason::NoPrivateThreadMembership,
            DenyReason::NotThreadCreator,
            DenyReason::ManagementDenied,
            DenyReason::CapabilityDisabled,
            DenyReason::ResourceScopeMismatch,
            DenyReason::MissingAuthoritativeResource,
            DenyReason::StaleAuthorizationRevision,
        ];
        let disclosures = [
            DisclosurePolicy::NotFound,
            DisclosurePolicy::Forbidden,
            DisclosurePolicy::AuthenticationTerminal,
            DisclosurePolicy::Validation,
        ];

        for safe_name in allow_reasons
            .into_iter()
            .map(AllowReason::safe_name)
            .chain(deny_reasons.into_iter().map(DenyReason::safe_name))
            .chain(disclosures.into_iter().map(DisclosurePolicy::safe_name))
        {
            assert!(!safe_name.is_empty());
            assert!(safe_name.len() <= 64);
            assert!(
                safe_name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            );
        }
    }

    #[test]
    fn persisted_access_class_round_trips_and_internal_is_not_selectable() {
        for access_class in ThreadAccessClass::ALL {
            assert_eq!(
                ThreadAccessClass::from_storage_value(access_class.storage_value()),
                Some(access_class)
            );
        }
        assert_eq!(ThreadAccessClass::from_storage_value("unknown"), None);
        assert_eq!(
            ThreadVisibility::try_from(ThreadAccessClass::Private).unwrap(),
            ThreadVisibility::Private
        );
        assert!(ThreadVisibility::try_from(ThreadAccessClass::Internal).is_err());
    }

    #[test]
    fn authorization_resource_id_is_bounded_and_log_safe() {
        assert_eq!(
            ThreadResourceId::new("thread-1").unwrap().as_str(),
            "thread-1"
        );
        assert!(ThreadResourceId::new("").is_err());
        assert!(ThreadResourceId::new("thread\nprivate").is_err());
        assert!(ThreadResourceId::new("x".repeat(RESOURCE_ID_MAX_LEN + 1)).is_err());
    }
}
