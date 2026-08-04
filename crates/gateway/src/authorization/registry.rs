use std::sync::OnceLock;

use pioneer_protocol::constants::methods::*;

use super::{DisclosurePolicy, ResourceAction};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ResourceResolverKind {
    Gateway,
    WorkspaceCollection,
    Workspace,
    Thread,
    Turn,
    Artifact,
    Task,
    Capability,
    OwnSession,
    InvitationGrantSet,
    InvitationCollection,
    Invitation,
    MemberDirectory,
    MemberPrincipal,
}

impl ResourceResolverKind {
    pub(crate) const fn safe_name(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::WorkspaceCollection => "workspace_collection",
            Self::Workspace => "workspace",
            Self::Thread => "thread",
            Self::Turn => "turn",
            Self::Artifact => "artifact",
            Self::Task => "task",
            Self::Capability => "capability",
            Self::OwnSession => "own_session",
            Self::InvitationGrantSet => "invitation_grant_set",
            Self::InvitationCollection => "invitation_collection",
            Self::Invitation => "invitation",
            Self::MemberDirectory => "member_directory",
            Self::MemberPrincipal => "member_principal",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AuthorizationAuditClass {
    Authentication,
    Read,
    Mutation,
    Execution,
    Management,
}

impl AuthorizationAuditClass {
    pub(crate) const fn safe_name(self) -> &'static str {
        match self {
            Self::Authentication => "authentication",
            Self::Read => "read",
            Self::Mutation => "mutation",
            Self::Execution => "execution",
            Self::Management => "management",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MethodAuthorizationEntry {
    pub(crate) method: &'static str,
    pub(crate) action: ResourceAction,
    pub(crate) resolver: ResourceResolverKind,
    pub(crate) disclosure: DisclosurePolicy,
    pub(crate) audit: AuthorizationAuditClass,
}

const fn method_entry(
    method: &'static str,
    action: ResourceAction,
    resolver: ResourceResolverKind,
    disclosure: DisclosurePolicy,
    audit: AuthorizationAuditClass,
) -> MethodAuthorizationEntry {
    MethodAuthorizationEntry {
        method,
        action,
        resolver,
        disclosure,
        audit,
    }
}

use AuthorizationAuditClass::{Authentication, Execution, Management, Mutation, Read};
use DisclosurePolicy::{Forbidden, NotFound};
use ResourceAction::{
    ArtifactDelete, ArtifactRead, ArtifactWrite, CliRuntimeManage, CliRuntimeUse, GatewayManage,
    InvitationCreate, InvitationList, InvitationRevoke, McpManage, MemberDeviceCreate,
    MemberDirectoryList, MemberRemove, MemberRestore, MemberSuspend, MemoryRead, MemoryWrite,
    ProviderManage, ProviderUse, SessionReadOwn, SessionRevokeOwn, SkillManage, SkillUse,
    TaskManage, TaskRead, TaskRun, ThreadCreate, ThreadManage, ThreadMove,
    ThreadParticipantsManage, ThreadRead, ThreadWrite, WorkspaceCreate, WorkspaceList,
    WorkspaceManage, WorkspaceMemberAdd, WorkspaceMemberList, WorkspaceMemberRemove, WorkspaceRead,
};
use ResourceResolverKind::{
    Artifact, Capability, Gateway, Invitation, InvitationCollection, InvitationGrantSet,
    MemberDirectory, MemberPrincipal, OwnSession, Task, Thread, Turn, Workspace,
    WorkspaceCollection,
};

pub(crate) static NORMAL_METHOD_REGISTRY: &[MethodAuthorizationEntry] = &[
    method_entry(
        INVITE_CREATE,
        InvitationCreate,
        InvitationGrantSet,
        NotFound,
        Management,
    ),
    method_entry(
        INVITE_LIST,
        InvitationList,
        InvitationCollection,
        NotFound,
        Read,
    ),
    method_entry(
        INVITE_REVOKE,
        InvitationRevoke,
        Invitation,
        NotFound,
        Management,
    ),
    method_entry(
        MEMBER_LIST,
        MemberDirectoryList,
        MemberDirectory,
        NotFound,
        Read,
    ),
    method_entry(
        MEMBER_SUSPEND,
        MemberSuspend,
        MemberPrincipal,
        NotFound,
        Management,
    ),
    method_entry(
        MEMBER_RESTORE,
        MemberRestore,
        MemberPrincipal,
        NotFound,
        Management,
    ),
    method_entry(
        MEMBER_DEVICE_CREATE,
        MemberDeviceCreate,
        MemberPrincipal,
        NotFound,
        Management,
    ),
    method_entry(
        MEMBER_REMOVE,
        MemberRemove,
        MemberPrincipal,
        NotFound,
        Management,
    ),
    method_entry(
        WORKSPACE_MEMBER_LIST,
        WorkspaceMemberList,
        Workspace,
        NotFound,
        Read,
    ),
    method_entry(
        WORKSPACE_MEMBER_ADD,
        WorkspaceMemberAdd,
        Workspace,
        NotFound,
        Management,
    ),
    method_entry(
        WORKSPACE_MEMBER_REMOVE,
        WorkspaceMemberRemove,
        Workspace,
        NotFound,
        Management,
    ),
    method_entry(
        AUTH_ME,
        SessionReadOwn,
        OwnSession,
        NotFound,
        Authentication,
    ),
    method_entry(
        AUTH_SESSION_LIST,
        SessionReadOwn,
        OwnSession,
        NotFound,
        Authentication,
    ),
    method_entry(
        AUTH_SESSION_REVOKE,
        SessionRevokeOwn,
        OwnSession,
        NotFound,
        Authentication,
    ),
    method_entry(
        AUTH_LOGOUT,
        SessionRevokeOwn,
        OwnSession,
        NotFound,
        Authentication,
    ),
    method_entry(
        AUTH_DEVICE_CREATE,
        SessionRevokeOwn,
        OwnSession,
        NotFound,
        Authentication,
    ),
    method_entry(
        WORKSPACE_LIST,
        WorkspaceList,
        WorkspaceCollection,
        NotFound,
        Read,
    ),
    method_entry(
        WORKSPACE_CREATE,
        WorkspaceCreate,
        Gateway,
        Forbidden,
        Management,
    ),
    method_entry(
        WORKSPACE_DEFAULT,
        WorkspaceRead,
        WorkspaceCollection,
        NotFound,
        Read,
    ),
    method_entry(
        WORKSPACE_SELECT,
        WorkspaceRead,
        Workspace,
        NotFound,
        Mutation,
    ),
    method_entry(
        WORKSPACE_UPDATE,
        WorkspaceManage,
        Workspace,
        NotFound,
        Management,
    ),
    method_entry(THREAD_START, ThreadCreate, Workspace, NotFound, Mutation),
    method_entry(THREAD_TREE, WorkspaceRead, Workspace, NotFound, Read),
    method_entry(THREAD_UPDATE, ThreadManage, Thread, NotFound, Mutation),
    method_entry(THREAD_MOVE, ThreadMove, Thread, NotFound, Management),
    method_entry(
        THREAD_PARTICIPANTS_LIST,
        ThreadParticipantsManage,
        Thread,
        NotFound,
        Read,
    ),
    method_entry(
        THREAD_PARTICIPANTS_ADD,
        ThreadParticipantsManage,
        Thread,
        NotFound,
        Mutation,
    ),
    method_entry(
        THREAD_PARTICIPANTS_REMOVE,
        ThreadParticipantsManage,
        Thread,
        NotFound,
        Mutation,
    ),
    method_entry(
        THREAD_FOLDER_CREATE,
        WorkspaceManage,
        Workspace,
        Forbidden,
        Management,
    ),
    method_entry(
        THREAD_FOLDER_MOVE,
        WorkspaceManage,
        Workspace,
        Forbidden,
        Management,
    ),
    method_entry(
        THREAD_FOLDER_DELETE,
        WorkspaceManage,
        Workspace,
        Forbidden,
        Management,
    ),
    method_entry(THREAD_AGENTS_DOC_GET, ThreadRead, Thread, NotFound, Read),
    method_entry(
        THREAD_AGENTS_DOC_SAVE,
        ThreadWrite,
        Thread,
        NotFound,
        Mutation,
    ),
    method_entry(
        THREAD_AGENTS_DOC_ARCHIVE,
        ThreadManage,
        Thread,
        NotFound,
        Mutation,
    ),
    method_entry(
        THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD,
        ThreadRead,
        Thread,
        NotFound,
        Read,
    ),
    method_entry(THREAD_GET, ThreadRead, Thread, NotFound, Read),
    method_entry(THREAD_TIMELINE_PAGE, ThreadRead, Thread, NotFound, Read),
    method_entry(THREAD_READ, ThreadRead, Thread, NotFound, Mutation),
    method_entry(THREAD_UNSUBSCRIBE, ThreadRead, Thread, NotFound, Mutation),
    method_entry(TURN_START, ThreadWrite, Thread, NotFound, Execution),
    method_entry(TURN_MESSAGE_EDIT, ThreadWrite, Turn, NotFound, Mutation),
    method_entry(TURN_MESSAGE_DELETE, ThreadWrite, Turn, NotFound, Mutation),
    method_entry(
        TURN_MESSAGE_REVISIONS_PAGE,
        ThreadRead,
        Turn,
        NotFound,
        Read,
    ),
    method_entry(TURN_CANCEL, ThreadWrite, Turn, NotFound, Mutation),
    method_entry(TURN_RESUME, ThreadWrite, Turn, NotFound, Execution),
    method_entry(TURN_GET, ThreadRead, Turn, NotFound, Read),
    method_entry(TURN_ITEMS, ThreadRead, Turn, NotFound, Read),
    method_entry(TURN_WORK_PAGE, ThreadRead, Turn, NotFound, Read),
    method_entry(TURN_WORK_ITEMS_GET, ThreadRead, Turn, NotFound, Read),
    method_entry(
        TURN_PERMISSION_REQUEST_RESPOND,
        ThreadWrite,
        Turn,
        NotFound,
        Execution,
    ),
    method_entry(VOICE_STATUS, ProviderUse, Capability, NotFound, Read),
    method_entry(
        VOICE_SESSION_START,
        ProviderUse,
        Capability,
        NotFound,
        Execution,
    ),
    method_entry(
        VOICE_SESSION_FINALIZE,
        ProviderUse,
        Capability,
        NotFound,
        Execution,
    ),
    method_entry(
        VOICE_SESSION_CANCEL,
        ProviderUse,
        Capability,
        NotFound,
        Mutation,
    ),
    method_entry(PROVIDER_LIST, ProviderUse, Capability, NotFound, Read),
    method_entry(
        PROVIDER_MODELS_LIST,
        ProviderUse,
        Capability,
        NotFound,
        Read,
    ),
    method_entry(
        PROVIDER_EMBEDDING_MODELS_LIST,
        ProviderUse,
        Capability,
        NotFound,
        Read,
    ),
    method_entry(
        PROVIDER_TRANSCRIPTION_MODELS_LIST,
        ProviderUse,
        Capability,
        NotFound,
        Read,
    ),
    method_entry(
        PROVIDER_CONFIGURE,
        ProviderManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        PROVIDER_SET_API_KEY,
        ProviderManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        PROVIDER_DELETE_API_KEY,
        ProviderManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_LIST,
        CliRuntimeManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_GET,
        CliRuntimeManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_STATUS,
        CliRuntimeManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_REFRESH,
        CliRuntimeManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_LIST_MODELS,
        CliRuntimeManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_THREAD_BINDING_GET,
        CliRuntimeUse,
        Thread,
        NotFound,
        Read,
    ),
    method_entry(
        CLI_RUNTIME_THREAD_FORK,
        CliRuntimeUse,
        Thread,
        NotFound,
        Execution,
    ),
    method_entry(
        CLI_RUNTIME_THREAD_COMPACT,
        CliRuntimeUse,
        Thread,
        NotFound,
        Execution,
    ),
    method_entry(
        CLI_RUNTIME_TURN_STEER,
        CliRuntimeUse,
        Turn,
        NotFound,
        Execution,
    ),
    method_entry(
        CLI_RUNTIME_REVIEW_START,
        CliRuntimeUse,
        Thread,
        NotFound,
        Execution,
    ),
    method_entry(
        CLI_RUNTIME_LOGIN_START,
        CliRuntimeManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_LOGIN_CANCEL,
        CliRuntimeManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_PROXY_SET,
        CliRuntimeManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_PROXY_DELETE,
        CliRuntimeManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        CLI_RUNTIME_REQUEST_RESPOND,
        CliRuntimeUse,
        Turn,
        NotFound,
        Execution,
    ),
    method_entry(SETTINGS_GET, GatewayManage, Gateway, Forbidden, Management),
    method_entry(
        SETTINGS_UPDATE,
        GatewayManage,
        Gateway,
        Forbidden,
        Management,
    ),
    method_entry(SKILLS_LIST, SkillUse, Workspace, NotFound, Read),
    method_entry(
        SKILLS_INSTALL,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_UPDATE,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_UNINSTALL,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_PACK_INSTALL,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_PACK_UPDATE,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_PACK_UNINSTALL,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_HEALTH,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_UPLOAD_START,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_UPLOAD_FINISH,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_UPLOAD_ABORT,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_POLICY_LIST,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(
        SKILLS_POLICY_SET,
        SkillManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(MCP_LIST, McpManage, Capability, Forbidden, Management),
    method_entry(MCP_INSTALL, McpManage, Capability, Forbidden, Management),
    method_entry(MCP_POLICY_SET, McpManage, Capability, Forbidden, Management),
    method_entry(
        MCP_SERVER_RESTART,
        McpManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(MCP_UNINSTALL, McpManage, Capability, Forbidden, Management),
    method_entry(
        MCP_SERVER_DETAILS,
        McpManage,
        Capability,
        Forbidden,
        Management,
    ),
    method_entry(TASK_CREATE, TaskRun, Thread, NotFound, Execution),
    method_entry(TASK_GET, TaskRead, Task, NotFound, Read),
    method_entry(TASK_LIST, TaskRead, Task, NotFound, Read),
    method_entry(TASK_TREE, TaskRead, Task, NotFound, Read),
    method_entry(TASK_EVENTS, TaskRead, Task, NotFound, Read),
    method_entry(TASK_WAIT, TaskRead, Task, NotFound, Read),
    method_entry(TASK_ACCEPT, TaskManage, Task, NotFound, Mutation),
    method_entry(TASK_REVISE, TaskManage, Task, NotFound, Mutation),
    method_entry(TASK_CANCEL, TaskManage, Task, NotFound, Mutation),
    method_entry(TASK_RESCHEDULE, TaskManage, Task, NotFound, Mutation),
    method_entry(TASK_DETACH, TaskManage, Task, NotFound, Mutation),
    method_entry(TASK_PAUSE, TaskManage, Task, NotFound, Mutation),
    method_entry(TASK_RESUME, TaskManage, Task, NotFound, Execution),
    method_entry(TASK_AGENDA, TaskRead, Task, NotFound, Read),
    method_entry(TASK_DELIVERIES, TaskRead, Task, NotFound, Read),
    method_entry(MEMORY_SEARCH, MemoryRead, Workspace, NotFound, Read),
    method_entry(MEMORY_GET, MemoryRead, Workspace, NotFound, Read),
    method_entry(MEMORY_LIST, MemoryRead, Workspace, NotFound, Read),
    method_entry(MEMORY_REMEMBER, MemoryWrite, Workspace, NotFound, Mutation),
    method_entry(MEMORY_FORGET, MemoryWrite, Workspace, NotFound, Mutation),
    method_entry(
        MEMORY_CANDIDATES_LIST,
        MemoryRead,
        Workspace,
        NotFound,
        Read,
    ),
    method_entry(MEMORY_CANDIDATES_GET, MemoryRead, Workspace, NotFound, Read),
    method_entry(
        MEMORY_CANDIDATES_DECIDE,
        MemoryWrite,
        Workspace,
        NotFound,
        Mutation,
    ),
    method_entry(
        MEMORY_CANDIDATES_APPROVE,
        MemoryWrite,
        Workspace,
        NotFound,
        Mutation,
    ),
    method_entry(
        MEMORY_CANDIDATES_REJECT,
        MemoryWrite,
        Workspace,
        NotFound,
        Mutation,
    ),
    method_entry(
        MEMORY_CANDIDATES_EDIT_AND_APPROVE,
        MemoryWrite,
        Workspace,
        NotFound,
        Mutation,
    ),
    method_entry(
        MEMORY_CANDIDATES_MERGE,
        MemoryWrite,
        Workspace,
        NotFound,
        Mutation,
    ),
    method_entry(
        MEMORY_CANDIDATES_SUPPRESS_SIMILAR,
        MemoryWrite,
        Workspace,
        NotFound,
        Mutation,
    ),
    method_entry(
        ARTIFACT_CAPABILITIES,
        ArtifactRead,
        Workspace,
        NotFound,
        Read,
    ),
    method_entry(ARTIFACT_LIST, ArtifactRead, Workspace, NotFound, Read),
    method_entry(
        ARTIFACT_LIST_FOR_THREAD,
        ArtifactRead,
        Thread,
        NotFound,
        Read,
    ),
    method_entry(ARTIFACT_LIST_FOR_TURN, ArtifactRead, Turn, NotFound, Read),
    method_entry(
        ARTIFACT_LIST_FOR_MESSAGE,
        ArtifactRead,
        Thread,
        NotFound,
        Read,
    ),
    method_entry(ARTIFACT_GET, ArtifactRead, Artifact, NotFound, Read),
    method_entry(
        ARTIFACT_VIEW_GRANT_CREATE,
        ArtifactRead,
        Artifact,
        NotFound,
        Read,
    ),
    method_entry(
        ARTIFACT_DELETE,
        ArtifactDelete,
        Artifact,
        NotFound,
        Management,
    ),
    method_entry(
        ARTIFACT_RESTORE,
        ArtifactDelete,
        Artifact,
        NotFound,
        Management,
    ),
    method_entry(ARTIFACT_BIND, ArtifactWrite, Artifact, NotFound, Mutation),
    method_entry(
        ARTIFACT_UPLOAD_START,
        ArtifactWrite,
        Artifact,
        NotFound,
        Mutation,
    ),
    method_entry(
        ARTIFACT_UPLOAD_FINISH,
        ArtifactWrite,
        Artifact,
        NotFound,
        Mutation,
    ),
    method_entry(
        ARTIFACT_UPLOAD_ABORT,
        ArtifactWrite,
        Artifact,
        NotFound,
        Mutation,
    ),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BinaryIngressKind {
    ArtifactUploadChunk,
    SkillUploadChunk,
    VoiceChunk,
}

impl BinaryIngressKind {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 3] = [
        Self::ArtifactUploadChunk,
        Self::SkillUploadChunk,
        Self::VoiceChunk,
    ];

    pub(crate) const fn safe_name(self) -> &'static str {
        match self {
            Self::ArtifactUploadChunk => "artifact/upload/chunk",
            Self::SkillUploadChunk => "skills/upload/chunk",
            Self::VoiceChunk => "voice/chunk",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BinaryAuthorizationEntry {
    pub(crate) kind: BinaryIngressKind,
    pub(crate) action: ResourceAction,
    pub(crate) resolver: ResourceResolverKind,
    pub(crate) disclosure: DisclosurePolicy,
    pub(crate) audit: AuthorizationAuditClass,
    pub(crate) reauthorize_each_frame: bool,
}

pub(crate) static BINARY_INGRESS_REGISTRY: &[BinaryAuthorizationEntry] = &[
    BinaryAuthorizationEntry {
        kind: BinaryIngressKind::ArtifactUploadChunk,
        action: ResourceAction::ArtifactWrite,
        resolver: ResourceResolverKind::Artifact,
        disclosure: DisclosurePolicy::NotFound,
        audit: AuthorizationAuditClass::Mutation,
        reauthorize_each_frame: true,
    },
    BinaryAuthorizationEntry {
        kind: BinaryIngressKind::SkillUploadChunk,
        action: ResourceAction::SkillManage,
        resolver: ResourceResolverKind::Capability,
        disclosure: DisclosurePolicy::Forbidden,
        audit: AuthorizationAuditClass::Management,
        reauthorize_each_frame: true,
    },
    BinaryAuthorizationEntry {
        kind: BinaryIngressKind::VoiceChunk,
        action: ResourceAction::ProviderUse,
        resolver: ResourceResolverKind::Capability,
        disclosure: DisclosurePolicy::NotFound,
        audit: AuthorizationAuditClass::Execution,
        reauthorize_each_frame: true,
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RegistryLookupError {
    InvalidDefinition,
    Unmapped,
}

static NORMAL_METHOD_REGISTRY_VALID: OnceLock<bool> = OnceLock::new();
static BINARY_INGRESS_REGISTRY_VALID: OnceLock<bool> = OnceLock::new();

pub(crate) fn normal_method_entry(
    method: &str,
) -> Result<&'static MethodAuthorizationEntry, RegistryLookupError> {
    if !*NORMAL_METHOD_REGISTRY_VALID
        .get_or_init(|| validate_method_registry(NORMAL_METHOD_REGISTRY).is_ok())
    {
        return Err(RegistryLookupError::InvalidDefinition);
    }
    NORMAL_METHOD_REGISTRY
        .iter()
        .find(|entry| entry.method == method)
        .ok_or(RegistryLookupError::Unmapped)
}

pub(crate) fn binary_ingress_entry(
    kind: BinaryIngressKind,
) -> Result<&'static BinaryAuthorizationEntry, RegistryLookupError> {
    if !*BINARY_INGRESS_REGISTRY_VALID
        .get_or_init(|| validate_binary_registry(BINARY_INGRESS_REGISTRY).is_ok())
    {
        return Err(RegistryLookupError::InvalidDefinition);
    }
    BINARY_INGRESS_REGISTRY
        .iter()
        .find(|entry| entry.kind == kind)
        .ok_or(RegistryLookupError::Unmapped)
}

fn validate_method_registry(entries: &[MethodAuthorizationEntry]) -> Result<(), ()> {
    for (index, entry) in entries.iter().enumerate() {
        if !is_canonical_method(entry.method)
            || entries[..index]
                .iter()
                .any(|previous| previous.method == entry.method)
        {
            return Err(());
        }
    }
    Ok(())
}

fn validate_binary_registry(entries: &[BinaryAuthorizationEntry]) -> Result<(), ()> {
    for (index, entry) in entries.iter().enumerate() {
        if entries[..index]
            .iter()
            .any(|previous| previous.kind == entry.kind)
        {
            return Err(());
        }
    }
    Ok(())
}

fn is_canonical_method(method: &str) -> bool {
    !method.is_empty()
        && method.len() <= 128
        && method.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'/' | b'_' | b'-' | b'.')
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use pioneer_protocol::constants::methods;

    use super::*;

    #[test]
    fn normal_registry_exactly_matches_the_authoritative_protocol_method_set() {
        validate_method_registry(NORMAL_METHOD_REGISTRY).expect("valid method registry");
        let registry = NORMAL_METHOD_REGISTRY
            .iter()
            .map(|entry| entry.method)
            .collect::<HashSet<_>>();
        let protocol = methods::NORMAL_METHODS
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        assert_eq!(NORMAL_METHOD_REGISTRY.len(), registry.len());
        assert_eq!(methods::NORMAL_METHODS.len(), protocol.len());
        assert_eq!(registry, protocol);
        assert_eq!(registry.len(), 138);
        for entry in NORMAL_METHOD_REGISTRY {
            assert_eq!(normal_method_entry(entry.method), Ok(entry));
            assert!(!entry.action.safe_name().is_empty());
            assert!(!entry.resolver.safe_name().is_empty());
            assert!(!entry.disclosure.safe_name().is_empty());
            assert!(!entry.audit.safe_name().is_empty());
        }
    }

    #[test]
    fn turn_message_operations_use_the_existing_turn_resolver() {
        for (method, action, audit) in [
            (
                methods::TURN_MESSAGE_EDIT,
                ResourceAction::ThreadWrite,
                AuthorizationAuditClass::Mutation,
            ),
            (
                methods::TURN_MESSAGE_DELETE,
                ResourceAction::ThreadWrite,
                AuthorizationAuditClass::Mutation,
            ),
            (
                methods::TURN_MESSAGE_REVISIONS_PAGE,
                ResourceAction::ThreadRead,
                AuthorizationAuditClass::Read,
            ),
        ] {
            let entry = normal_method_entry(method).expect("registered Turn message method");
            assert_eq!(entry.action, action);
            assert_eq!(entry.resolver, ResourceResolverKind::Turn);
            assert_eq!(entry.disclosure, DisclosurePolicy::NotFound);
            assert_eq!(entry.audit, audit);
        }
    }

    #[test]
    fn restricted_auth_methods_are_exact_and_never_normal() {
        let restricted = methods::RESTRICTED_AUTH_METHODS
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        assert_eq!(restricted.len(), 4);
        assert_eq!(
            methods::RESTRICTED_AUTH_METHODS,
            &[
                methods::AUTH_REFRESH,
                methods::AUTH_DEVICE_ACTIVATE,
                methods::INVITE_PREVIEW,
                methods::INVITE_ACCEPT,
            ]
        );
        for method in restricted {
            assert_eq!(
                normal_method_entry(method),
                Err(RegistryLookupError::Unmapped)
            );
        }
    }

    #[test]
    fn shared_folder_mutations_are_explicit_workspace_management() {
        for method in [
            methods::THREAD_FOLDER_CREATE,
            methods::THREAD_FOLDER_MOVE,
            methods::THREAD_FOLDER_DELETE,
        ] {
            let entry = normal_method_entry(method).expect("registered folder mutation");
            assert_eq!(entry.action, ResourceAction::WorkspaceManage);
            assert_eq!(entry.resolver, ResourceResolverKind::Workspace);
            assert_eq!(entry.disclosure, DisclosurePolicy::Forbidden);
            assert_eq!(entry.audit, AuthorizationAuditClass::Management);
        }
    }

    #[test]
    fn member_discovery_methods_resolve_the_requested_workspace() {
        for (method, action) in [
            (methods::ARTIFACT_CAPABILITIES, ResourceAction::ArtifactRead),
            (methods::SKILLS_LIST, ResourceAction::SkillUse),
        ] {
            let entry = normal_method_entry(method).expect("registered discovery method");
            assert_eq!(entry.action, action);
            assert_eq!(entry.resolver, ResourceResolverKind::Workspace);
            assert_eq!(entry.disclosure, DisclosurePolicy::NotFound);
            assert_eq!(entry.audit, AuthorizationAuditClass::Read);
        }
    }

    #[test]
    fn stale_and_unknown_method_names_fail_closed() {
        for method in [
            methods::TASK_UPDATE,
            "artifact/read",
            "artifact/download/start",
            "artifact/download/chunk",
            "artifact/download/finish",
            "artifact/download/abort",
            "future/unregistered",
            "workspace/list\nforged",
        ] {
            assert_eq!(
                normal_method_entry(method),
                Err(RegistryLookupError::Unmapped),
                "legacy or unknown method {method} must fail closed",
            );
        }
    }

    #[test]
    fn duplicate_method_definitions_are_rejected() {
        let duplicate = [
            method_entry(
                methods::WORKSPACE_LIST,
                ResourceAction::WorkspaceList,
                ResourceResolverKind::WorkspaceCollection,
                DisclosurePolicy::NotFound,
                AuthorizationAuditClass::Read,
            ),
            method_entry(
                methods::WORKSPACE_LIST,
                ResourceAction::WorkspaceRead,
                ResourceResolverKind::Workspace,
                DisclosurePolicy::NotFound,
                AuthorizationAuditClass::Read,
            ),
        ];
        assert_eq!(validate_method_registry(&duplicate), Err(()));
    }

    #[test]
    fn binary_registry_is_exact_unique_and_reauthorizes_every_frame() {
        validate_binary_registry(BINARY_INGRESS_REGISTRY).expect("valid binary registry");
        let registry = BINARY_INGRESS_REGISTRY
            .iter()
            .map(|entry| entry.kind)
            .collect::<HashSet<_>>();
        let public = BinaryIngressKind::ALL.into_iter().collect::<HashSet<_>>();

        assert_eq!(BINARY_INGRESS_REGISTRY.len(), registry.len());
        assert_eq!(registry, public);
        assert_eq!(
            BinaryIngressKind::ALL.map(BinaryIngressKind::safe_name),
            [
                "artifact/upload/chunk",
                "skills/upload/chunk",
                "voice/chunk"
            ],
        );
        for kind in BinaryIngressKind::ALL {
            let entry = binary_ingress_entry(kind).expect("registered binary ingress");
            assert!(entry.reauthorize_each_frame);
            assert_eq!(entry.kind.safe_name(), kind.safe_name());
            assert!(!entry.action.safe_name().is_empty());
            assert!(!entry.resolver.safe_name().is_empty());
            assert!(!entry.disclosure.safe_name().is_empty());
            assert!(!entry.audit.safe_name().is_empty());
        }
    }

    #[test]
    fn duplicate_binary_definitions_are_rejected() {
        let duplicate = [BINARY_INGRESS_REGISTRY[0], BINARY_INGRESS_REGISTRY[0]];
        assert_eq!(validate_binary_registry(&duplicate), Err(()));
    }
}
