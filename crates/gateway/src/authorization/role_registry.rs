use pioneer_protocol::{
    AuthorizationInvitationRoleOption, AuthorizationRolePresentation, MEMBER_ROLE_KEY,
    McpInvocationResourceLimits, PrincipalKind, RoleKey, SUPERUSER_CAPABILITY_ROLE_KEY,
    TaskResourceBudget, TurnApprovalScopePolicySnapshot, TurnPermissionMode,
    TurnPermissionProfileCap,
};
use sha2::{Digest, Sha256};

use pioneer_cli_agent_runtime::NativeEventBudget;
use pioneer_crud::{ExecutionAdmissionQuotaPolicy, ExecutionQuotaCeilings};

use crate::human_interaction::HumanInteractionBudget;

use super::ResourceAction;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RoleActionPolicy {
    All,
    Only(&'static [ResourceAction]),
}

impl RoleActionPolicy {
    pub(crate) fn allows(self, action: ResourceAction) -> bool {
        match self {
            Self::All => true,
            Self::Only(actions) => actions.contains(&action),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RoleResourcePolicy {
    Absolute,
    ScopedCollaboration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OperationalResourcePolicy {
    pub(crate) providers: Option<&'static [&'static str]>,
    pub(crate) provider_models: Option<&'static [(&'static str, &'static str)]>,
    pub(crate) cli_runtimes: Option<&'static [&'static str]>,
    pub(crate) cli_models: Option<&'static [(&'static str, &'static str)]>,
    pub(crate) skills: Option<&'static [&'static str]>,
    pub(crate) mcp_servers: Option<&'static [&'static str]>,
}

impl OperationalResourcePolicy {
    pub(crate) const ALL: Self = Self {
        providers: None,
        provider_models: None,
        cli_runtimes: None,
        cli_models: None,
        skills: None,
        mcp_servers: None,
    };

    pub(crate) fn provider_allowed(self, provider: &str) -> bool {
        self.providers
            .is_none_or(|allowed| allowed.contains(&provider))
    }

    pub(crate) fn provider_model_allowed(self, provider: &str, model: &str) -> bool {
        self.provider_allowed(provider)
            && self
                .provider_models
                .is_none_or(|allowed| allowed.contains(&(provider, model)))
    }

    pub(crate) fn cli_runtime_allowed(self, runtime_id: &str) -> bool {
        self.cli_runtimes
            .is_none_or(|allowed| allowed.contains(&runtime_id))
    }

    pub(crate) fn cli_model_allowed(self, runtime_id: &str, model: &str) -> bool {
        self.cli_runtime_allowed(runtime_id)
            && self
                .cli_models
                .is_none_or(|allowed| allowed.contains(&(runtime_id, model)))
    }

    pub(crate) fn skill_allowed(self, skill_id: &str) -> bool {
        self.skills
            .is_none_or(|allowed| allowed.contains(&skill_id))
    }

    pub(crate) fn mcp_server_allowed(self, server_id: &str) -> bool {
        self.mcp_servers
            .is_none_or(|allowed| allowed.contains(&server_id))
    }
}

/// Role-owned operational traits consumed by execution code. Runtime code
/// branches on these traits, never on `PrincipalKind` or a role name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuntimePrincipalPolicy {
    Absolute,
    ScopedCollaboration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RoleDisclosurePolicy {
    Administrative,
    Collaborator,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RolePresentation {
    pub(crate) display_name: &'static str,
    pub(crate) description: &'static str,
    pub(crate) built_in: bool,
}

/// Role-owned budget for bounded operational reads. These limits are enforced
/// by the Gateway independently from action authorization, so a future
/// observer role can receive narrower replay resources without handler-level
/// role branches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ObservationResourcePolicy {
    pub(crate) max_turn_page_items: u32,
    pub(crate) max_turn_page_bytes: usize,
    pub(crate) max_concurrent_pages_per_principal: usize,
    pub(crate) max_concurrent_pages_per_role: usize,
    pub(crate) max_concurrent_pages_per_workspace: usize,
}

/// Complete server-owned definition of a code-defined authorization role.
///
/// The protocol validates only `RoleKey` syntax. Support, actions, execution
/// caps and lifecycle/projection traits are resolved exclusively here.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RoleDefinition {
    pub(crate) key: &'static str,
    pub(crate) principal_kind: PrincipalKind,
    pub(crate) actions: RoleActionPolicy,
    pub(crate) resources: RoleResourcePolicy,
    pub(crate) operational_resources: OperationalResourcePolicy,
    pub(crate) permission_cap: fn() -> TurnPermissionProfileCap,
    /// Maximum consent scope this role may grant while collaborating on an
    /// existing execution. This is deliberately independent from
    /// `permission_cap`: the latter constrains executions started by the
    /// role, while this policy constrains approval decisions over any
    /// execution in the shared thread capsule.
    pub(crate) approval_scope_cap: fn() -> TurnApprovalScopePolicySnapshot,
    pub(crate) permission_presets: &'static [TurnPermissionMode],
    pub(crate) human_interaction_budget: HumanInteractionBudget,
    pub(crate) execution_resources: ExecutionAdmissionQuotaPolicy,
    pub(crate) observation_resources: ObservationResourcePolicy,
    pub(crate) task_resources: TaskResourceBudget,
    pub(crate) mcp_invocation_resources: McpInvocationResourceLimits,
    pub(crate) native_event_resources: NativeEventBudget,
    pub(crate) runtime_principal: RuntimePrincipalPolicy,
    pub(crate) disclosure: RoleDisclosurePolicy,
    pub(crate) invitation_assignable: bool,
    pub(crate) invitation_default: bool,
    pub(crate) lifecycle_managed: bool,
    pub(crate) presentation: RolePresentation,
}

const GATEWAY_ACTIVE_EXECUTION_LIMIT: u32 = 256;
const GATEWAY_QUEUED_EXECUTION_LIMIT: u32 = 2_048;
const GATEWAY_SCHEDULED_EXECUTION_LIMIT: u32 = 4_096;

const ALL_PERMISSION_PRESETS: &[TurnPermissionMode] = &[
    TurnPermissionMode::FullAccess,
    TurnPermissionMode::AutoAcceptEdits,
    TurnPermissionMode::Supervised,
];
const SUPERVISED_PERMISSION_PRESET: &[TurnPermissionMode] = &[TurnPermissionMode::Supervised];

fn full_access_permission_cap() -> TurnPermissionProfileCap {
    pioneer_protocol::task_permission_cap_for_mode(TurnPermissionMode::FullAccess)
}

fn supervised_permission_cap() -> TurnPermissionProfileCap {
    pioneer_protocol::task_permission_cap_for_mode(TurnPermissionMode::Supervised)
}

fn collaboration_approval_scope_cap() -> TurnApprovalScopePolicySnapshot {
    TurnApprovalScopePolicySnapshot::supervised()
}

#[cfg(test)]
fn no_approval_scope_cap() -> TurnApprovalScopePolicySnapshot {
    TurnApprovalScopePolicySnapshot::full_access()
}

#[cfg(test)]
fn synthetic_observer_permission_cap() -> TurnPermissionProfileCap {
    TurnPermissionProfileCap {
        mode: TurnPermissionMode::Supervised,
        effective_policy: pioneer_protocol::ToolPermissionPolicySnapshot {
            default_behavior: pioneer_protocol::PermissionBehavior::Deny,
            file_read: pioneer_protocol::PermissionBehavior::Allow,
            file_write: pioneer_protocol::PermissionBehavior::Deny,
            shell_command: pioneer_protocol::PermissionBehavior::Deny,
            network: pioneer_protocol::PermissionBehavior::Deny,
            mcp_read: pioneer_protocol::PermissionBehavior::Allow,
            mcp_write_or_unknown: pioneer_protocol::PermissionBehavior::Deny,
            dynamic_skill_tool: pioneer_protocol::PermissionBehavior::Deny,
            computer_use: pioneer_protocol::PermissionBehavior::Deny,
            task_subagent: pioneer_protocol::PermissionBehavior::Deny,
            allowed_tools: vec!["review_read".to_owned()],
            denied_tools: vec!["shell".to_owned()],
            allowed_paths: Vec::new(),
        },
    }
}

const DEFAULT_TASK_RESOURCES: TaskResourceBudget = TaskResourceBudget {
    profile_version: 1,
    max_encoded_bytes: 512 * 1024,
    max_title_bytes: 512,
    max_string_bytes: 64 * 1024,
    max_collection_items: 128,
    max_value_depth: 32,
    max_value_nodes: 4_096,
    min_interval_seconds: 10,
    max_page_items: 100,
    max_response_bytes: 1024 * 1024,
    max_tree_nodes: 128,
    max_tree_depth: 8,
    max_event_page_items: 200,
    max_wait_targets: 64,
    max_wait_duration_ms: 120_000,
    max_concurrent_waits: 4,
};

const DEFAULT_MCP_INVOCATION_RESOURCES: McpInvocationResourceLimits = McpInvocationResourceLimits {
    profile_version: 1,
    max_arguments_bytes: 128 * 1024,
    max_arguments_depth: 32,
    max_result_wire_bytes: 1024 * 1024,
    max_result_decoded_bytes: 1024 * 1024,
    max_result_depth: 32,
    max_result_tokens: 64 * 1024,
    max_result_media: 8,
    max_timeout_ms: 120_000,
    max_concurrent_calls: 8,
    max_queued_calls: 16,
};

const DEFAULT_NATIVE_EVENT_RESOURCES: NativeEventBudget = NativeEventBudget {
    profile_version: 1,
    max_frame_bytes: 1024 * 1024,
    max_recovery_frame_bytes: 64 * 1024 * 1024,
    max_json_depth: 64,
    max_json_nodes: 16_384,
    max_string_bytes: 256 * 1024,
    max_journal_payload_bytes: 256 * 1024,
};

#[cfg(test)]
const SYNTHETIC_NATIVE_EVENT_RESOURCES: NativeEventBudget = NativeEventBudget {
    profile_version: 1,
    max_frame_bytes: 16 * 1024,
    max_recovery_frame_bytes: 1024 * 1024,
    max_json_depth: 8,
    max_json_nodes: 256,
    max_string_bytes: 4 * 1024,
    max_journal_payload_bytes: 8 * 1024,
};

#[cfg(test)]
const SYNTHETIC_MCP_INVOCATION_RESOURCES: McpInvocationResourceLimits =
    McpInvocationResourceLimits {
        profile_version: 1,
        max_arguments_bytes: 4 * 1024,
        max_arguments_depth: 8,
        max_result_wire_bytes: 16 * 1024,
        max_result_decoded_bytes: 16 * 1024,
        max_result_depth: 8,
        max_result_tokens: 4 * 1024,
        max_result_media: 1,
        max_timeout_ms: 5_000,
        max_concurrent_calls: 1,
        max_queued_calls: 1,
    };

const SUPERUSER_EXECUTION_RESOURCES: ExecutionAdmissionQuotaPolicy =
    ExecutionAdmissionQuotaPolicy {
        active: ExecutionQuotaCeilings {
            per_principal: 64,
            per_role: GATEWAY_ACTIVE_EXECUTION_LIMIT,
            per_workspace: 128,
            gateway: GATEWAY_ACTIVE_EXECUTION_LIMIT,
        },
        queued: ExecutionQuotaCeilings {
            per_principal: 512,
            per_role: GATEWAY_QUEUED_EXECUTION_LIMIT,
            per_workspace: 1_024,
            gateway: GATEWAY_QUEUED_EXECUTION_LIMIT,
        },
        scheduled: ExecutionQuotaCeilings {
            per_principal: 1_024,
            per_role: GATEWAY_SCHEDULED_EXECUTION_LIMIT,
            per_workspace: 2_048,
            gateway: GATEWAY_SCHEDULED_EXECUTION_LIMIT,
        },
    };

const MEMBER_EXECUTION_RESOURCES: ExecutionAdmissionQuotaPolicy = ExecutionAdmissionQuotaPolicy {
    active: ExecutionQuotaCeilings {
        per_principal: 4,
        per_role: 128,
        per_workspace: 64,
        gateway: GATEWAY_ACTIVE_EXECUTION_LIMIT,
    },
    queued: ExecutionQuotaCeilings {
        per_principal: 32,
        per_role: 1_024,
        per_workspace: 512,
        gateway: GATEWAY_QUEUED_EXECUTION_LIMIT,
    },
    scheduled: ExecutionQuotaCeilings {
        per_principal: 64,
        per_role: 2_048,
        per_workspace: 1_024,
        gateway: GATEWAY_SCHEDULED_EXECUTION_LIMIT,
    },
};

const SUPERUSER_OBSERVATION_RESOURCES: ObservationResourcePolicy = ObservationResourcePolicy {
    max_turn_page_items: 200,
    max_turn_page_bytes: 1024 * 1024,
    max_concurrent_pages_per_principal: 16,
    max_concurrent_pages_per_role: 128,
    max_concurrent_pages_per_workspace: 64,
};

const MEMBER_OBSERVATION_RESOURCES: ObservationResourcePolicy = ObservationResourcePolicy {
    max_turn_page_items: 200,
    max_turn_page_bytes: 1024 * 1024,
    max_concurrent_pages_per_principal: 4,
    max_concurrent_pages_per_role: 128,
    max_concurrent_pages_per_workspace: 32,
};

#[cfg(test)]
const SYNTHETIC_OBSERVATION_RESOURCES: ObservationResourcePolicy = ObservationResourcePolicy {
    max_turn_page_items: 32,
    max_turn_page_bytes: 128 * 1024,
    max_concurrent_pages_per_principal: 1,
    max_concurrent_pages_per_role: 4,
    max_concurrent_pages_per_workspace: 4,
};

#[cfg(test)]
const SYNTHETIC_TASK_RESOURCES: TaskResourceBudget = TaskResourceBudget {
    max_page_items: 7,
    max_response_bytes: 64 * 1024,
    max_tree_nodes: 9,
    max_tree_depth: 3,
    max_event_page_items: 11,
    max_wait_targets: 5,
    max_wait_duration_ms: 2_000,
    max_concurrent_waits: 1,
    ..DEFAULT_TASK_RESOURCES
};

#[cfg(test)]
const SYNTHETIC_EXECUTION_RESOURCES: ExecutionAdmissionQuotaPolicy =
    ExecutionAdmissionQuotaPolicy {
        active: ExecutionQuotaCeilings {
            per_principal: 1,
            per_role: 8,
            per_workspace: 4,
            gateway: GATEWAY_ACTIVE_EXECUTION_LIMIT,
        },
        queued: ExecutionQuotaCeilings {
            per_principal: 2,
            per_role: 16,
            per_workspace: 8,
            gateway: GATEWAY_QUEUED_EXECUTION_LIMIT,
        },
        scheduled: ExecutionQuotaCeilings {
            per_principal: 2,
            per_role: 16,
            per_workspace: 8,
            gateway: GATEWAY_SCHEDULED_EXECUTION_LIMIT,
        },
    };

const MEMBER_ACTIONS: &[ResourceAction] = &[
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

const SUPERUSER: RoleDefinition = RoleDefinition {
    key: SUPERUSER_CAPABILITY_ROLE_KEY,
    principal_kind: PrincipalKind::Superuser,
    actions: RoleActionPolicy::All,
    resources: RoleResourcePolicy::Absolute,
    operational_resources: OperationalResourcePolicy::ALL,
    permission_cap: full_access_permission_cap,
    approval_scope_cap: collaboration_approval_scope_cap,
    permission_presets: ALL_PERMISSION_PRESETS,
    human_interaction_budget: HumanInteractionBudget::DEFAULT,
    execution_resources: SUPERUSER_EXECUTION_RESOURCES,
    observation_resources: SUPERUSER_OBSERVATION_RESOURCES,
    task_resources: DEFAULT_TASK_RESOURCES,
    mcp_invocation_resources: DEFAULT_MCP_INVOCATION_RESOURCES,
    native_event_resources: DEFAULT_NATIVE_EVENT_RESOURCES,
    runtime_principal: RuntimePrincipalPolicy::Absolute,
    disclosure: RoleDisclosurePolicy::Administrative,
    invitation_assignable: false,
    invitation_default: false,
    lifecycle_managed: false,
    presentation: RolePresentation {
        display_name: "Superuser",
        description: "Gateway administrator",
        built_in: true,
    },
};

const MEMBER: RoleDefinition = RoleDefinition {
    key: MEMBER_ROLE_KEY,
    principal_kind: PrincipalKind::User,
    actions: RoleActionPolicy::Only(MEMBER_ACTIONS),
    resources: RoleResourcePolicy::ScopedCollaboration,
    operational_resources: OperationalResourcePolicy::ALL,
    permission_cap: supervised_permission_cap,
    approval_scope_cap: collaboration_approval_scope_cap,
    permission_presets: SUPERVISED_PERMISSION_PRESET,
    human_interaction_budget: HumanInteractionBudget::DEFAULT,
    execution_resources: MEMBER_EXECUTION_RESOURCES,
    observation_resources: MEMBER_OBSERVATION_RESOURCES,
    task_resources: DEFAULT_TASK_RESOURCES,
    mcp_invocation_resources: DEFAULT_MCP_INVOCATION_RESOURCES,
    native_event_resources: DEFAULT_NATIVE_EVENT_RESOURCES,
    runtime_principal: RuntimePrincipalPolicy::ScopedCollaboration,
    disclosure: RoleDisclosurePolicy::Collaborator,
    invitation_assignable: true,
    invitation_default: true,
    lifecycle_managed: true,
    presentation: RolePresentation {
        display_name: "Member",
        description: "Workspace collaborator",
        built_in: true,
    },
};

#[cfg(test)]
const SYNTHETIC_OBSERVER_ACTIONS: &[ResourceAction] = &[
    ResourceAction::WorkspaceList,
    ResourceAction::WorkspaceRead,
    ResourceAction::ThreadRead,
    ResourceAction::AgentExecutionObserve,
    ResourceAction::AgentRequestObserve,
    ResourceAction::ChildObserve,
    ResourceAction::AgentsDocumentRead,
    ResourceAction::ArtifactRead,
    ResourceAction::TaskRead,
];

#[cfg(test)]
const SYNTHETIC_EXECUTOR_ACTIONS: &[ResourceAction] = &[
    ResourceAction::WorkspaceList,
    ResourceAction::WorkspaceRead,
    ResourceAction::ThreadRead,
    ResourceAction::AgentTurnStart,
    ResourceAction::ChildStart,
    ResourceAction::TaskCreate,
    // Deliberately excludes ArtifactBindThread. This proves that a role may
    // create an artifact without acquiring authority to attach it to a
    // collaborative capsule.
    ResourceAction::ArtifactCreateThread,
    ResourceAction::ProviderDiscover,
    ResourceAction::ProviderUse,
];

#[cfg(test)]
const SYNTHETIC_APPROVER_ACTIONS: &[ResourceAction] = &[
    ResourceAction::WorkspaceList,
    ResourceAction::WorkspaceRead,
    ResourceAction::ThreadRead,
    ResourceAction::AgentExecutionObserve,
    ResourceAction::AgentRequestObserve,
    ResourceAction::AgentRequestRespond,
    ResourceAction::ChildObserve,
    ResourceAction::ChildRespond,
];

#[cfg(test)]
const SYNTHETIC_OBSERVER: RoleDefinition = RoleDefinition {
    key: "synthetic_observer",
    principal_kind: PrincipalKind::User,
    actions: RoleActionPolicy::Only(SYNTHETIC_OBSERVER_ACTIONS),
    resources: RoleResourcePolicy::ScopedCollaboration,
    // Deliberately has no backend selector. This proves that collaboration
    // actions over an existing execution are independent from authority to
    // start or continue its provider/runtime.
    operational_resources: OperationalResourcePolicy {
        providers: Some(&[]),
        provider_models: Some(&[]),
        cli_runtimes: Some(&[]),
        cli_models: Some(&[]),
        skills: Some(&[]),
        mcp_servers: Some(&[]),
    },
    permission_cap: synthetic_observer_permission_cap,
    approval_scope_cap: no_approval_scope_cap,
    permission_presets: SUPERVISED_PERMISSION_PRESET,
    human_interaction_budget: HumanInteractionBudget::DEFAULT,
    execution_resources: SYNTHETIC_EXECUTION_RESOURCES,
    observation_resources: SYNTHETIC_OBSERVATION_RESOURCES,
    task_resources: SYNTHETIC_TASK_RESOURCES,
    mcp_invocation_resources: SYNTHETIC_MCP_INVOCATION_RESOURCES,
    native_event_resources: SYNTHETIC_NATIVE_EVENT_RESOURCES,
    runtime_principal: RuntimePrincipalPolicy::ScopedCollaboration,
    disclosure: RoleDisclosurePolicy::Collaborator,
    invitation_assignable: true,
    invitation_default: false,
    lifecycle_managed: true,
    presentation: RolePresentation {
        display_name: "Synthetic observer",
        description: "Proposal 63 read-only contract role",
        built_in: false,
    },
};

#[cfg(test)]
const SYNTHETIC_EXECUTOR: RoleDefinition = RoleDefinition {
    key: "synthetic_executor",
    principal_kind: PrincipalKind::User,
    actions: RoleActionPolicy::Only(SYNTHETIC_EXECUTOR_ACTIONS),
    resources: RoleResourcePolicy::ScopedCollaboration,
    operational_resources: OperationalResourcePolicy {
        providers: Some(&["allowed-provider"]),
        provider_models: Some(&[("allowed-provider", "allowed-model")]),
        cli_runtimes: Some(&["allowed-cli"]),
        cli_models: Some(&[("allowed-cli", "allowed-cli-model")]),
        skills: Some(&["allowed-skill"]),
        mcp_servers: Some(&["allowed-mcp"]),
    },
    permission_cap: supervised_permission_cap,
    approval_scope_cap: no_approval_scope_cap,
    permission_presets: SUPERVISED_PERMISSION_PRESET,
    human_interaction_budget: HumanInteractionBudget::DEFAULT,
    execution_resources: SYNTHETIC_EXECUTION_RESOURCES,
    observation_resources: SYNTHETIC_OBSERVATION_RESOURCES,
    task_resources: SYNTHETIC_TASK_RESOURCES,
    mcp_invocation_resources: SYNTHETIC_MCP_INVOCATION_RESOURCES,
    native_event_resources: SYNTHETIC_NATIVE_EVENT_RESOURCES,
    runtime_principal: RuntimePrincipalPolicy::ScopedCollaboration,
    disclosure: RoleDisclosurePolicy::Collaborator,
    invitation_assignable: true,
    invitation_default: false,
    lifecycle_managed: true,
    presentation: RolePresentation {
        display_name: "Synthetic executor",
        description: "Proposal 63 bounded execution contract role",
        built_in: false,
    },
};

#[cfg(test)]
const SYNTHETIC_APPROVER: RoleDefinition = RoleDefinition {
    key: "synthetic_approver",
    principal_kind: PrincipalKind::User,
    actions: RoleActionPolicy::Only(SYNTHETIC_APPROVER_ACTIONS),
    resources: RoleResourcePolicy::ScopedCollaboration,
    operational_resources: OperationalResourcePolicy {
        providers: Some(&[]),
        provider_models: Some(&[]),
        cli_runtimes: Some(&[]),
        cli_models: Some(&[]),
        skills: Some(&[]),
        mcp_servers: Some(&[]),
    },
    permission_cap: supervised_permission_cap,
    approval_scope_cap: collaboration_approval_scope_cap,
    permission_presets: SUPERVISED_PERMISSION_PRESET,
    human_interaction_budget: HumanInteractionBudget::DEFAULT,
    execution_resources: SYNTHETIC_EXECUTION_RESOURCES,
    observation_resources: SYNTHETIC_OBSERVATION_RESOURCES,
    task_resources: SYNTHETIC_TASK_RESOURCES,
    mcp_invocation_resources: SYNTHETIC_MCP_INVOCATION_RESOURCES,
    native_event_resources: SYNTHETIC_NATIVE_EVENT_RESOURCES,
    runtime_principal: RuntimePrincipalPolicy::ScopedCollaboration,
    disclosure: RoleDisclosurePolicy::Collaborator,
    invitation_assignable: true,
    invitation_default: false,
    lifecycle_managed: true,
    presentation: RolePresentation {
        display_name: "Synthetic approver",
        description: "Proposal 63 approval-only collaboration role",
        built_in: false,
    },
};

#[cfg(not(test))]
const ROLE_DEFINITIONS: &[RoleDefinition] = &[SUPERUSER, MEMBER];

#[cfg(test)]
const ROLE_DEFINITIONS: &[RoleDefinition] = &[
    SUPERUSER,
    MEMBER,
    SYNTHETIC_OBSERVER,
    SYNTHETIC_EXECUTOR,
    SYNTHETIC_APPROVER,
];

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RoleDefinitionRegistry;

impl RoleDefinitionRegistry {
    pub(crate) const fn new() -> Self {
        Self
    }

    pub(crate) fn resolve(
        self,
        principal_kind: PrincipalKind,
        role_key: Option<&RoleKey>,
    ) -> Option<&'static RoleDefinition> {
        let key = match (principal_kind, role_key) {
            (PrincipalKind::Superuser, None) => SUPERUSER_CAPABILITY_ROLE_KEY,
            (PrincipalKind::User, Some(role_key)) => role_key.as_str(),
            _ => return None,
        };
        ROLE_DEFINITIONS
            .iter()
            .find(|definition| definition.principal_kind == principal_kind && definition.key == key)
    }

    pub(crate) fn resolve_user_role(self, role_key: &RoleKey) -> Option<&'static RoleDefinition> {
        self.resolve(PrincipalKind::User, Some(role_key))
    }

    pub(crate) fn invitation_role_options(self) -> Vec<AuthorizationInvitationRoleOption> {
        ROLE_DEFINITIONS
            .iter()
            .filter(|definition| {
                definition.principal_kind == PrincipalKind::User && definition.invitation_assignable
            })
            .map(|definition| AuthorizationInvitationRoleOption {
                role: AuthorizationRolePresentation {
                    key: definition.key.to_owned(),
                    display_name: definition.presentation.display_name.to_owned(),
                    description: definition.presentation.description.to_owned(),
                    built_in: definition.presentation.built_in,
                },
                is_default: definition.invitation_default,
            })
            .collect()
    }

    /// Resolves the globally unique policy identity persisted in execution
    /// envelopes. Runtime code must use this registry lookup instead of
    /// inferring a principal class from a well-known role-name string.
    pub(crate) fn resolve_key(self, role_key: &RoleKey) -> Option<&'static RoleDefinition> {
        ROLE_DEFINITIONS
            .iter()
            .find(|definition| definition.key == role_key.as_str())
    }

    /// Stable fingerprint used to advance durable policy generation whenever
    /// a code-defined role changes across deployments.
    pub(crate) fn policy_fingerprint(self) -> String {
        policy_fingerprint_for(ROLE_DEFINITIONS)
    }
}

fn policy_fingerprint_for(definitions: &[RoleDefinition]) -> String {
    let mut digest = Sha256::new();
    for definition in definitions {
        digest.update(definition.key.as_bytes());
        digest.update([match definition.principal_kind {
            PrincipalKind::Superuser => 0,
            PrincipalKind::User => 1,
        }]);
        match definition.actions {
            RoleActionPolicy::All => digest.update(b"*"),
            RoleActionPolicy::Only(actions) => {
                for action in actions {
                    digest.update(action.safe_name().as_bytes());
                    digest.update([0]);
                }
            }
        }
        let permission_cap = (definition.permission_cap)();
        digest.update(
            serde_json::to_vec(&permission_cap).expect("registered permission cap must serialize"),
        );
        let approval_scope_cap = (definition.approval_scope_cap)();
        digest.update(
            serde_json::to_vec(&approval_scope_cap)
                .expect("registered approval scope cap must serialize"),
        );
        for preset in definition.permission_presets {
            digest.update(preset.as_str().as_bytes());
            digest.update([0]);
        }
        for value in [
            definition
                .human_interaction_budget
                .max_pending_requests_per_execution,
            definition
                .human_interaction_budget
                .max_questions_per_request,
            definition.human_interaction_budget.max_options_per_question,
            definition.human_interaction_budget.max_field_bytes,
            definition.human_interaction_budget.max_aggregate_bytes,
            definition.human_interaction_budget.max_response_bytes,
            definition.human_interaction_budget.max_json_depth,
            definition.human_interaction_budget.max_json_nodes,
        ] {
            digest.update((value as u64).to_le_bytes());
        }
        for ceilings in [
            definition.execution_resources.active,
            definition.execution_resources.queued,
            definition.execution_resources.scheduled,
        ] {
            for value in [
                ceilings.per_principal,
                ceilings.per_role,
                ceilings.per_workspace,
                ceilings.gateway,
            ] {
                digest.update(value.to_le_bytes());
            }
        }
        for value in [
            definition.observation_resources.max_turn_page_items as u64,
            definition.observation_resources.max_turn_page_bytes as u64,
            definition
                .observation_resources
                .max_concurrent_pages_per_principal as u64,
            definition
                .observation_resources
                .max_concurrent_pages_per_role as u64,
            definition
                .observation_resources
                .max_concurrent_pages_per_workspace as u64,
        ] {
            digest.update(value.to_le_bytes());
        }
        for value in [
            definition.task_resources.profile_version as u64,
            definition.task_resources.max_encoded_bytes as u64,
            definition.task_resources.max_title_bytes as u64,
            definition.task_resources.max_string_bytes as u64,
            definition.task_resources.max_collection_items as u64,
            definition.task_resources.max_value_depth as u64,
            definition.task_resources.max_value_nodes as u64,
            definition.task_resources.min_interval_seconds as u64,
            definition.task_resources.max_page_items as u64,
            definition.task_resources.max_response_bytes as u64,
            definition.task_resources.max_tree_nodes as u64,
            definition.task_resources.max_tree_depth as u64,
            definition.task_resources.max_event_page_items as u64,
            definition.task_resources.max_wait_targets as u64,
            definition.task_resources.max_wait_duration_ms,
            definition.task_resources.max_concurrent_waits as u64,
        ] {
            digest.update(value.to_le_bytes());
        }
        let mcp = definition.mcp_invocation_resources;
        for value in [
            mcp.profile_version as u64,
            mcp.max_arguments_bytes as u64,
            mcp.max_arguments_depth as u64,
            mcp.max_result_wire_bytes as u64,
            mcp.max_result_decoded_bytes as u64,
            mcp.max_result_depth as u64,
            mcp.max_result_tokens as u64,
            mcp.max_result_media as u64,
            mcp.max_timeout_ms,
            mcp.max_concurrent_calls as u64,
            mcp.max_queued_calls as u64,
        ] {
            digest.update(value.to_le_bytes());
        }
        let native = definition.native_event_resources;
        for value in [
            native.profile_version as u64,
            native.max_frame_bytes as u64,
            native.max_json_depth as u64,
            native.max_json_nodes as u64,
            native.max_string_bytes as u64,
            native.max_journal_payload_bytes as u64,
        ] {
            digest.update(value.to_le_bytes());
        }
        digest.update([match definition.resources {
            RoleResourcePolicy::Absolute => 0,
            RoleResourcePolicy::ScopedCollaboration => 1,
        }]);
        for selector in [
            definition.operational_resources.providers,
            definition.operational_resources.cli_runtimes,
            definition.operational_resources.skills,
            definition.operational_resources.mcp_servers,
        ] {
            match selector {
                None => digest.update(b"*"),
                Some(ids) => {
                    for id in ids {
                        digest.update(id.as_bytes());
                        digest.update([0]);
                    }
                }
            }
            digest.update([0xff]);
        }
        for selector in [
            definition.operational_resources.provider_models,
            definition.operational_resources.cli_models,
        ] {
            match selector {
                None => digest.update(b"*"),
                Some(entries) => {
                    for (parent, id) in entries {
                        digest.update(parent.as_bytes());
                        digest.update([0]);
                        digest.update(id.as_bytes());
                        digest.update([0]);
                    }
                }
            }
            digest.update([0xfe]);
        }
        digest.update([match definition.runtime_principal {
            RuntimePrincipalPolicy::Absolute => 0,
            RuntimePrincipalPolicy::ScopedCollaboration => 1,
        }]);
        digest.update([match definition.disclosure {
            RoleDisclosurePolicy::Administrative => 0,
            RoleDisclosurePolicy::Collaborator => 1,
        }]);
        digest.update([definition.invitation_assignable as u8]);
        digest.update([definition.invitation_default as u8]);
        digest.update([definition.lifecycle_managed as u8]);
        digest.update(definition.presentation.display_name.as_bytes());
        digest.update(definition.presentation.description.as_bytes());
        digest.update([definition.presentation.built_in as u8]);
    }
    hex::encode(digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_role_is_added_by_one_definition_and_resolves_every_trait() {
        let registry = RoleDefinitionRegistry::new();
        let role = RoleKey::new("synthetic_observer").expect("valid role");
        let definition = registry
            .resolve_user_role(&role)
            .expect("synthetic definition registered");

        assert!(definition.actions.allows(ResourceAction::ThreadRead));
        assert!(definition.actions.allows(ResourceAction::ChildObserve));
        assert!(!definition.actions.allows(ResourceAction::ChildControl));
        assert!(!definition.actions.allows(ResourceAction::MessageCreate));
        assert!(!definition.actions.allows(ResourceAction::AgentTurnStart));
        assert_eq!(
            definition.runtime_principal,
            RuntimePrincipalPolicy::ScopedCollaboration
        );
        assert!(definition.invitation_assignable);
        assert!(definition.lifecycle_managed);
        assert!(!definition.presentation.display_name.is_empty());
        assert!(!definition.presentation.description.is_empty());
        assert_eq!(definition.task_resources.max_page_items, 7);
        assert_eq!(definition.mcp_invocation_resources.max_concurrent_calls, 1);
        assert_eq!(definition.mcp_invocation_resources.max_queued_calls, 1);
        assert_ne!(
            definition.mcp_invocation_resources,
            registry
                .resolve_user_role(&RoleKey::member())
                .expect("member role is registered")
                .mcp_invocation_resources
        );
        assert_eq!(definition.native_event_resources.max_frame_bytes, 16 * 1024);
        assert_ne!(
            definition.native_event_resources,
            registry
                .resolve_user_role(&RoleKey::member())
                .expect("member role is registered")
                .native_event_resources
        );
        assert_eq!(
            definition
                .observation_resources
                .max_concurrent_pages_per_principal,
            1
        );
        assert_ne!(registry.policy_fingerprint(), "");
    }

    #[test]
    fn user_kind_without_exact_registered_key_never_becomes_member() {
        let registry = RoleDefinitionRegistry::new();
        assert!(registry.resolve(PrincipalKind::User, None).is_none());
        assert!(
            registry
                .resolve_user_role(&RoleKey::new("unknown_role").unwrap())
                .is_none()
        );
    }

    #[test]
    fn registered_role_keys_are_globally_unique_and_resolve_without_name_branches() {
        let registry = RoleDefinitionRegistry::new();
        let mut keys = std::collections::BTreeSet::new();
        for definition in ROLE_DEFINITIONS {
            assert!(keys.insert(definition.key), "duplicate role key");
            let role_key = RoleKey::new(definition.key).expect("registered role key is valid");
            let resolved = registry
                .resolve_key(&role_key)
                .expect("registered role resolves by persisted key");
            assert_eq!(resolved.key, definition.key);
            assert_eq!(resolved.principal_kind, definition.principal_kind);
        }
    }

    #[test]
    fn invitation_projection_has_one_explicit_default_and_no_unassignable_roles() {
        let registry = RoleDefinitionRegistry::new();
        let options = registry.invitation_role_options();
        assert_eq!(
            options.iter().filter(|option| option.is_default).count(),
            1,
            "the client must never infer an invitation default from role order"
        );
        for option in options {
            let role_key = RoleKey::new(option.role.key).expect("projected role key is valid");
            let definition = registry
                .resolve_user_role(&role_key)
                .expect("projected role is registered");
            assert!(definition.invitation_assignable);
            assert_eq!(option.is_default, definition.invitation_default);
        }
    }

    #[test]
    fn synthetic_approver_is_independent_from_start_and_backend_authority() {
        let role = RoleKey::new("synthetic_approver").expect("valid role");
        let definition = RoleDefinitionRegistry::new()
            .resolve_user_role(&role)
            .expect("approver role is registered");
        assert!(
            definition
                .actions
                .allows(ResourceAction::AgentRequestObserve)
        );
        assert!(
            definition
                .actions
                .allows(ResourceAction::AgentRequestRespond)
        );
        assert!(!definition.actions.allows(ResourceAction::AgentTurnStart));
        assert!(!definition.actions.allows(ResourceAction::ProviderUse));
        assert!(!definition.actions.allows(ResourceAction::CliRuntimeUse));
    }

    #[test]
    fn synthetic_executor_keeps_artifact_create_and_bind_as_independent_actions() {
        let role = RoleKey::new("synthetic_executor").expect("valid role");
        let definition = RoleDefinitionRegistry::new()
            .resolve_user_role(&role)
            .expect("executor role is registered");
        assert!(
            definition
                .actions
                .allows(ResourceAction::ArtifactCreateThread)
        );
        assert!(
            !definition
                .actions
                .allows(ResourceAction::ArtifactBindThread)
        );
    }

    #[test]
    fn approval_scope_changes_advance_the_role_policy_fingerprint() {
        let mut original = MEMBER;
        let original_fingerprint = policy_fingerprint_for(&[original]);

        original.approval_scope_cap = no_approval_scope_cap;
        let narrowed_fingerprint = policy_fingerprint_for(&[original]);

        assert_ne!(original_fingerprint, narrowed_fingerprint);
    }
}
