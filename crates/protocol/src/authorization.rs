use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{PrincipalId, ThreadVisibility, ToolPermissionPolicySnapshot, TurnPermissionMode};

/// Wire contract version for Gateway-owned UI capability projections.
///
/// This version is independent from the transport protocol version so a
/// future capability vocabulary can be evolved deliberately.
pub const AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION: u32 = 7;

/// Server-owned presentation metadata for the authenticated role. The key is
/// deliberately open-ended: clients display this object but never derive
/// authorization decisions from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationRolePresentation {
    pub key: String,
    pub display_name: String,
    pub description: String,
    pub built_in: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationCapabilitiesParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationGlobalCapabilities {
    pub can_create_workspace: bool,
    pub can_manage_gateway_settings: bool,
    pub can_manage_capabilities: bool,
    pub can_manage_providers: bool,
    pub can_manage_mcp: bool,
    pub can_manage_skills: bool,
    pub can_manage_cli_runtimes: bool,
    pub can_manage_all_threads: bool,
    pub can_view_invitations: bool,
    pub can_create_invitation: bool,
    /// Roles this principal may assign through a new invitation. The Gateway
    /// owns both availability and presentation; clients never manufacture a
    /// role key from a local enum or principal kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub invitation_role_options: Vec<AuthorizationInvitationRoleOption>,
    pub can_view_member_directory: bool,
    pub can_manage_member_lifecycle: bool,
    pub can_manage_own_sessions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationInvitationRoleOption {
    pub role: AuthorizationRolePresentation,
    pub is_default: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationWorkspaceCapabilities {
    pub can_read: bool,
    pub can_create_thread: bool,
    pub can_manage: bool,
    /// Read the authenticated principal's durable Task notification inbox in
    /// this workspace. The Gateway still filters every row by recipient.
    pub can_read_own_notifications: bool,
    /// Acknowledge rows from the authenticated principal's durable Task
    /// notification inbox in this workspace.
    pub can_acknowledge_own_notifications: bool,
    pub can_use_providers: bool,
    pub can_use_cli_runtimes: bool,
    pub can_use_skills: bool,
    pub can_use_mcp: bool,
    pub can_run_tasks: bool,
    pub can_read_artifacts: bool,
    pub can_write_artifacts: bool,
    /// Server-enforced aggregate execution limits for this principal. These
    /// are presentation only; every durable start reserves a Gateway lease.
    pub execution_limits: AuthorizationExecutionResourceLimits,
    /// Server-compiled UX presets after intersection with the immutable role
    /// ceiling. Clients render these policies and locked fields verbatim.
    pub agent_permission_options: Vec<AuthorizationAgentPermissionOption>,
    pub can_list_members: bool,
    pub can_add_member: bool,
    pub can_remove_member: bool,
    pub thread_visibility_options: Vec<ThreadVisibility>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationPermissionLock {
    pub field: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationAgentPermissionOption {
    pub id: String,
    pub label: String,
    pub description: String,
    pub mode: TurnPermissionMode,
    pub effective_policy: ToolPermissionPolicySnapshot,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locked: Vec<AuthorizationPermissionLock>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationExecutionResourceLimits {
    pub max_active_executions: u32,
    pub max_queued_tasks: u32,
    pub max_scheduled_tasks: u32,
}

/// Server-owned MCP invocation envelope. Clients may present these limits,
/// while the Gateway enforces the same immutable values for every backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct McpInvocationResourceLimits {
    pub profile_version: u32,
    pub max_arguments_bytes: usize,
    pub max_arguments_depth: usize,
    pub max_result_wire_bytes: usize,
    pub max_result_decoded_bytes: usize,
    pub max_result_depth: usize,
    pub max_result_tokens: usize,
    pub max_result_media: usize,
    pub max_timeout_ms: u64,
    pub max_concurrent_calls: usize,
    pub max_queued_calls: usize,
}

impl Default for McpInvocationResourceLimits {
    fn default() -> Self {
        Self {
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
        }
    }
}

impl McpInvocationResourceLimits {
    pub const fn is_valid(self) -> bool {
        self.profile_version > 0
            && self.max_arguments_bytes > 0
            && self.max_arguments_depth > 0
            && self.max_result_wire_bytes > 0
            && self.max_result_decoded_bytes > 0
            && self.max_result_depth > 0
            && self.max_result_tokens > 0
            && self.max_result_media > 0
            && self.max_timeout_ms > 0
            && self.max_concurrent_calls > 0
            && self.max_queued_calls > 0
    }

    /// Intersects an immutable execution ceiling with a current role ceiling.
    /// Profile versions must match so a future schema is never interpreted as
    /// an older, potentially wider policy.
    pub const fn intersect(self, current: Self) -> Option<Self> {
        if !self.is_valid()
            || !current.is_valid()
            || self.profile_version != current.profile_version
        {
            return None;
        }
        Some(Self {
            profile_version: self.profile_version,
            max_arguments_bytes: min_usize(self.max_arguments_bytes, current.max_arguments_bytes),
            max_arguments_depth: min_usize(self.max_arguments_depth, current.max_arguments_depth),
            max_result_wire_bytes: min_usize(
                self.max_result_wire_bytes,
                current.max_result_wire_bytes,
            ),
            max_result_decoded_bytes: min_usize(
                self.max_result_decoded_bytes,
                current.max_result_decoded_bytes,
            ),
            max_result_depth: min_usize(self.max_result_depth, current.max_result_depth),
            max_result_tokens: min_usize(self.max_result_tokens, current.max_result_tokens),
            max_result_media: min_usize(self.max_result_media, current.max_result_media),
            max_timeout_ms: min_u64(self.max_timeout_ms, current.max_timeout_ms),
            max_concurrent_calls: min_usize(
                self.max_concurrent_calls,
                current.max_concurrent_calls,
            ),
            max_queued_calls: min_usize(self.max_queued_calls, current.max_queued_calls),
        })
    }
}

const fn min_usize(left: usize, right: usize) -> usize {
    if left < right { left } else { right }
}

const fn min_u64(left: u64, right: u64) -> u64 {
    if left < right { left } else { right }
}

/// Role-scoped constraints used by discovery and exact admission. `all=true`
/// means every operationally enabled resource of that kind; otherwise only
/// the listed stable identifiers are visible and usable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationResourceSelector {
    pub all: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationProviderModelGrant {
    pub provider: String,
    pub model: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationCliModelGrant {
    pub runtime_id: String,
    pub model: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationOperationalResourceProjection {
    /// Opaque receipt over role policy, workspace and authorization revision.
    pub fingerprint: String,
    pub providers: AuthorizationResourceSelector,
    /// `true` means every model of an allowed provider; `false` means the
    /// exact grants below. The flag preserves the distinction between an
    /// unrestricted model set and an intentionally empty one.
    pub provider_models_all: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_models: Vec<AuthorizationProviderModelGrant>,
    pub cli_runtimes: AuthorizationResourceSelector,
    /// Same contract as `provider_models_all`, scoped to allowed CLI runtimes.
    pub cli_models_all: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cli_models: Vec<AuthorizationCliModelGrant>,
    pub skills: AuthorizationResourceSelector,
    pub mcp_servers: AuthorizationResourceSelector,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationExecutionDraftPolicyProjection {
    // Stable semantic receipt for Composer selections. Unlike the enclosing
    // capability snapshot generation, this changes only when the effective
    // draft policy changes.
    pub fingerprint: String,
    pub resources: AuthorizationOperationalResourceProjection,
    pub permission_options: Vec<AuthorizationAgentPermissionOption>,
    pub can_attach_artifacts: bool,
    pub mcp_invocation_limits: McpInvocationResourceLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationWorkspaceCapabilitySnapshot {
    pub workspace_id: String,
    pub capabilities: AuthorizationWorkspaceCapabilities,
    pub operational_resources: AuthorizationOperationalResourceProjection,
    pub execution_draft_policy: AuthorizationExecutionDraftPolicyProjection,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationThreadCapabilities {
    pub can_read: bool,
    pub can_write: bool,
    pub can_edit_own_message: bool,
    pub can_delete_own_message: bool,
    pub can_start_turn: bool,
    pub can_observe_agent_execution: bool,
    pub can_cancel_agent_execution: bool,
    pub can_resume_agent_execution: bool,
    pub can_steer_agent_execution: bool,
    pub can_observe_agent_requests: bool,
    pub can_respond_to_agent_requests: bool,
    pub can_control_cli_runtime: bool,
    pub can_create_task: bool,
    pub can_review_tasks: bool,
    pub can_cancel_tasks: bool,
    pub can_read_artifacts: bool,
    pub can_write_artifacts: bool,
    pub can_bind_artifacts: bool,
    pub can_read_agents_document: bool,
    pub can_manage_agents_document: bool,
    pub can_manage: bool,
    pub can_manage_private_participants: bool,
    pub can_move: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationThreadCapabilitySnapshot {
    pub workspace_id: String,
    pub thread_id: String,
    pub capabilities: AuthorizationThreadCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationCapabilitySnapshot {
    pub schema_version: u32,
    pub authorization_revision: u64,
    pub principal_id: PrincipalId,
    /// Stable built-in role identifier. Clients may display this value but
    /// must never derive permissions from it.
    pub role_key: String,
    pub role: AuthorizationRolePresentation,
    pub global: AuthorizationGlobalCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<AuthorizationWorkspaceCapabilitySnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<AuthorizationThreadCapabilitySnapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_snapshot_round_trip_keeps_version_revision_and_future_role_key() {
        let snapshot = AuthorizationCapabilitySnapshot {
            schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision: 91,
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            role_key: "future_code_role".to_owned(),
            role: AuthorizationRolePresentation {
                key: "future_code_role".to_owned(),
                display_name: "Future role".to_owned(),
                description: "Future server-defined role".to_owned(),
                built_in: false,
            },
            global: AuthorizationGlobalCapabilities::default(),
            workspace: None,
            thread: None,
        };

        let encoded = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(
            encoded["schema_version"],
            AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION
        );
        assert_eq!(encoded["authorization_revision"], 91);
        assert_eq!(encoded["role_key"], "future_code_role");
        assert_eq!(
            serde_json::from_value::<AuthorizationCapabilitySnapshot>(encoded).unwrap(),
            snapshot
        );
    }

    #[test]
    fn capability_params_reject_unknown_fields() {
        let error = serde_json::from_value::<AuthorizationCapabilitiesParams>(
            serde_json::json!({ "workspace_id": "workspace", "legacy_role": "member" }),
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn mcp_resource_intersection_never_widens_either_ceiling() {
        let admitted = McpInvocationResourceLimits {
            max_arguments_bytes: 8_000,
            max_timeout_ms: 4_000,
            max_concurrent_calls: 2,
            ..Default::default()
        };
        let current = McpInvocationResourceLimits {
            max_arguments_bytes: 4_000,
            max_timeout_ms: 8_000,
            max_concurrent_calls: 1,
            ..Default::default()
        };
        let effective = admitted.intersect(current).expect("compatible profiles");
        assert_eq!(effective.max_arguments_bytes, 4_000);
        assert_eq!(effective.max_timeout_ms, 4_000);
        assert_eq!(effective.max_concurrent_calls, 1);

        let incompatible = McpInvocationResourceLimits {
            profile_version: admitted.profile_version + 1,
            ..current
        };
        assert!(admitted.intersect(incompatible).is_none());
    }
}
