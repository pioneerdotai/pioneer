use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{PrincipalId, ThreadVisibility, TurnPermissionMode};

/// Wire contract version for Gateway-owned UI capability projections.
///
/// This version is independent from the transport protocol version so a
/// future capability vocabulary can be evolved deliberately.
pub const AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

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
    pub can_manage_all_threads: bool,
    pub can_view_invitations: bool,
    pub can_create_invitation: bool,
    pub can_view_member_directory: bool,
    pub can_manage_member_lifecycle: bool,
    pub can_manage_own_sessions: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationWorkspaceCapabilities {
    pub can_read: bool,
    pub can_create_thread: bool,
    pub can_manage: bool,
    pub can_use_providers: bool,
    pub can_use_cli_runtimes: bool,
    pub can_use_skills: bool,
    pub can_use_mcp: bool,
    pub can_run_tasks: bool,
    /// Permission modes this principal may select for an agent turn in this
    /// workspace. The Gateway applies the same role cap again at execution.
    pub turn_permission_modes: Vec<TurnPermissionMode>,
    pub can_list_members: bool,
    pub can_add_member: bool,
    pub can_remove_member: bool,
    pub thread_visibility_options: Vec<ThreadVisibility>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationWorkspaceCapabilitySnapshot {
    pub workspace_id: String,
    pub capabilities: AuthorizationWorkspaceCapabilities,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationThreadCapabilities {
    pub can_read: bool,
    pub can_write: bool,
    pub can_start_turn: bool,
    pub can_respond_to_agent_requests: bool,
    pub can_control_cli_runtime: bool,
    pub can_create_task: bool,
    pub can_read_artifacts: bool,
    pub can_write_artifacts: bool,
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
            global: AuthorizationGlobalCapabilities::default(),
            workspace: None,
            thread: None,
        };

        let encoded = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(encoded["schema_version"], 1);
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
}
