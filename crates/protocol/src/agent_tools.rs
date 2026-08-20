//! Model-facing agent tools.
//!
//! The model is allowed to select only opaque, server-projected identities and
//! execution profiles.  The values in this module deliberately do not contain
//! actor references, provider configuration, credentials, host paths, or
//! runtime session identifiers.  Adapters turn these typed inputs into the
//! canonical [`AgentActionIntent`] after binding the current execution.

use crate::{
    AgentAuthoredInput, AgentExecutionProfileBackend, AgentExecutionProfileId,
    AgentExecutionProfileSelection, AgentIdentityId, AgentIdentitySelection, AgentReviewDecision,
    AgentStartOptionsProjection, AgentTaskControl,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;

/// Names exposed to a model.  These are intentionally distinct from internal
/// service methods and contain no provider-specific tool names.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentModelToolName {
    AgentStartOptions,
    SendMessage,
    CreateThread,
    StartAgent,
    CreateTask,
    ScheduleTask,
    ReviewTask,
    ControlTask,
    Wait,
    Result,
}

impl AgentModelToolName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AgentStartOptions => "agent_start_options",
            Self::SendMessage => "thread_message_send",
            Self::CreateThread => "thread_create",
            Self::StartAgent => "agent_start",
            Self::CreateTask => "task_create",
            Self::ScheduleTask => "task_schedule",
            Self::ReviewTask => "task_review",
            Self::ControlTask => "task_control",
            Self::Wait => "task_wait",
            Self::Result => "task_result",
        }
    }
}

/// Capability gates are evaluated by the server before a tool is projected.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolCapability {
    MessageCreate,
    ThreadCreate,
    ChildStart,
    TaskCreate,
    TaskSchedule,
    TaskReview,
    TaskControl,
    TaskObserve,
    ResultRead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentToolIdentityOption {
    pub id: AgentIdentityId,
    pub display_name: String,
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_label: Option<String>,
    pub source_kind: crate::AgentIdentitySourceKind,
}

/// Deliberately contains only a backend class.  Runtime instance IDs and
/// provider/model configuration remain server-side implementation details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolBackendKind {
    Api,
    Cli,
    Acp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentToolProfileOption {
    pub id: AgentExecutionProfileId,
    pub compatible_identity_ids: Vec<AgentIdentityId>,
    pub backend: AgentToolBackendKind,
    #[serde(default)]
    pub allowed_reasoning: Vec<crate::TurnReasoningSelection>,
    #[serde(default)]
    pub allowed_permission_profiles: Vec<crate::TurnPermissionMode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentToolOptionsProjection {
    pub identities: Vec<AgentToolIdentityOption>,
    pub profiles: Vec<AgentToolProfileOption>,
    pub inherit_parent_identity_available: bool,
    pub default_pioneer_identity_available: bool,
    pub derived_ephemeral_identity_available: bool,
    pub inherit_parent_profile_available: bool,
    #[serde(default)]
    pub allowed_skill_ids: Vec<crate::SkillId>,
    #[serde(default)]
    pub allowed_mcp_server_ids: Vec<String>,
    pub max_permission_profile: crate::TurnPermissionProfileCap,
    #[serde(default)]
    pub thread_creation_options: Vec<AgentToolThreadCreationOption>,
    #[serde(default)]
    pub target_options: Vec<AgentToolTargetOption>,
    pub generation_fingerprint: String,
}

/// Read-only options lookup takes no model-controlled selector.  Target and
/// execution context come exclusively from the execution-bound provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentStartOptionsToolInput {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentToolThreadCreationOption {
    pub id: String,
    pub label: String,
}

/// Opaque action target projected by the server. The model never receives a
/// thread, route, workspace, capsule or Gateway identifier and therefore
/// cannot turn guessed infrastructure IDs into an existence oracle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentToolTargetOption {
    pub id: String,
    pub label: String,
}

impl AgentToolOptionsProjection {
    pub fn contains_identity(&self, id: &AgentIdentityId) -> bool {
        self.identities.iter().any(|candidate| &candidate.id == id)
    }

    pub fn contains_profile_for_identity(
        &self,
        profile_id: &AgentExecutionProfileId,
        identity_id: &AgentIdentityId,
    ) -> bool {
        self.profiles.iter().any(|profile| {
            &profile.id == profile_id
                && profile
                    .compatible_identity_ids
                    .iter()
                    .any(|candidate| candidate == identity_id)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum AgentToolIdentityChoice {
    InheritParent,
    DefaultPioneer,
    Exact {
        id: AgentIdentityId,
    },
    ServerDerivedEphemeral {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_name_hint: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role_label: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(
    rename_all = "snake_case",
    tag = "kind",
    content = "id",
    deny_unknown_fields
)]
pub enum AgentToolProfileChoice {
    InheritParent,
    Exact(AgentExecutionProfileId),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentToolLaunchSelection {
    pub identity: AgentToolIdentityChoice,
    pub profile: AgentToolProfileChoice,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<crate::TurnReasoningSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_profile: Option<crate::TurnPermissionProfileSelection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skill_ids: Vec<crate::SkillId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mcp_server_ids: Vec<String>,
}

impl AgentToolLaunchSelection {
    pub fn into_server_selection(self) -> crate::AgentLaunchSelection {
        crate::AgentLaunchSelection {
            agent: match self.identity {
                AgentToolIdentityChoice::InheritParent => AgentIdentitySelection::InheritParent,
                AgentToolIdentityChoice::DefaultPioneer => AgentIdentitySelection::DefaultPioneer,
                AgentToolIdentityChoice::Exact { id } => AgentIdentitySelection::Exact {
                    agent_identity_id: id,
                },
                AgentToolIdentityChoice::ServerDerivedEphemeral {
                    display_name_hint,
                    role_label,
                } => AgentIdentitySelection::ServerDerivedEphemeral {
                    display_name_hint,
                    role_label,
                },
            },
            execution: crate::AgentExecutionSelection {
                profile: match self.profile {
                    AgentToolProfileChoice::InheritParent => {
                        AgentExecutionProfileSelection::InheritParent
                    }
                    AgentToolProfileChoice::Exact(id) => {
                        AgentExecutionProfileSelection::Exact { profile_id: id }
                    }
                },
                reasoning: self.reasoning,
                permission_profile: self.permission_profile,
                skill_ids: self.skill_ids,
                mcp_server_ids: self.mcp_server_ids,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentSendMessageToolInput {
    pub target_option_id: String,
    pub input: AgentAuthoredInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentCreateThreadToolInput {
    pub option_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentStartToolInput {
    pub target_option_id: String,
    pub input: AgentAuthoredInput,
    /// Both fields are required; the server still validates them against the
    /// immutable launch grant and the current runtime capability snapshot.
    pub launch: AgentToolLaunchSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentTaskToolInput {
    pub target_option_id: String,
    pub task_template_id: String,
    pub launch: AgentToolLaunchSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentScheduleTaskToolInput {
    pub target_option_id: String,
    pub task_template_id: String,
    pub schedule_option_id: String,
    pub launch: AgentToolLaunchSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentReviewTaskToolInput {
    pub task_id: String,
    pub decision: AgentReviewDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentControlTaskToolInput {
    pub task_id: String,
    pub control: AgentTaskControl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentWaitToolInput {
    #[schemars(length(min = 1, max = 64))]
    pub task_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0, max = 30000))]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentResultToolInput {
    pub task_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentToolSafeResult {
    pub status: AgentToolResultStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<AgentPublicOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolResultStatus {
    Accepted,
    Queued,
    Completed,
    Denied,
    Failed,
}

/// Stable, non-disclosing public outcomes for agent domain operations.
/// Internal diagnostics may be more precise, but public adapters use only
/// these bounded codes and never turn target failures into an existence oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentPublicOutcome {
    AgentActorUnavailable,
    AgentIdentityUnavailable,
    AgentIdentityRevisionStale,
    AgentExecutionStale,
    AgentExecutionProfileUnavailable,
    AgentExecutionSelectionNotAllowed,
    AgentNicknameUnavailable,
    AgentActionNotAllowed,
    AgentRouteRequired,
    AgentRouteRevoked,
    AgentRouteExpired,
    AgentDestinationUnavailable,
    AgentContextExportDenied,
    AgentActionConflict,
    AgentActionAlreadyCommitted,
    AgentRecoveryQuarantined,
    AgentWorkQueued,
    AgentWorkQueueFull,
    AgentWorkGraphLimitExceeded,
    AgentActionPayloadLimitExceeded,
    AgentRuntimeIntegrityLost,
}

impl AgentPublicOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AgentActorUnavailable => "agent_actor_unavailable",
            Self::AgentIdentityUnavailable => "agent_identity_unavailable",
            Self::AgentIdentityRevisionStale => "agent_identity_revision_stale",
            Self::AgentExecutionStale => "agent_execution_stale",
            Self::AgentExecutionProfileUnavailable => "agent_execution_profile_unavailable",
            Self::AgentExecutionSelectionNotAllowed => "agent_execution_selection_not_allowed",
            Self::AgentNicknameUnavailable => "agent_nickname_unavailable",
            Self::AgentActionNotAllowed => "agent_action_not_allowed",
            Self::AgentRouteRequired => "agent_route_required",
            Self::AgentRouteRevoked => "agent_route_revoked",
            Self::AgentRouteExpired => "agent_route_expired",
            Self::AgentDestinationUnavailable => "agent_destination_unavailable",
            Self::AgentContextExportDenied => "agent_context_export_denied",
            Self::AgentActionConflict => "agent_action_conflict",
            Self::AgentActionAlreadyCommitted => "agent_action_already_committed",
            Self::AgentRecoveryQuarantined => "agent_recovery_quarantined",
            Self::AgentWorkQueued => "agent_work_queued",
            Self::AgentWorkQueueFull => "agent_work_queue_full",
            Self::AgentWorkGraphLimitExceeded => "agent_work_graph_limit_exceeded",
            Self::AgentActionPayloadLimitExceeded => "agent_action_payload_limit_exceeded",
            Self::AgentRuntimeIntegrityLost => "agent_runtime_integrity_lost",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentModelToolCatalogEntry {
    pub name: AgentModelToolName,
    pub description: String,
    pub parameters: JsonValue,
    pub capability: AgentToolCapability,
}

/// Build the capability-filtered tool catalog.  No entry is returned for a
/// missing capability; callers must not replace a missing tool with a prose
/// fallback that pretends the action succeeded.
pub fn project_agent_model_tool_catalog(
    capabilities: &BTreeSet<AgentToolCapability>,
    options: Option<&AgentToolOptionsProjection>,
) -> Vec<AgentModelToolCatalogEntry> {
    let mut entries = Vec::new();
    let mut push = |name: AgentModelToolName,
                    capability: AgentToolCapability,
                    description: &str,
                    parameters: JsonValue| {
        if capabilities.contains(&capability) {
            entries.push(AgentModelToolCatalogEntry {
                name,
                description: description.to_owned(),
                parameters,
                capability,
            });
        }
    };
    push(
        AgentModelToolName::SendMessage,
        AgentToolCapability::MessageCreate,
        "Send authored content to a server-approved target.",
        schema::<AgentSendMessageToolInput>(),
    );
    push(
        AgentModelToolName::CreateThread,
        AgentToolCapability::ThreadCreate,
        "Create a server-approved thread.",
        schema::<AgentCreateThreadToolInput>(),
    );
    if options.is_some() {
        push(
            AgentModelToolName::AgentStartOptions,
            AgentToolCapability::ChildStart,
            "List identities and execution profiles currently available for agent_start.",
            schema::<AgentStartOptionsToolInput>(),
        );
    }
    if options.is_some() {
        push(
            AgentModelToolName::StartAgent,
            AgentToolCapability::ChildStart,
            "Start an agent using one identity and one execution profile from agent_start_options.",
            schema::<AgentStartToolInput>(),
        );
    }
    // Task operations are projected by the established TaskToolProvider.
    // That provider owns richer task_create/accept/revise/cancel/wait schemas
    // and now attaches the same execution-bound AgentAction transaction.
    // Projecting a second catalog here would create duplicate tool names and
    // two competing domain adapters.
    entries.sort_by_key(|entry| entry.name);
    entries
}

pub fn schema<T: JsonSchema>() -> JsonValue {
    serde_json::to_value(schemars::schema_for!(T)).expect("agent tool schema must serialize")
}

impl AgentStartOptionsProjection {
    /// Strip implementation details before the catalog is handed to a model.
    pub fn safe_tool_projection(&self) -> AgentToolOptionsProjection {
        let identities = self
            .agents
            .iter()
            .map(|identity| AgentToolIdentityOption {
                id: identity.id.clone(),
                display_name: identity.display_name.clone(),
                nickname: identity.nickname.clone(),
                avatar_revision: identity.avatar_revision.clone(),
                role_label: identity.role_label.clone(),
                source_kind: identity.source_kind,
            })
            .collect::<Vec<_>>();
        let visible_identity_ids = identities
            .iter()
            .map(|identity| identity.id.clone())
            .collect::<BTreeSet<_>>();
        let profiles = self
            .profiles
            .iter()
            .filter_map(|profile| {
                let compatible_identity_ids = profile
                    .compatible_agent_identity_ids
                    .iter()
                    .filter(|id| visible_identity_ids.contains(*id))
                    .cloned()
                    .collect::<Vec<_>>();
                (!compatible_identity_ids.is_empty()).then(|| AgentToolProfileOption {
                    id: profile.id.clone(),
                    compatible_identity_ids,
                    backend: backend_kind(profile.backend.clone()),
                    allowed_reasoning: profile.allowed_reasoning.clone(),
                    allowed_permission_profiles: profile
                        .allowed_permission_profiles
                        .iter()
                        .copied()
                        .filter(|mode| {
                            let selected = crate::task_permission_cap_snapshot(
                                &crate::task_permission_cap_for_mode(*mode),
                            );
                            let ceiling =
                                crate::task_permission_cap_snapshot(&self.max_permission_profile);
                            crate::intersect_turn_permission_profiles(
                                &selected,
                                &ceiling,
                                crate::TurnPermissionProfileSource::TaskPermissionCap,
                            ) == selected
                        })
                        .collect(),
                })
            })
            .collect::<Vec<_>>();
        AgentToolOptionsProjection {
            identities,
            profiles,
            inherit_parent_identity_available: self.inherit_parent_agent_available,
            default_pioneer_identity_available: self.agents.iter().any(|identity| {
                identity.source_kind == crate::AgentIdentitySourceKind::NativeAgent
                    && identity.nickname.eq_ignore_ascii_case("pioneer")
            }),
            derived_ephemeral_identity_available: self.derived_ephemeral_available,
            inherit_parent_profile_available: self.inherit_parent_profile_available,
            allowed_skill_ids: self.allowed_skill_ids.clone(),
            allowed_mcp_server_ids: self.allowed_mcp_server_ids.clone(),
            max_permission_profile: self.max_permission_profile.clone(),
            thread_creation_options: Vec::new(),
            target_options: Vec::new(),
            generation_fingerprint: self.generation_fingerprint.clone(),
        }
    }
}

fn backend_kind(backend: AgentExecutionProfileBackend) -> AgentToolBackendKind {
    match backend {
        AgentExecutionProfileBackend::ApiProvider => AgentToolBackendKind::Api,
        AgentExecutionProfileBackend::CliRuntime { .. } => AgentToolBackendKind::Cli,
        AgentExecutionProfileBackend::AcpAgentRuntime { .. } => AgentToolBackendKind::Acp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentExecutionProfileBackend, AgentExecutionProfileId, AgentExecutionProfileProjection,
        AgentIdentityId, AgentIdentityProjection, AgentIdentitySourceKind,
        AgentStartOptionsProjection,
    };
    use serde_json::json;

    fn options() -> AgentToolOptionsProjection {
        AgentToolOptionsProjection {
            identities: vec![AgentToolIdentityOption {
                id: AgentIdentityId::new("A12345678901234567890").unwrap(),
                display_name: "A".to_owned(),
                nickname: "a".to_owned(),
                avatar_revision: None,
                role_label: None,
                source_kind: AgentIdentitySourceKind::NativeAgent,
            }],
            profiles: vec![AgentToolProfileOption {
                id: AgentExecutionProfileId::new("P12345678901234567890").unwrap(),
                compatible_identity_ids: vec![
                    AgentIdentityId::new("A12345678901234567890").unwrap(),
                ],
                backend: AgentToolBackendKind::Api,
                allowed_reasoning: Vec::new(),
                allowed_permission_profiles: Vec::new(),
            }],
            inherit_parent_identity_available: true,
            default_pioneer_identity_available: false,
            derived_ephemeral_identity_available: true,
            inherit_parent_profile_available: true,
            allowed_skill_ids: Vec::new(),
            allowed_mcp_server_ids: Vec::new(),
            max_permission_profile: crate::task_permission_cap_for_mode(
                crate::TurnPermissionMode::Supervised,
            ),
            thread_creation_options: Vec::new(),
            target_options: Vec::new(),
            generation_fingerprint: "generation".to_owned(),
        }
    }

    #[test]
    fn catalog_is_capability_filtered_and_missing_capability_is_not_simulated() {
        let mut capabilities = BTreeSet::new();
        capabilities.insert(AgentToolCapability::ChildStart);
        let catalog = project_agent_model_tool_catalog(&capabilities, Some(&options()));
        assert!(
            catalog
                .iter()
                .any(|entry| entry.name == AgentModelToolName::StartAgent)
        );
        assert!(
            !catalog
                .iter()
                .any(|entry| entry.name == AgentModelToolName::SendMessage)
        );
    }

    #[test]
    fn agent_start_options_accepts_no_model_controlled_selector() {
        let mut capabilities = BTreeSet::new();
        capabilities.insert(AgentToolCapability::ChildStart);
        let catalog = project_agent_model_tool_catalog(&capabilities, Some(&options()));
        let entry = catalog
            .iter()
            .find(|entry| entry.name == AgentModelToolName::AgentStartOptions)
            .expect("agent_start_options should be projected");
        assert_eq!(
            entry.parameters.get("additionalProperties"),
            Some(&json!(false))
        );
        let schema = entry.parameters.to_string();
        assert!(!schema.contains("generationFingerprint"));
        assert!(!schema.contains("identities"));
        assert!(!schema.contains("profiles"));
        assert!(
            serde_json::from_value::<AgentStartOptionsToolInput>(serde_json::json!({})).is_ok()
        );
        assert!(
            serde_json::from_value::<AgentStartOptionsToolInput>(serde_json::json!({
                "actor": "forged"
            }))
            .is_err()
        );
    }

    #[test]
    fn public_outcome_codes_are_stable_and_unique() {
        let outcomes = [
            AgentPublicOutcome::AgentActorUnavailable,
            AgentPublicOutcome::AgentIdentityUnavailable,
            AgentPublicOutcome::AgentIdentityRevisionStale,
            AgentPublicOutcome::AgentExecutionStale,
            AgentPublicOutcome::AgentExecutionProfileUnavailable,
            AgentPublicOutcome::AgentExecutionSelectionNotAllowed,
            AgentPublicOutcome::AgentNicknameUnavailable,
            AgentPublicOutcome::AgentActionNotAllowed,
            AgentPublicOutcome::AgentRouteRequired,
            AgentPublicOutcome::AgentRouteRevoked,
            AgentPublicOutcome::AgentRouteExpired,
            AgentPublicOutcome::AgentDestinationUnavailable,
            AgentPublicOutcome::AgentContextExportDenied,
            AgentPublicOutcome::AgentActionConflict,
            AgentPublicOutcome::AgentActionAlreadyCommitted,
            AgentPublicOutcome::AgentRecoveryQuarantined,
            AgentPublicOutcome::AgentWorkQueued,
            AgentPublicOutcome::AgentWorkQueueFull,
            AgentPublicOutcome::AgentWorkGraphLimitExceeded,
            AgentPublicOutcome::AgentActionPayloadLimitExceeded,
            AgentPublicOutcome::AgentRuntimeIntegrityLost,
        ];
        let codes = outcomes
            .iter()
            .map(|outcome| outcome.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(codes.len(), outcomes.len());
        for outcome in outcomes {
            assert_eq!(
                serde_json::to_value(outcome).unwrap(),
                serde_json::Value::String(outcome.as_str().to_owned())
            );
        }
    }

    #[test]
    fn safe_options_strip_provider_model_runtime_and_credentials() {
        let identity = AgentIdentityProjection::new(
            AgentIdentityId::new("A12345678901234567890").unwrap(),
            AgentIdentitySourceKind::CliRuntimeInstance,
            "CLI",
            "cli",
            None,
            None,
            1,
            "runtime-secret-fingerprint",
        )
        .unwrap();
        let profile = AgentExecutionProfileProjection {
            id: AgentExecutionProfileId::new("P12345678901234567890").unwrap(),
            compatible_agent_identity_ids: vec![identity.id.clone()],
            backend: AgentExecutionProfileBackend::CliRuntime {
                runtime_instance_id: "runtime-secret".to_owned(),
            },
            provider_id: "provider-secret".to_owned(),
            model_id: "model-secret".to_owned(),
            provider_display_name: "Provider".to_owned(),
            model_display_name: "Model".to_owned(),
            allowed_reasoning: Vec::new(),
            allowed_permission_profiles: Vec::new(),
            catalog_generation: 1,
            policy_generation: 1,
            fingerprint: "profile-secret".to_owned(),
        };
        let source = AgentStartOptionsProjection {
            agents: vec![identity],
            inherit_parent_agent_available: true,
            derived_ephemeral_available: true,
            profiles: vec![profile],
            inherit_parent_profile_available: true,
            allowed_skill_ids: Vec::new(),
            allowed_mcp_server_ids: Vec::new(),
            max_permission_profile: crate::task_permission_cap_for_mode(
                crate::TurnPermissionMode::Supervised,
            ),
            generation_fingerprint: "options-secret".to_owned(),
        };
        let safe = source.safe_tool_projection();
        let encoded = serde_json::to_string(&safe).unwrap();
        for forbidden in [
            "provider-secret",
            "model-secret",
            "runtime-secret",
            "runtime-secret-fingerprint",
            "credentials",
            "session_id",
            "binary_path",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "found forbidden field {forbidden}"
            );
        }
        assert_eq!(safe.profiles.len(), 1);
    }

    #[test]
    fn start_input_rejects_actor_and_raw_provider_fields() {
        let value = json!({
            "target": "current_thread",
            "input": [],
            "launch": {
                "identity": { "kind": "exact", "id": "A12345678901234567890" },
                "profile": { "kind": "exact", "id": "P12345678901234567890" },
                "actor": "forged",
                "provider": "openai",
                "model": "o4-mini"
            }
        });
        assert!(serde_json::from_value::<AgentStartToolInput>(value).is_err());

        let nested_profile_injection = json!({
            "targetOptionId": "current-thread",
            "input": [],
            "launch": {
                "identity": { "kind": "exact", "id": "A12345678901234567890" },
                "profile": {
                    "kind": "exact",
                    "id": "P12345678901234567890",
                    "provider": "openai"
                }
            }
        });
        assert!(serde_json::from_value::<AgentStartToolInput>(nested_profile_injection).is_err());
    }

    #[test]
    fn launch_choice_converts_only_to_inherit_or_exact_server_selection() {
        let selection = AgentToolLaunchSelection {
            identity: AgentToolIdentityChoice::Exact {
                id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            },
            profile: AgentToolProfileChoice::InheritParent,
            reasoning: None,
            permission_profile: None,
            skill_ids: Vec::new(),
            mcp_server_ids: Vec::new(),
        };
        let launch = selection.into_server_selection();
        assert!(matches!(launch.agent, AgentIdentitySelection::Exact { .. }));
        assert!(matches!(
            launch.execution.profile,
            AgentExecutionProfileSelection::InheritParent
        ));
    }
}
