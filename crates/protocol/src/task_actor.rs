//! Durable actor, occurrence and delivery facts for agent domain Tasks.
//!
//! Task rows describe the user-facing job.  These contracts keep the exact
//! author, launch intent, work-graph root and delivery authority beside that
//! job so a scheduler or recovery worker never has to infer them from the
//! current user, latest message, provider or runtime process.

use crate::{
    AgentExecutionProfileProjection, AgentIdentityProjection, AgentLaunchSelection,
    AgentPresentationSnapshot, PersistedActorRef,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

const TASK_DERIVED_CHILD_LAUNCH_GRANT_MAX_BYTES: usize = 131_072;

fn validate_task_route_receipt(
    receipt_json: &str,
    expected_route_id: &str,
    allowed_actions: &[&str],
) -> Result<(), TaskActorContractError> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(receipt_json) else {
        return Err(TaskActorContractError::InvalidRouteReceipt);
    };
    let Some(object) = value.as_object() else {
        return Err(TaskActorContractError::InvalidRouteReceipt);
    };
    let valid_generation = |field: &str| {
        value
            .get(field)
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|generation| generation > 0 && i64::try_from(generation).is_ok())
    };
    if object.len() != 5
        || expected_route_id.trim().is_empty()
        || value.get("routeId").and_then(serde_json::Value::as_str) != Some(expected_route_id)
        || !valid_generation("routeGeneration")
        || !valid_generation("sourcePolicyGeneration")
        || !valid_generation("destinationPolicyGeneration")
        || value
            .get("action")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|action| !allowed_actions.contains(&action))
    {
        return Err(TaskActorContractError::InvalidRouteReceipt);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskReviewerIntent {
    Parent,
    Human { principal_id: String },
    Agent { execution_id: String },
    RuntimeAuto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskDerivedChildLaunchGrant {
    ResolvedTaskLaunch {
        identity: AgentIdentityProjection,
        profile: AgentExecutionProfileProjection,
        role_key: String,
        agent_policy_generation: u64,
        allowed_actions: Vec<String>,
        agent_authorization_fingerprint: String,
        child_launch_grant: crate::ChildAgentLaunchGrantSet,
    },
}

/// Migrates the nested child-launch contract through every registered adjacent
/// version and returns a JSON document that current runtime code can consume.
pub fn migrate_task_derived_child_launch_grant_json_to_current(
    json: &str,
) -> Result<String, TaskDerivedChildLaunchGrantMigrationError> {
    if json.len() > TASK_DERIVED_CHILD_LAUNCH_GRANT_MAX_BYTES {
        return Err(
            TaskDerivedChildLaunchGrantMigrationError::InvalidCurrentContract(format!(
                "task launch contract exceeds {} bytes",
                TASK_DERIVED_CHILD_LAUNCH_GRANT_MAX_BYTES
            )),
        );
    }
    let migrated = crate::migrate_embedded_child_launch_grant_json_to_current(json)
        .map_err(TaskDerivedChildLaunchGrantMigrationError::ChildGrant)?;
    let current =
        serde_json::from_str::<TaskDerivedChildLaunchGrant>(&migrated).map_err(|error| {
            TaskDerivedChildLaunchGrantMigrationError::InvalidCurrentContract(error.to_string())
        })?;
    current.validate_embedded().map_err(|error| {
        TaskDerivedChildLaunchGrantMigrationError::InvalidCurrentContract(format!("{error:?}"))
    })?;
    // Task actor contracts compare their immutable JSON on idempotent insert.
    // Re-encode through the current typed outer contract so an upcast V1 row
    // has exactly the same representation as a newly produced V2 row. The
    // generic embedded migrator cannot do this because AgentExecution grants
    // intentionally have a different, open outer document.
    let encoded = serde_json::to_string(&current).map_err(|error| {
        TaskDerivedChildLaunchGrantMigrationError::InvalidCurrentContract(error.to_string())
    })?;
    if encoded.len() > TASK_DERIVED_CHILD_LAUNCH_GRANT_MAX_BYTES {
        return Err(
            TaskDerivedChildLaunchGrantMigrationError::InvalidCurrentContract(format!(
                "migrated task launch contract exceeds {} bytes",
                TASK_DERIVED_CHILD_LAUNCH_GRANT_MAX_BYTES
            )),
        );
    }
    Ok(encoded)
}

#[derive(Debug)]
pub enum TaskDerivedChildLaunchGrantMigrationError {
    ChildGrant(crate::ChildAgentLaunchGrantMigrationError),
    InvalidCurrentContract(String),
}

impl fmt::Display for TaskDerivedChildLaunchGrantMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChildGrant(error) => write!(formatter, "{error}"),
            Self::InvalidCurrentContract(message) => {
                write!(
                    formatter,
                    "invalid migrated task launch contract: {message}"
                )
            }
        }
    }
}

impl std::error::Error for TaskDerivedChildLaunchGrantMigrationError {}

impl TaskDerivedChildLaunchGrant {
    fn validate_embedded(&self) -> Result<(), TaskActorContractError> {
        let Self::ResolvedTaskLaunch {
            identity,
            profile,
            role_key,
            agent_policy_generation,
            allowed_actions,
            agent_authorization_fingerprint,
            child_launch_grant,
        } = self;
        if role_key.trim().is_empty()
            || *agent_policy_generation == 0
            || allowed_actions.is_empty()
            || agent_authorization_fingerprint.len() != 64
            || !agent_authorization_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(TaskActorContractError::InvalidResolvedLaunchGrant);
        }
        child_launch_grant
            .validate()
            .map_err(|_| TaskActorContractError::InvalidResolvedLaunchGrant)?;
        if !child_launch_grant
            .identities
            .iter()
            .any(|candidate| candidate == identity)
            || !child_launch_grant
                .profiles
                .iter()
                .any(|candidate| candidate == profile)
            || (identity.source_kind != crate::AgentIdentitySourceKind::Ephemeral
                && !profile
                    .compatible_agent_identity_ids
                    .iter()
                    .any(|candidate| candidate == &identity.id))
        {
            return Err(TaskActorContractError::ResolvedLaunchGrantMismatch);
        }
        let mut unique_actions = allowed_actions.clone();
        unique_actions.sort();
        unique_actions.dedup();
        if unique_actions != *allowed_actions
            || allowed_actions
                .iter()
                .any(|action| action.trim().is_empty())
        {
            return Err(TaskActorContractError::InvalidResolvedLaunchGrant);
        }
        Ok(())
    }

    fn validate_for(&self, contract: &TaskActorContract) -> Result<(), TaskActorContractError> {
        let Self::ResolvedTaskLaunch {
            identity,
            profile,
            child_launch_grant,
            ..
        } = self;
        if contract.resolved_identity_id.as_deref() != Some(identity.id.as_str())
            || contract.resolved_profile_id.as_deref() != Some(profile.id.as_str())
            || contract.source_config_fingerprint.as_deref()
                != Some(identity.source_fingerprint.as_str())
        {
            return Err(TaskActorContractError::ResolvedLaunchGrantMismatch);
        }
        self.validate_embedded()?;
        let launch = contract
            .launch
            .as_ref()
            .ok_or(TaskActorContractError::ResolvedLaunchGrantMismatch)?;
        let identity_selection_allowed = match &launch.agent {
            crate::AgentIdentitySelection::InheritParent => {
                child_launch_grant.allow_inherit_parent_identity
            }
            crate::AgentIdentitySelection::ServerDerivedEphemeral { .. } => {
                child_launch_grant.allow_server_derived_ephemeral
                    && identity.source_kind == crate::AgentIdentitySourceKind::Ephemeral
            }
            crate::AgentIdentitySelection::DefaultPioneer
            | crate::AgentIdentitySelection::Exact { .. } => true,
        };
        let profile_selection_allowed = match &launch.execution.profile {
            crate::AgentExecutionProfileSelection::InheritParent => {
                child_launch_grant.allow_inherit_parent_profile
            }
            crate::AgentExecutionProfileSelection::Exact { profile_id } => {
                profile_id == &profile.id
            }
        };
        let permission_selection_allowed =
            launch
                .execution
                .permission_profile
                .as_ref()
                .is_none_or(|selection| {
                    let selected = crate::task_permission_cap_snapshot(
                        &crate::task_permission_cap_for_mode(selection.mode),
                    );
                    let ceiling = crate::task_permission_cap_snapshot(
                        &child_launch_grant.max_permission_profile,
                    );
                    crate::intersect_turn_permission_profiles(
                        &selected,
                        &ceiling,
                        crate::TurnPermissionProfileSource::TaskPermissionCap,
                    ) == selected
                });
        let reasoning_selection_allowed =
            launch.execution.reasoning.as_ref().is_none_or(|selection| {
                child_launch_grant.max_reasoning.allowed.contains(selection)
                    && profile.allowed_reasoning.contains(selection)
            });
        if !identity_selection_allowed
            || !profile_selection_allowed
            || !permission_selection_allowed
            || !reasoning_selection_allowed
            || launch
                .execution
                .skill_ids
                .iter()
                .any(|id| !child_launch_grant.skill_ids.contains(id))
            || launch
                .execution
                .mcp_server_ids
                .iter()
                .any(|id| !child_launch_grant.mcp_server_ids.contains(id))
        {
            return Err(TaskActorContractError::ResolvedLaunchGrantMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskDeliveryActorContract {
    pub enabled: bool,
    pub destination_thread_id: Option<String>,
    pub destination_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_webhook_url_fingerprint: Option<String>,
    pub route_id: Option<String>,
    pub return_route_id: Option<String>,
    pub author_snapshot: Option<AgentPresentationSnapshot>,
    pub route_receipt_json: Option<String>,
    pub disclosure_generation: u64,
    pub route_expires_at_millis: Option<i64>,
}

impl TaskDeliveryActorContract {
    pub fn validate(&self) -> Result<(), TaskActorContractError> {
        if !self.enabled {
            if self.destination_thread_id.is_some()
                || self.destination_user_id.is_some()
                || self.destination_webhook_url_fingerprint.is_some()
                || self.route_id.is_some()
                || self.return_route_id.is_some()
                || self.author_snapshot.is_some()
                || self.route_receipt_json.is_some()
                || self.route_expires_at_millis.is_some()
            {
                return Err(TaskActorContractError::UnexpectedDeliveryAuthority);
            }
            return Ok(());
        }
        let destination_count = usize::from(self.destination_thread_id.is_some())
            + usize::from(self.destination_user_id.is_some())
            + usize::from(self.destination_webhook_url_fingerprint.is_some());
        if destination_count == 0 {
            return Err(TaskActorContractError::MissingDeliveryDestination);
        }
        if destination_count != 1 {
            return Err(TaskActorContractError::ConflictingDeliveryDestinations);
        }
        if self
            .destination_webhook_url_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                fingerprint.len() != 64
                    || !fingerprint
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            })
        {
            return Err(TaskActorContractError::InvalidDeliveryDestination);
        }
        if self.route_id.is_none()
            && (self.return_route_id.is_some()
                || self.route_receipt_json.is_some()
                || self.route_expires_at_millis.is_some())
        {
            return Err(TaskActorContractError::RouteFactsWithoutRoute);
        }
        // A result-return route authorizes crossing back into the destination
        // capsule.  The final delivery surface can be either that capsule's
        // Thread or the initiating collaborator's durable inbox.  Webhooks are
        // a separate external surface and never inherit thread-route authority.
        if self.route_id.is_some() && self.destination_webhook_url_fingerprint.is_some() {
            return Err(TaskActorContractError::InvalidDeliveryDestination);
        }
        if self.route_id.is_some() && self.route_receipt_json.is_none() {
            return Err(TaskActorContractError::MissingDeliveryRouteReceipt);
        }
        if self
            .route_receipt_json
            .as_deref()
            .is_some_and(|value| value.len() > 16_384)
        {
            return Err(TaskActorContractError::OversizedField("route_receipt_json"));
        }
        if let (Some(route_id), Some(receipt_json)) =
            (self.route_id.as_deref(), self.route_receipt_json.as_deref())
        {
            validate_task_route_receipt(receipt_json, route_id, &["deliver_result"])?;
        }
        Ok(())
    }

    pub fn validate_at(
        &self,
        now_millis: i64,
        current_disclosure_generation: u64,
    ) -> Result<(), TaskActorContractError> {
        self.validate()?;
        if self.disclosure_generation != current_disclosure_generation {
            return Err(TaskActorContractError::DisclosureGenerationChanged);
        }
        if self
            .route_expires_at_millis
            .is_some_and(|expires_at| expires_at <= now_millis)
        {
            return Err(TaskActorContractError::RouteExpired);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskActorContract {
    pub task_id: String,
    pub workspace_id: String,
    pub creator: PersistedActorRef,
    pub creator_presentation_snapshot: Option<AgentPresentationSnapshot>,
    pub reviewer: TaskReviewerIntent,
    /// Exact capsule/thread in which an occurrence executes. For ordinary
    /// Tasks this is absent and the admitted collaboration root is used.
    /// Routed Tasks pin both fields at create/schedule time so the scheduler
    /// never infers a destination from current membership or prompt text.
    pub execution_destination_thread_id: Option<String>,
    pub execution_route_id: Option<String>,
    pub execution_route_receipt_json: Option<String>,
    pub execution_route_expires_at_millis: Option<i64>,
    pub delivery: TaskDeliveryActorContract,
    pub launch: Option<AgentLaunchSelection>,
    pub requested_identity_json: Option<String>,
    pub resolved_identity_id: Option<String>,
    pub resolved_profile_id: Option<String>,
    pub source_config_fingerprint: Option<String>,
    pub derived_child_launch_grant_json: Option<String>,
    /// Immutable graph lineage of an Agent creator. This remains distinct
    /// from the occurrence root: scheduled occurrences create a fresh root
    /// when admitted, while immediate Agent Tasks inherit the creator root.
    pub creator_work_graph_root_execution_id: Option<String>,
    pub work_graph_root_execution_id: Option<String>,
    pub root_resource_scope_id: Option<String>,
    pub accounting_attribution: Option<PersistedActorRef>,
    pub controller_principal_id: Option<String>,
    pub revision: u64,
}

impl TaskActorContract {
    pub fn validate(&self) -> Result<(), TaskActorContractError> {
        for (field, value) in [
            ("task_id", self.task_id.as_str()),
            ("workspace_id", self.workspace_id.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(TaskActorContractError::MissingField(field));
            }
        }
        if self.revision == 0 {
            return Err(TaskActorContractError::InvalidRevision);
        }
        if self.execution_route_id.is_none()
            && (self.execution_route_receipt_json.is_some()
                || self.execution_route_expires_at_millis.is_some())
        {
            return Err(TaskActorContractError::RouteFactsWithoutRoute);
        }
        if self.execution_route_id.is_some() && self.execution_destination_thread_id.is_none() {
            return Err(TaskActorContractError::MissingExecutionRouteDestination);
        }
        if self.execution_route_id.is_some() && self.execution_route_receipt_json.is_none() {
            return Err(TaskActorContractError::MissingExecutionRouteReceipt);
        }
        if self
            .execution_route_receipt_json
            .as_deref()
            .is_some_and(|value| value.len() > 16_384)
        {
            return Err(TaskActorContractError::OversizedField(
                "execution_route_receipt_json",
            ));
        }
        if let (Some(route_id), Some(receipt_json)) = (
            self.execution_route_id.as_deref(),
            self.execution_route_receipt_json.as_deref(),
        ) {
            validate_task_route_receipt(receipt_json, route_id, &["create_task", "schedule_task"])?;
        }
        if self
            .source_config_fingerprint
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(TaskActorContractError::MissingField(
                "source_config_fingerprint",
            ));
        }
        match (&self.launch, self.requested_identity_json.as_deref()) {
            (None, None) => {}
            (Some(launch), Some(requested_identity_json)) => {
                if requested_identity_json.len() > 16_384 {
                    return Err(TaskActorContractError::OversizedField(
                        "requested_identity_json",
                    ));
                }
                let requested_identity: crate::AgentIdentitySelection =
                    serde_json::from_str(requested_identity_json)
                        .map_err(|_| TaskActorContractError::InvalidRequestedIdentity)?;
                if requested_identity != launch.agent {
                    return Err(TaskActorContractError::RequestedIdentityMismatch);
                }
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(TaskActorContractError::IncompleteRequestedLaunch);
            }
        }
        match (&self.creator, &self.creator_presentation_snapshot) {
            (PersistedActorRef::AgentExecution(execution_id), Some(snapshot))
                if &snapshot.agent_execution_id == execution_id => {}
            (PersistedActorRef::AgentExecution(_), None) => {
                return Err(TaskActorContractError::MissingAgentCreatorSnapshot);
            }
            (PersistedActorRef::AgentExecution(_), Some(_)) => {
                return Err(TaskActorContractError::AgentCreatorSnapshotMismatch);
            }
            (PersistedActorRef::Principal(_) | PersistedActorRef::System, Some(_)) => {
                return Err(TaskActorContractError::UnexpectedAgentCreatorSnapshot);
            }
            (PersistedActorRef::Principal(_) | PersistedActorRef::System, None) => {}
        }
        if self.work_graph_root_execution_id != self.root_resource_scope_id {
            return Err(TaskActorContractError::GraphResourceRootMismatch);
        }
        match &self.creator {
            PersistedActorRef::AgentExecution(_) => {
                let Some(creator_root) = self.creator_work_graph_root_execution_id.as_deref()
                else {
                    return Err(TaskActorContractError::MissingAgentCreatorGraphRoot);
                };
                if self
                    .work_graph_root_execution_id
                    .as_deref()
                    .is_some_and(|occurrence_root| occurrence_root != creator_root)
                {
                    return Err(TaskActorContractError::CreatorGraphRootMismatch);
                }
            }
            PersistedActorRef::Principal(_) | PersistedActorRef::System => {
                if self.creator_work_graph_root_execution_id.is_some() {
                    return Err(TaskActorContractError::UnexpectedCreatorGraphRoot);
                }
                if self.work_graph_root_execution_id.is_some() {
                    return Err(TaskActorContractError::UnexpectedOccurrenceGraphRoot);
                }
            }
        }
        let resolved_fields = [
            self.resolved_identity_id.is_some(),
            self.resolved_profile_id.is_some(),
            self.source_config_fingerprint.is_some(),
            self.derived_child_launch_grant_json.is_some(),
        ];
        if resolved_fields.iter().any(|present| *present)
            && !resolved_fields.iter().all(|present| *present)
        {
            return Err(TaskActorContractError::IncompleteResolvedLaunch);
        }
        if let Some(grant_json) = self.derived_child_launch_grant_json.as_deref() {
            if grant_json.len() > TASK_DERIVED_CHILD_LAUNCH_GRANT_MAX_BYTES {
                return Err(TaskActorContractError::OversizedField(
                    "derived_child_launch_grant_json",
                ));
            }
            serde_json::from_str::<TaskDerivedChildLaunchGrant>(grant_json)
                .map_err(|_| TaskActorContractError::InvalidResolvedLaunchGrant)?
                .validate_for(self)?;
        }
        self.delivery.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskOccurrenceStatus {
    Dormant,
    Queued,
    Recovering,
    Running,
    WaitingReview,
    Delivered,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskOccurrenceContract {
    pub occurrence_id: String,
    pub task_id: String,
    pub run_id: String,
    pub trigger_id: Option<String>,
    pub occurrence_key: String,
    pub execution_generation: u64,
    pub agent_execution_id: Option<String>,
    pub work_graph_root_execution_id: Option<String>,
    pub root_resource_scope_id: Option<String>,
    pub status: TaskOccurrenceStatus,
    pub queue_position: Option<u64>,
    pub retry_attempt: u32,
    pub action_idempotency_key: String,
    pub route_id: Option<String>,
    pub result_return_route_id: Option<String>,
    pub terminal_reason: Option<String>,
}

impl TaskOccurrenceContract {
    pub fn validate(&self) -> Result<(), TaskActorContractError> {
        for (field, value) in [
            ("occurrence_id", self.occurrence_id.as_str()),
            ("task_id", self.task_id.as_str()),
            ("run_id", self.run_id.as_str()),
            ("occurrence_key", self.occurrence_key.as_str()),
            (
                "action_idempotency_key",
                self.action_idempotency_key.as_str(),
            ),
        ] {
            if value.trim().is_empty() {
                return Err(TaskActorContractError::MissingField(field));
            }
        }
        if self.execution_generation == 0 {
            return Err(TaskActorContractError::InvalidRevision);
        }
        if self.result_return_route_id.is_some() && self.route_id.is_none() {
            return Err(TaskActorContractError::ReturnRouteWithoutRoute);
        }
        if self.work_graph_root_execution_id != self.root_resource_scope_id {
            return Err(TaskActorContractError::GraphResourceRootMismatch);
        }
        if self.agent_execution_id.is_some() && self.work_graph_root_execution_id.is_none() {
            return Err(TaskActorContractError::MissingOccurrenceGraphRoot);
        }
        Ok(())
    }

    /// Retries preserve the same occurrence/action idempotency key.  A new
    /// attempt is an execution detail, not a new logical occurrence.
    pub fn retry(&self, retry_attempt: u32) -> Result<Self, TaskActorContractError> {
        if retry_attempt <= self.retry_attempt {
            return Err(TaskActorContractError::RetryMustAdvance);
        }
        let mut next = self.clone();
        next.retry_attempt = retry_attempt;
        next.status = TaskOccurrenceStatus::Queued;
        next.queue_position = None;
        next.terminal_reason = None;
        Ok(next)
    }

    /// Recurrence creates a fresh execution generation without changing the
    /// persisted actor/profile intent in the parent contract.
    pub fn next_generation(
        &self,
        occurrence_id: impl Into<String>,
        run_id: impl Into<String>,
        occurrence_key: impl Into<String>,
    ) -> Result<Self, TaskActorContractError> {
        let mut next = self.clone();
        next.occurrence_id = occurrence_id.into();
        next.run_id = run_id.into();
        next.occurrence_key = occurrence_key.into();
        next.execution_generation = self
            .execution_generation
            .checked_add(1)
            .ok_or(TaskActorContractError::GenerationOverflow)?;
        next.retry_attempt = 0;
        next.status = TaskOccurrenceStatus::Queued;
        next.terminal_reason = None;
        next.agent_execution_id = None;
        next.work_graph_root_execution_id = None;
        next.root_resource_scope_id = None;
        next.queue_position = None;
        next.action_idempotency_key = format!("task:{}:{}", next.task_id, next.run_id);
        Ok(next)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskActorContractError {
    MissingField(&'static str),
    MissingDeliveryDestination,
    ConflictingDeliveryDestinations,
    InvalidDeliveryDestination,
    UnexpectedDeliveryAuthority,
    MissingDeliveryRouteReceipt,
    InvalidRouteReceipt,
    RouteFactsWithoutRoute,
    MissingExecutionRouteDestination,
    MissingExecutionRouteReceipt,
    ReturnRouteWithoutRoute,
    InvalidRevision,
    RetryMustAdvance,
    GenerationOverflow,
    OversizedField(&'static str),
    DisclosureGenerationChanged,
    RouteExpired,
    MissingAgentCreatorSnapshot,
    AgentCreatorSnapshotMismatch,
    UnexpectedAgentCreatorSnapshot,
    GraphResourceRootMismatch,
    MissingAgentCreatorGraphRoot,
    UnexpectedCreatorGraphRoot,
    CreatorGraphRootMismatch,
    UnexpectedOccurrenceGraphRoot,
    MissingOccurrenceGraphRoot,
    IncompleteRequestedLaunch,
    InvalidRequestedIdentity,
    RequestedIdentityMismatch,
    IncompleteResolvedLaunch,
    InvalidResolvedLaunchGrant,
    ResolvedLaunchGrantMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route_receipt(route_id: &str, action: &str) -> String {
        serde_json::json!({
            "routeId": route_id,
            "routeGeneration": 1,
            "sourcePolicyGeneration": 1,
            "destinationPolicyGeneration": 1,
            "action": action,
        })
        .to_string()
    }

    fn contract() -> TaskActorContract {
        TaskActorContract {
            task_id: "TASK123456789012345".to_owned(),
            workspace_id: "WORK123456789012345".to_owned(),
            creator: PersistedActorRef::System,
            creator_presentation_snapshot: None,
            reviewer: TaskReviewerIntent::RuntimeAuto,
            execution_destination_thread_id: None,
            execution_route_id: None,
            execution_route_receipt_json: None,
            execution_route_expires_at_millis: None,
            delivery: TaskDeliveryActorContract {
                enabled: true,
                destination_thread_id: Some("THREAD1234567890123".to_owned()),
                destination_user_id: None,
                destination_webhook_url_fingerprint: None,
                route_id: None,
                return_route_id: None,
                author_snapshot: None,
                route_receipt_json: None,
                disclosure_generation: 1,
                route_expires_at_millis: None,
            },
            launch: None,
            requested_identity_json: None,
            resolved_identity_id: None,
            resolved_profile_id: None,
            source_config_fingerprint: None,
            derived_child_launch_grant_json: None,
            creator_work_graph_root_execution_id: None,
            work_graph_root_execution_id: None,
            root_resource_scope_id: None,
            accounting_attribution: None,
            controller_principal_id: None,
            revision: 1,
        }
    }

    #[test]
    fn contract_requires_a_destination() {
        let mut value = contract();
        value.delivery.destination_thread_id = None;
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::MissingDeliveryDestination)
        );
    }

    #[test]
    fn contract_accepts_one_exact_webhook_fingerprint() {
        let mut value = contract();
        value.delivery.destination_thread_id = None;
        value.delivery.destination_webhook_url_fingerprint = Some("a".repeat(64));
        assert!(value.validate().is_ok());

        value.delivery.destination_user_id = Some("USER1234567890123456".to_owned());
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::ConflictingDeliveryDestinations)
        );
    }

    #[test]
    fn delivery_route_supports_thread_or_user_notification_but_not_webhook() {
        let mut value = contract();
        value.delivery.destination_thread_id = None;
        value.delivery.destination_user_id = Some("USER1234567890123456".to_owned());
        value.delivery.route_id = Some("ROUTE123456789012345".to_owned());
        value.delivery.route_receipt_json =
            Some(route_receipt("ROUTE123456789012345", "deliver_result"));
        assert!(value.validate().is_ok());

        value.delivery.destination_user_id = None;
        value.delivery.destination_webhook_url_fingerprint = Some("a".repeat(64));
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::InvalidDeliveryDestination)
        );
    }

    #[test]
    fn execution_route_requires_exact_destination_and_receipt() {
        let mut value = contract();
        value.execution_route_id = Some("ROUTE123456789012345".to_owned());
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::MissingExecutionRouteDestination)
        );

        value.execution_destination_thread_id = Some("THREAD1234567890123".to_owned());
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::MissingExecutionRouteReceipt)
        );

        value.execution_route_receipt_json =
            Some(route_receipt("ROUTE123456789012345", "create_task"));
        assert!(value.validate().is_ok());

        value.execution_route_id = None;
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::RouteFactsWithoutRoute)
        );
    }

    #[test]
    fn route_receipts_are_exact_and_action_bound() {
        let mut value = contract();
        value.delivery.route_id = Some("ROUTE123456789012345".to_owned());
        value.delivery.route_receipt_json =
            Some(route_receipt("ROUTE123456789012345", "deliver_result"));
        assert!(value.validate().is_ok());

        value.delivery.route_receipt_json =
            Some(route_receipt("OTHER123456789012345", "deliver_result"));
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::InvalidRouteReceipt)
        );

        value.delivery.route_receipt_json =
            Some(route_receipt("ROUTE123456789012345", "create_task"));
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::InvalidRouteReceipt)
        );
    }

    #[test]
    fn requested_identity_is_exactly_bound_to_launch_selection() {
        let mut value = contract();
        let requested = crate::AgentIdentitySelection::DefaultPioneer;
        value.launch = Some(crate::AgentLaunchSelection {
            agent: requested.clone(),
            execution: crate::AgentExecutionSelection {
                profile: crate::AgentExecutionProfileSelection::Exact {
                    profile_id: crate::AgentExecutionProfileId::new("P12345678901234567890")
                        .unwrap(),
                },
                reasoning: None,
                permission_profile: None,
                skill_ids: Vec::new(),
                mcp_server_ids: Vec::new(),
            },
        });
        value.requested_identity_json = Some(serde_json::to_string(&requested).unwrap());
        assert!(value.validate().is_ok());

        value.requested_identity_json = Some(
            serde_json::to_string(&crate::AgentIdentitySelection::Exact {
                agent_identity_id: crate::AgentIdentityId::new("A12345678901234567890").unwrap(),
            })
            .unwrap(),
        );
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::RequestedIdentityMismatch)
        );

        value.requested_identity_json = None;
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::IncompleteRequestedLaunch)
        );
    }

    #[test]
    fn exact_agent_creator_requires_the_matching_immutable_snapshot() {
        let execution_id = crate::AgentExecutionId::new("EXEC12345678901234567").unwrap();
        let mut value = contract();
        value.creator = PersistedActorRef::AgentExecution(execution_id.clone());
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::MissingAgentCreatorSnapshot)
        );
        value.creator_presentation_snapshot = Some(AgentPresentationSnapshot {
            agent_identity_id: crate::AgentIdentityId::new("AGNT12345678901234567").unwrap(),
            agent_execution_id: execution_id,
            identity_source_kind: crate::AgentIdentitySourceKind::Ephemeral,
            identity_source_revision: 1,
            display_name: "Worker".to_owned(),
            nickname: "worker".to_owned(),
            avatar_revision: None,
            role_label: None,
        });
        value.creator_work_graph_root_execution_id = Some("EXECROOT1234567890123".to_owned());
        value.work_graph_root_execution_id = value.creator_work_graph_root_execution_id.clone();
        value.root_resource_scope_id = value.work_graph_root_execution_id.clone();
        assert!(value.validate().is_ok());

        value.work_graph_root_execution_id = None;
        value.root_resource_scope_id = None;
        assert!(
            value.validate().is_ok(),
            "scheduled Task keeps creator lineage without inheriting its occurrence root"
        );
        value.work_graph_root_execution_id = Some("EXECOTHER123456789012".to_owned());
        value.root_resource_scope_id = value.work_graph_root_execution_id.clone();
        assert_eq!(
            value.validate(),
            Err(TaskActorContractError::CreatorGraphRootMismatch)
        );
    }

    #[test]
    fn retry_preserves_logical_idempotency() {
        let occurrence = TaskOccurrenceContract {
            occurrence_id: "OCC12345678901234567".to_owned(),
            task_id: "TASK123456789012345".to_owned(),
            run_id: "RUN12345678901234567".to_owned(),
            trigger_id: None,
            occurrence_key: "occurrence-key".to_owned(),
            execution_generation: 1,
            agent_execution_id: None,
            work_graph_root_execution_id: None,
            root_resource_scope_id: None,
            status: TaskOccurrenceStatus::Failed,
            queue_position: Some(3),
            retry_attempt: 1,
            action_idempotency_key: "task:occurrence-key".to_owned(),
            route_id: None,
            result_return_route_id: None,
            terminal_reason: Some("temporary saturation".to_owned()),
        };
        let retry = occurrence.retry(2).unwrap();
        assert_eq!(
            retry.action_idempotency_key,
            occurrence.action_idempotency_key
        );
        assert_eq!(retry.status, TaskOccurrenceStatus::Queued);
        assert_eq!(retry.queue_position, None);
    }

    #[test]
    fn delivery_authority_rejects_expiry_and_generation_replay() {
        let mut value = contract();
        value.delivery.route_id = Some("ROUTE123456789012345".to_owned());
        value.delivery.route_receipt_json =
            Some(route_receipt("ROUTE123456789012345", "deliver_result"));
        value.delivery.route_expires_at_millis = Some(100);
        assert_eq!(
            value.delivery.validate_at(100, 1),
            Err(TaskActorContractError::RouteExpired)
        );
        value.delivery.route_expires_at_millis = None;
        value.delivery.disclosure_generation = 2;
        assert_eq!(
            value.delivery.validate_at(100, 1),
            Err(TaskActorContractError::DisclosureGenerationChanged)
        );
    }

    #[test]
    fn recurrence_gets_a_new_generation_without_reusing_action_key() {
        let occurrence = TaskOccurrenceContract {
            occurrence_id: "OCC12345678901234567".to_owned(),
            task_id: "TASK123456789012345".to_owned(),
            run_id: "RUN12345678901234567".to_owned(),
            trigger_id: Some("TRIGGER12345678901".to_owned()),
            occurrence_key: "2026-08-16T00:00:00Z".to_owned(),
            execution_generation: 3,
            agent_execution_id: Some("EXEC1234567890123456".to_owned()),
            work_graph_root_execution_id: Some("ROOT1234567890123456".to_owned()),
            root_resource_scope_id: Some("ROOT1234567890123456".to_owned()),
            status: TaskOccurrenceStatus::Delivered,
            queue_position: None,
            retry_attempt: 0,
            action_idempotency_key: "task:recurring:3".to_owned(),
            route_id: None,
            result_return_route_id: None,
            terminal_reason: None,
        };
        let next = occurrence
            .next_generation(
                "OCC22345678901234567",
                "RUN22345678901234567",
                "2026-08-17T00:00:00Z",
            )
            .unwrap();
        assert_eq!(next.execution_generation, 4);
        assert_eq!(next.retry_attempt, 0);
        assert_eq!(next.status, TaskOccurrenceStatus::Queued);
        assert_ne!(
            next.action_idempotency_key,
            occurrence.action_idempotency_key
        );
        assert_eq!(next.occurrence_key, "2026-08-17T00:00:00Z");
        assert_eq!(next.agent_execution_id, None);
        assert_eq!(next.work_graph_root_execution_id, None);
        assert_eq!(next.root_resource_scope_id, None);
    }

    #[test]
    fn recurrence_fails_closed_when_execution_generation_is_exhausted() {
        let occurrence = TaskOccurrenceContract {
            occurrence_id: "OCC12345678901234567".to_owned(),
            task_id: "TASK123456789012345".to_owned(),
            run_id: "RUN12345678901234567".to_owned(),
            trigger_id: None,
            occurrence_key: "2026-08-16T00:00:00Z".to_owned(),
            execution_generation: u64::MAX,
            agent_execution_id: None,
            work_graph_root_execution_id: None,
            root_resource_scope_id: None,
            status: TaskOccurrenceStatus::Queued,
            queue_position: None,
            retry_attempt: 0,
            action_idempotency_key: "task:recurring:max".to_owned(),
            route_id: None,
            result_return_route_id: None,
            terminal_reason: None,
        };

        assert_eq!(
            occurrence.next_generation(
                "OCC22345678901234567",
                "RUN22345678901234567",
                "2026-08-17T00:00:00Z",
            ),
            Err(TaskActorContractError::GenerationOverflow)
        );
    }
}
