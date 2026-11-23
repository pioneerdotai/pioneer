//! Execution-owned admission and scheduling of durable agent work graphs.

use anyhow::{Context, Result, bail};
use chrono::Duration;
use pioneer_crud_access::actor_nickname::{
    NICKNAME_OWNER_AGENT, NICKNAME_OWNER_RESERVED, claim_actor_nickname,
};
use pioneer_entity::{
    agent_execution, agent_execution_resource_state, agent_running_permit, agent_work_queue,
    agent_work_resource_scope,
};
use pioneer_protocol::{AgentIdentitySourceKind, TaskActorContract, TaskOccurrenceContract};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, Func};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, PaginatorTrait, QueryFilter,
    QuerySelect,
};
use std::future::Future;

use crate::agent_action_commit::AgentResourceCommitInput;
use crate::agent_delegation_route::{AgentDelegationRouteInput, insert_agent_delegation_route};
use crate::agent_execution::{
    AgentExecutionInput, AgentResourceStateInput, acquire_agent_running_permit,
    insert_agent_execution, insert_agent_resource_state,
};
use crate::agent_grant::{
    AgentExecutionGrantInput, insert_agent_execution_grant, load_agent_execution_grant,
    parse_agent_execution_launch_grant,
};
use crate::agent_graph_projection::agent_work_resource_limits_are_bounded;
use crate::agent_graph_resources::{ensure_root_resource_scope, validate_agent_graph_bounds};
use crate::agent_identity::{
    AgentIdentityInput, AgentTurnResponseInput, PresentationSnapshotInput,
    SOURCE_CLI_RUNTIME_INSTANCE, SOURCE_NATIVE_AGENT, agent_identity_source_kind,
    ensure_agent_identity, insert_agent_turn_response, insert_presentation_snapshot,
    load_agent_identity_by_source, load_current_agent_presentation_snapshot,
    load_native_agent_config,
};
use crate::agent_queue::{
    AgentQueueEntryInput, enqueue_agent_execution, ensure_agent_branch_schedule,
    load_fair_queued_candidate, mark_agent_branch_scheduled,
};
use crate::canonical_id::canonical_agent_id;

/// Allows a caller to maintain another domain's projection inside the same
/// transaction at the exact execution transition.
pub trait ExecutionChangeSink: Sync {
    fn agent_changed<'a, C: ConnectionTrait>(
        &'a self,
        db: &'a C,
        before: Option<&'a agent_execution::Model>,
        after: Option<&'a agent_execution::Model>,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

async fn insert_and_record(
    db: &DatabaseTransaction,
    input: &AgentExecutionInput,
    changes: &impl ExecutionChangeSink,
) -> Result<agent_execution::Model> {
    let (execution, inserted) = insert_agent_execution(db, input).await?;
    if inserted {
        changes.agent_changed(db, None, Some(&execution)).await?;
    }
    Ok(execution)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionGraphCommitInput {
    /// Current source fence checked atomically at commit. On replay this may
    /// be newer than the immutable execution/presentation source below.
    pub identity: AgentIdentityInput,
    pub presentation: PresentationSnapshotInput,
    pub root_execution_id: String,
    /// Present only when this occurrence is a new root. Descendants reuse the
    /// already-persisted root instead of fabricating a root row from child
    /// identity/profile facts.
    pub root_execution: Option<AgentExecutionInput>,
    pub child_execution: AgentExecutionInput,
    pub root_resource_state: Option<AgentResourceStateInput>,
    pub child_resource_state: AgentResourceStateInput,
    pub grant: AgentExecutionGrantInput,
    /// Present when the graph is committed together with an already durable
    /// Turn. Task candidate/reviewer graphs are admitted before their hidden
    /// Turn exists; their response binding is committed atomically by the
    /// Turn projector instead of creating a dangling pre-Turn reference.
    pub response: Option<AgentTurnResponseInput>,
    /// Root-admission routes are committed only with a newly created root
    /// execution. Descendant graph writers cannot smuggle authority expansion.
    pub root_routes: Vec<AgentDelegationRouteInput>,
    pub max_concurrency: i32,
    pub max_queue_depth: i32,
    pub max_depth: i32,
    pub max_fan_out: i32,
    pub max_total_nodes: i32,
    pub idle_timeout_secs: i64,
    pub hard_timeout_secs: i64,
    pub child_permit_id: String,
    pub child_queue_id: String,
    pub task_actor_contract: Option<TaskActorContract>,
    pub task_occurrence_contract: Option<TaskOccurrenceContract>,
    pub contract_now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionGraphCommitResult {
    pub root_execution_id: String,
    pub execution_id: String,
    pub queued: bool,
    pub queue_position: Option<u64>,
}

/// Observe the exact resource and queue rows used by graph admission.
pub async fn graph_scheduling_facts(
    db: &DatabaseTransaction,
    input: &AgentExecutionGraphCommitInput,
) -> Result<crate::agent_graph_resources::GraphSchedulingFacts> {
    crate::agent_graph_resources::graph_scheduling_facts(
        db,
        &input.child_resource_state.id,
        &input.root_execution_id,
        &input.child_execution.id,
        input.child_resource_state.attempt_generation,
    )
    .await
}

/// Atomically materialize the agent domain execution graph used by a durable
/// agent Task occurrence.  The graph rows are immutable/idempotent; only the
/// child resource state and its permit/queue placement are changed when this
/// is the first materialization.  A retry therefore cannot create a second
/// identity, execution, grant or permit.
pub async fn commit_agent_execution_graph(
    db: &DatabaseTransaction,
    input: &AgentExecutionGraphCommitInput,
    changes: &impl ExecutionChangeSink,
) -> Result<AgentExecutionGraphCommitResult> {
    if !agent_work_resource_limits_are_bounded(
        i64::from(input.max_concurrency),
        i64::from(input.max_queue_depth),
        i64::from(input.max_depth),
        i64::from(input.max_fan_out),
        i64::from(input.max_total_nodes),
    ) || input.idle_timeout_secs < 1
        || input.hard_timeout_secs < input.idle_timeout_secs
    {
        bail!("agent execution graph resource ceilings must be positive");
    }
    let (grant_identity, grant_profile, child_launch_grant) =
        parse_agent_execution_launch_grant(input.grant.grant_json.as_str())?;
    child_launch_grant.validate_for_execution().map_err(|_| {
        anyhow::anyhow!("historical child launch ceiling cannot authorize a new execution")
    })?;
    // Admission and queue promotion acquire the graph root before inspecting
    // any child or consuming capacity. Existing roots therefore serialize
    // independent writers without holding a global writer connection.
    let locked_root = agent_execution::Entity::find_by_id(&input.root_execution_id)
        .lock_exclusive()
        .one(db)
        .await
        .context("failed to lock work-graph root")?;
    let existing = agent_execution::Entity::find_by_id(&input.child_execution.id)
        .lock_exclusive()
        .one(db)
        .await?;
    let identity = ensure_agent_identity(db, &input.identity).await?;
    if identity.workspace_id != input.child_execution.workspace_id
        || agent_identity_source_kind(&identity.source_kind)? != grant_identity.source_kind
        || match (&grant_identity.source_kind, &grant_profile.backend) {
            (
                AgentIdentitySourceKind::NativeAgent,
                pioneer_protocol::AgentExecutionProfileBackend::ApiProvider,
            ) => false,
            (
                AgentIdentitySourceKind::CliRuntimeInstance,
                pioneer_protocol::AgentExecutionProfileBackend::CliRuntime {
                    runtime_instance_id,
                }
                | pioneer_protocol::AgentExecutionProfileBackend::AcpAgentRuntime {
                    runtime_id: runtime_instance_id,
                },
            ) => {
                runtime_instance_id != &identity.source_id
                    || grant_profile.provider_id != format!("cli_runtime:{runtime_instance_id}")
            }
            (AgentIdentitySourceKind::Ephemeral, _) => false,
            _ => true,
        }
    {
        bail!("execution graph changed its workspace or launch source");
    }
    if identity.source_kind == SOURCE_NATIVE_AGENT {
        let config = load_native_agent_config(db, &identity.source_id)
            .await?
            .context("native Agent source disappeared before graph commit")?;
        if config.workspace_id != identity.workspace_id
            || !config.enabled
            || config.config_revision != identity.source_revision
        {
            bail!("native Agent source changed before graph commit");
        }
    }
    // An ephemeral actor has no CLI kind of its own. Its profile must retain
    // the registered runtime source from the admitted ceiling, rather than
    // treating an execution-local identity as authority to change engines.
    if grant_identity.source_kind == AgentIdentitySourceKind::Ephemeral
        && let pioneer_protocol::AgentExecutionProfileBackend::CliRuntime {
            runtime_instance_id,
        }
        | pioneer_protocol::AgentExecutionProfileBackend::AcpAgentRuntime {
            runtime_id: runtime_instance_id,
        } = &grant_profile.backend
    {
        let source = load_agent_identity_by_source(
            db,
            &identity.workspace_id,
            SOURCE_CLI_RUNTIME_INSTANCE,
            runtime_instance_id,
        )
        .await?
        .context("ephemeral CLI profile source disappeared before commit")?;
        let backing = child_launch_grant
            .identities
            .iter()
            .find(|candidate| {
                candidate.id.as_str() == source.id
                    && candidate.source_kind == AgentIdentitySourceKind::CliRuntimeInstance
                    && grant_profile
                        .compatible_agent_identity_ids
                        .contains(&candidate.id)
            })
            .context("ephemeral CLI profile has no admitted registered source")?;
        if source.status != "active"
            || source.retired_at.is_some()
            || grant_profile.provider_id != format!("cli_runtime:{runtime_instance_id}")
            || (existing.is_none()
                && (u64::try_from(source.source_revision).ok() != Some(backing.source_revision)
                    || source.source_fingerprint != backing.source_fingerprint))
        {
            bail!("ephemeral CLI profile source changed before commit");
        }
        let historical = load_current_agent_presentation_snapshot(
            db,
            &source.id,
            i64::try_from(backing.source_revision).unwrap_or(-1),
            &backing.source_fingerprint,
        )
        .await?
        .context("ephemeral CLI profile source snapshot is missing")?;
        let current = load_current_agent_presentation_snapshot(
            db,
            &source.id,
            source.source_revision,
            &source.source_fingerprint,
        )
        .await?
        .context("ephemeral CLI profile current source snapshot is missing")?;
        if historical.display_name != backing.display_name
            || historical.nickname != backing.nickname
            || historical.avatar_revision != backing.avatar_revision
            || historical.role_label != backing.role_label
            || current.role_label != backing.role_label
        {
            bail!("ephemeral CLI profile changed its admitted runtime kind");
        }
    }
    let presentation_matches_source = if let Some(existing) = existing.as_ref() {
        existing.agent_identity_id == identity.id
            && existing.presentation_snapshot_id.as_deref() == Some(input.presentation.id.as_str())
            && existing.identity_source_revision == input.presentation.source_revision
            && existing.identity_source_fingerprint == input.presentation.source_fingerprint
    } else {
        identity.source_revision == input.presentation.source_revision
            && identity.source_fingerprint == input.presentation.source_fingerprint
    };
    if identity.source_kind == SOURCE_CLI_RUNTIME_INSTANCE && existing.is_some() {
        // Replay keeps its historical presentation, but the registered source
        // must still implement the admitted CLI engine. Technical revisions
        // and display-name changes do not change that engine.
        let current = load_current_agent_presentation_snapshot(
            db,
            &identity.id,
            identity.source_revision,
            &identity.source_fingerprint,
        )
        .await?
        .context("CLI execution current source snapshot is missing")?;
        if current.role_label.is_none() || current.role_label != input.presentation.role_label {
            bail!("CLI execution source changed its admitted runtime kind");
        }
    }
    if identity.id != input.presentation.agent_identity_id
        || !presentation_matches_source
        || grant_identity.id.as_str() != input.presentation.agent_identity_id
        || i64::try_from(grant_identity.source_revision).ok()
            != Some(input.presentation.source_revision)
        || grant_identity.source_fingerprint != input.presentation.source_fingerprint
        || grant_identity.display_name != input.presentation.display_name
        || grant_identity.nickname != input.presentation.nickname
        || grant_identity.avatar_revision != input.presentation.avatar_revision
        || grant_identity.role_label != input.presentation.role_label
    {
        bail!("agent presentation snapshot is not pinned to the exact identity source");
    }
    let (nickname_owner_kind, nickname_owner_id) = if input.identity.source_kind
        == SOURCE_NATIVE_AGENT
        && input.presentation.nickname.eq_ignore_ascii_case("pioneer")
    {
        (NICKNAME_OWNER_RESERVED, "pioneer".to_owned())
    } else {
        (NICKNAME_OWNER_AGENT, identity.id.clone())
    };
    if existing.is_none() {
        claim_actor_nickname(
            db,
            input.identity.workspace_id.as_str(),
            input.presentation.nickname.as_str(),
            nickname_owner_kind,
            nickname_owner_id.as_str(),
            input.presentation.now.clone(),
        )
        .await?;
    }
    let _presentation = insert_presentation_snapshot(db, &input.presentation).await?;

    if input.root_execution_id != input.child_execution.work_graph_root_execution_id {
        bail!("child execution is bound to a different work-graph root");
    }
    let root = if let Some(root_execution) = input.root_execution.as_ref() {
        if root_execution.id != input.root_execution_id
            || root_execution.work_graph_root_execution_id != input.root_execution_id
            || root_execution.parent_execution_id.is_some()
        {
            bail!("new root execution has invalid root lineage");
        }
        insert_and_record(db, root_execution, changes).await?
    } else {
        locked_root.context("inherited work-graph root does not exist")?
    };
    if root.workspace_id != input.child_execution.workspace_id
        || root.work_graph_root_execution_id != input.root_execution_id
    {
        bail!("inherited work-graph root is outside the task workspace");
    }
    let child = if input.child_execution.id == root.id {
        if input.root_execution.is_none() {
            bail!("an inherited root cannot also be a new child execution");
        }
        root.clone()
    } else {
        if input.child_execution.parent_execution_id.is_none() {
            bail!("descendant execution requires its exact parent execution");
        }
        insert_and_record(db, &input.child_execution, changes).await?
    };
    if input.grant.execution_id != child.id
        || input.grant.parent_execution_id != child.parent_execution_id
        || input.grant.child_identity_id != child.agent_identity_id
        || grant_identity.id.as_str() != child.agent_identity_id
        || i64::try_from(grant_identity.source_revision).ok()
            != Some(child.identity_source_revision)
        || grant_identity.source_fingerprint != child.identity_source_fingerprint
        || child.resolved_profile_id.as_deref() != Some(grant_profile.id.as_str())
        || child.resolved_profile_fingerprint.as_deref() != Some(grant_profile.fingerprint.as_str())
    {
        bail!("agent execution graph grant differs from its exact child execution");
    }
    // Independent roots retain the source's launch ceiling without inheriting
    // its lifecycle. The canonical StartAgent action proves that source at commit.
    #[derive(serde::Deserialize)]
    struct LaunchSource {
        #[serde(default)]
        source_execution_id: Option<String>,
    }
    let source_grant: LaunchSource = serde_json::from_str(&input.grant.grant_json)?;
    let launch_source = child
        .parent_execution_id
        .as_deref()
        .or(source_grant.source_execution_id.as_deref());
    if let Some(parent_execution_id) = launch_source {
        let parent = agent_execution::Entity::find_by_id(parent_execution_id.to_owned())
            .lock_exclusive()
            .one(db)
            .await
            .context("failed to load exact parent execution")?
            .context("exact parent execution does not exist")?;
        if parent.workspace_id != child.workspace_id
            || (child.parent_execution_id.is_some()
                && parent.work_graph_root_execution_id != input.root_execution_id)
        {
            bail!("exact parent execution is outside the child work graph");
        }
        let parent_grant = load_agent_execution_grant(db, parent_execution_id)
            .await?
            .context("exact parent execution has no immutable launch grant")?;
        let (_, _, parent_launch_grant) =
            parse_agent_execution_launch_grant(parent_grant.grant_json.as_str())?;
        parent_launch_grant.validate_for_execution().map_err(|_| {
            anyhow::anyhow!("historical parent launch ceiling cannot authorize a new execution")
        })?;
        let ephemeral_child =
            grant_identity.source_kind == pioneer_protocol::AgentIdentitySourceKind::Ephemeral;
        let identity_ceiling_is_monotonic = child_launch_grant.identities.iter().all(|candidate| {
            parent_launch_grant.identities.iter().any(|parent| {
                pioneer_protocol::agent_identity_has_same_launch_source(candidate, parent)
            }) || (ephemeral_child && candidate == &grant_identity)
        });
        let capability_ceiling_is_monotonic = child_launch_grant
            .skill_ids
            .iter()
            .all(|id| parent_launch_grant.skill_ids.contains(id))
            && child_launch_grant
                .mcp_server_ids
                .iter()
                .all(|id| parent_launch_grant.mcp_server_ids.contains(id));
        let child_permission = pioneer_protocol::task_permission_cap_snapshot(
            &child_launch_grant.max_permission_profile,
        );
        let parent_permission = pioneer_protocol::task_permission_cap_snapshot(
            &parent_launch_grant.max_permission_profile,
        );
        let permission_ceiling_is_monotonic = pioneer_protocol::intersect_turn_permission_profiles(
            &child_permission,
            &parent_permission,
            pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
        ) == child_permission;
        if !identity_ceiling_is_monotonic
            || !capability_ceiling_is_monotonic
            || !permission_ceiling_is_monotonic
            || (ephemeral_child && !parent_launch_grant.allow_server_derived_ephemeral)
            || (child_launch_grant.allow_inherit_parent_identity
                && !parent_launch_grant.allow_inherit_parent_identity)
            || (child_launch_grant.allow_server_derived_ephemeral
                && !parent_launch_grant.allow_server_derived_ephemeral)
            || (child_launch_grant.allow_inherit_parent_profile
                && !parent_launch_grant.allow_inherit_parent_profile)
        {
            bail!("child execution launch ceiling widens its exact parent grant");
        }
    }
    if !input.root_routes.is_empty() && (input.root_execution.is_none() || child.id != root.id) {
        bail!("only a new root execution may commit root admission routes");
    }
    for route in &input.root_routes {
        if route.source_execution_id != root.id
            || route.source_capsule_id.as_deref() != Some(root.home_root_thread_id.as_str())
            || route.home_capsule_id.as_deref() != Some(root.home_root_thread_id.as_str())
            || route.source_workspace_id.as_deref() != Some(root.workspace_id.as_str())
            || route.source_identity_id.as_deref() != Some(root.agent_identity_id.as_str())
            || route.route_kind != "execution_bound"
        {
            bail!("root admission route differs from its exact root execution");
        }
        insert_agent_delegation_route(db, route).await?;
    }
    let terminal_statuses = [
        "completed",
        "succeeded",
        "failed",
        "blocked",
        "cancelled",
        "timed_out",
    ];
    // A completed root execution is not aggregate graph liveness. Detached
    // descendants may keep using the still-active root scope; only the exact
    // child being admitted must be non-terminal. A closed scope is rejected
    // by `ensure_root_resource_scope` below.
    if terminal_statuses.contains(&child.status.as_str()) {
        bail!("cannot materialize a terminal agent execution graph");
    }
    if let Some(response) = input.response.as_ref()
        && (response.turn_id.trim().is_empty()
            || response.execution_id != child.id
            || response.presentation_snapshot_id
                != child
                    .presentation_snapshot_id
                    .as_deref()
                    .context("responding AgentExecution has no presentation snapshot")?)
    {
        bail!("Agent Turn response differs from its exact execution graph");
    }
    insert_agent_execution_grant(db, &input.grant).await?;
    if let Some(response) = input.response.as_ref() {
        insert_agent_turn_response(db, response).await?;
    }
    ensure_root_resource_scope(
        db,
        input.root_execution_id.as_str(),
        input.max_concurrency,
        input.max_queue_depth,
        input.max_depth,
        input.max_fan_out,
        input.max_total_nodes,
        input.child_execution.now.clone(),
    )
    .await?;
    if let Some(root_resource_state) = input.root_resource_state.as_ref() {
        if root_resource_state.execution_id != input.root_execution_id {
            bail!("root resource state belongs to a different execution");
        }
        insert_agent_resource_state(db, root_resource_state).await?;
    }
    let child_state = insert_agent_resource_state(db, &input.child_resource_state).await?;

    let root_execution_id = input.root_execution_id.as_str();
    let execution_id = input.child_execution.id.as_str();
    let now = input.child_execution.now;
    ensure_agent_branch_schedule(
        db,
        root_execution_id,
        input.child_resource_state.branch_key.as_str(),
        now.clone(),
    )
    .await?;
    let scope = agent_work_resource_scope::Entity::find_by_id(root_execution_id.to_owned())
        .lock_exclusive()
        .one(db)
        .await
        .context("failed to reload task execution root resource scope")?
        .context("task execution root resource scope disappeared")?;
    validate_agent_graph_bounds(db, &child, &scope).await?;
    let held_count = agent_running_permit::Entity::find()
        .filter(agent_running_permit::Column::RootExecutionId.eq(root_execution_id.to_owned()))
        .filter(agent_running_permit::Column::Status.eq("held"))
        .count(db)
        .await
        .context("failed to count task execution permits")?;
    let capacity = u64::try_from(scope.max_concurrency).unwrap_or_default();
    let existing_queue = agent_work_queue::Entity::find()
        .filter(agent_work_queue::Column::RootExecutionId.eq(root_execution_id.to_owned()))
        .filter(agent_work_queue::Column::ExecutionId.eq(execution_id.to_owned()))
        .filter(
            agent_work_queue::Column::AttemptGeneration
                .eq(input.child_resource_state.attempt_generation),
        )
        .one(db)
        .await
        .context("failed to inspect task execution queue entry")?;
    let fair_queued = load_fair_queued_candidate(db, root_execution_id, now.clone()).await?;
    let queued_count = agent_work_queue::Entity::find()
        .filter(agent_work_queue::Column::RootExecutionId.eq(root_execution_id.to_owned()))
        .filter(agent_work_queue::Column::State.eq("queued"))
        .count(db)
        .await
        .context("failed to count task execution queue entries")?;
    let existing_permit = agent_running_permit::Entity::find()
        .filter(agent_running_permit::Column::RootExecutionId.eq(root_execution_id.to_owned()))
        .filter(agent_running_permit::Column::ExecutionId.eq(execution_id.to_owned()))
        .filter(
            agent_running_permit::Column::AttemptGeneration
                .eq(input.child_resource_state.attempt_generation),
        )
        .filter(agent_running_permit::Column::Status.eq("held"))
        .one(db)
        .await
        .context("failed to inspect task execution permit")?;
    if let Some(permit) = existing_permit {
        bind_running_state(
            db,
            child_state.id.as_str(),
            execution_id,
            permit.id.as_str(),
            now.clone(),
            Some(input.idle_timeout_secs),
            Some(input.hard_timeout_secs),
            changes,
        )
        .await?;
        return Ok(AgentExecutionGraphCommitResult {
            root_execution_id: root_execution_id.to_owned(),
            execution_id: execution_id.to_owned(),
            queued: false,
            queue_position: None,
        });
    }
    if let Some(queue) = existing_queue
        .as_ref()
        .filter(|queue| queue.state == "queued")
    {
        // A newly free permit is assigned only to the oldest queued attempt.
        // This prevents a later sibling (or a retry that happens to wake first)
        // from repeatedly leapfrogging a long-running queued branch.
        if held_count >= capacity
            || fair_queued
                .as_ref()
                .is_some_and(|first| first.id != queue.id)
        {
            return Ok(AgentExecutionGraphCommitResult {
                root_execution_id: root_execution_id.to_owned(),
                execution_id: execution_id.to_owned(),
                queued: true,
                queue_position: Some(u64::try_from(queue.enqueue_sequence).unwrap_or(u64::MAX)),
            });
        }
        let permit = acquire_agent_running_permit(
            db,
            input.child_permit_id.as_str(),
            root_execution_id,
            execution_id,
            input.child_resource_state.attempt_generation,
            now.clone(),
        )
        .await?;
        bind_running_state(
            db,
            child_state.id.as_str(),
            execution_id,
            permit.id.as_str(),
            now.clone(),
            Some(input.idle_timeout_secs),
            Some(input.hard_timeout_secs),
            changes,
        )
        .await?;
        let claim_result = agent_work_queue::Entity::update_many()
            .col_expr(
                agent_work_queue::Column::State,
                sea_orm::sea_query::Expr::value("claimed"),
            )
            .col_expr(
                agent_work_queue::Column::ClaimToken,
                sea_orm::sea_query::Expr::value(Some(permit.id.clone())),
            )
            .col_expr(
                agent_work_queue::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now.clone()),
            )
            .filter(agent_work_queue::Column::Id.eq(queue.id.clone()))
            .filter(agent_work_queue::Column::State.eq("queued"))
            .exec(db)
            .await
            .context("failed to claim queued task execution")?;
        if claim_result.rows_affected != 1 {
            bail!("queued task execution was claimed concurrently");
        }
        mark_agent_branch_scheduled(
            db,
            root_execution_id,
            queue.branch_key.as_str(),
            now.clone(),
        )
        .await?;
        return Ok(AgentExecutionGraphCommitResult {
            root_execution_id: root_execution_id.to_owned(),
            execution_id: execution_id.to_owned(),
            queued: false,
            queue_position: None,
        });
    }
    if existing_queue.is_some() {
        bail!("task execution queue entry is not in a retryable state");
    }
    if fair_queued.is_some() {
        // Preserve FIFO fairness even when this newly materialized child sees
        // spare capacity while another branch is already waiting.
        if queued_count >= u64::try_from(scope.max_queue_depth).unwrap_or(u64::MAX) {
            bail!("agent execution graph queue boundary exceeded");
        }
        let enqueue_sequence = scope
            .queue_generation
            .checked_add(1)
            .context("task execution queue generation exhausted")?;
        let generation_result = agent_work_resource_scope::Entity::update_many()
            .col_expr(
                agent_work_resource_scope::Column::QueueGeneration,
                sea_orm::sea_query::Expr::value(enqueue_sequence),
            )
            .col_expr(
                agent_work_resource_scope::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now.clone()),
            )
            .filter(
                agent_work_resource_scope::Column::RootExecutionId.eq(root_execution_id.to_owned()),
            )
            .filter(agent_work_resource_scope::Column::QueueGeneration.eq(scope.queue_generation))
            .exec(db)
            .await
            .context("failed to advance fair task execution queue generation")?;
        if generation_result.rows_affected != 1 {
            bail!("task execution queue generation changed concurrently");
        }
        enqueue_agent_execution(
            db,
            &AgentQueueEntryInput {
                id: input.child_queue_id.clone(),
                root_execution_id: root_execution_id.to_owned(),
                execution_id: execution_id.to_owned(),
                attempt_generation: input.child_resource_state.attempt_generation,
                branch_key: input.child_resource_state.branch_key.clone(),
                enqueue_sequence,
                eligible_at: None,
                now: now.clone(),
            },
        )
        .await?;
        let occurrence_old_agent = agent_execution::Entity::find_by_id(execution_id)
            .lock_exclusive()
            .one(db)
            .await?;
        let execution_result = agent_execution::Entity::update_many()
            .col_expr(
                agent_execution::Column::Status,
                sea_orm::sea_query::Expr::value("queued"),
            )
            .col_expr(
                agent_execution::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now),
            )
            .filter(agent_execution::Column::Id.eq(execution_id.to_owned()))
            .filter(agent_execution::Column::Status.is_in(["created", "recovering"]))
            .exec(db)
            .await
            .context("failed to mark fair queued task agent execution")?;
        let occurrence_new_agent = agent_execution::Entity::find_by_id(execution_id)
            .lock_exclusive()
            .one(db)
            .await?;
        changes
            .agent_changed(
                db,
                occurrence_old_agent.as_ref(),
                occurrence_new_agent.as_ref(),
            )
            .await?;
        if execution_result.rows_affected != 1 {
            bail!("fair queued task agent execution changed concurrently");
        }
        return Ok(AgentExecutionGraphCommitResult {
            root_execution_id: root_execution_id.to_owned(),
            execution_id: execution_id.to_owned(),
            queued: true,
            queue_position: Some(u64::try_from(enqueue_sequence).unwrap_or(u64::MAX)),
        });
    }
    if held_count < capacity {
        let permit = acquire_agent_running_permit(
            db,
            input.child_permit_id.as_str(),
            root_execution_id,
            execution_id,
            input.child_resource_state.attempt_generation,
            now,
        )
        .await?;
        bind_running_state(
            db,
            child_state.id.as_str(),
            execution_id,
            permit.id.as_str(),
            now,
            Some(input.idle_timeout_secs),
            Some(input.hard_timeout_secs),
            changes,
        )
        .await?;
        mark_agent_branch_scheduled(
            db,
            root_execution_id,
            input.child_resource_state.branch_key.as_str(),
            now,
        )
        .await?;
        return Ok(AgentExecutionGraphCommitResult {
            root_execution_id: root_execution_id.to_owned(),
            execution_id: execution_id.to_owned(),
            queued: false,
            queue_position: None,
        });
    }

    if queued_count >= u64::try_from(scope.max_queue_depth).unwrap_or(u64::MAX) {
        bail!("agent execution graph queue boundary exceeded");
    }
    if scope.queue_generation == i64::MAX {
        bail!("task execution queue generation exhausted");
    }
    let enqueue_sequence = scope
        .queue_generation
        .checked_add(1)
        .context("task execution queue generation exhausted")?;
    let generation_result = agent_work_resource_scope::Entity::update_many()
        .col_expr(
            agent_work_resource_scope::Column::QueueGeneration,
            sea_orm::sea_query::Expr::value(enqueue_sequence),
        )
        .col_expr(
            agent_work_resource_scope::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(agent_work_resource_scope::Column::RootExecutionId.eq(root_execution_id.to_owned()))
        .filter(agent_work_resource_scope::Column::QueueGeneration.eq(scope.queue_generation))
        .exec(db)
        .await
        .context("failed to advance task execution queue generation")?;
    if generation_result.rows_affected != 1 {
        bail!("task execution queue generation changed concurrently");
    }
    enqueue_agent_execution(
        db,
        &AgentQueueEntryInput {
            id: input.child_queue_id.clone(),
            root_execution_id: root_execution_id.to_owned(),
            execution_id: execution_id.to_owned(),
            attempt_generation: input.child_resource_state.attempt_generation,
            branch_key: input.child_resource_state.branch_key.clone(),
            enqueue_sequence,
            eligible_at: None,
            now: now.clone(),
        },
    )
    .await?;
    let occurrence_old_agent = agent_execution::Entity::find_by_id(execution_id)
        .lock_exclusive()
        .one(db)
        .await?;
    let execution_result = agent_execution::Entity::update_many()
        .col_expr(
            agent_execution::Column::Status,
            sea_orm::sea_query::Expr::value("queued"),
        )
        .col_expr(
            agent_execution::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(agent_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(agent_execution::Column::Status.is_in(["created", "recovering"]))
        .exec(db)
        .await
        .context("failed to mark queued task agent execution")?;
    let occurrence_new_agent = agent_execution::Entity::find_by_id(execution_id)
        .lock_exclusive()
        .one(db)
        .await?;
    changes
        .agent_changed(
            db,
            occurrence_old_agent.as_ref(),
            occurrence_new_agent.as_ref(),
        )
        .await?;
    if execution_result.rows_affected != 1 {
        bail!("queued task agent execution changed concurrently");
    }
    Ok(AgentExecutionGraphCommitResult {
        root_execution_id: root_execution_id.to_owned(),
        execution_id: execution_id.to_owned(),
        queued: true,
        queue_position: Some(u64::try_from(enqueue_sequence).unwrap_or(u64::MAX)),
    })
}

pub async fn bind_running_state<C: ConnectionTrait>(
    db: &C,
    state_id: &str,
    execution_id: &str,
    permit_id: &str,
    now: DateTimeWithTimeZone,
    idle_timeout_secs: Option<i64>,
    hard_timeout_secs: Option<i64>,
    changes: &impl ExecutionChangeSink,
) -> Result<()> {
    if idle_timeout_secs.is_some_and(|seconds| seconds < 1)
        || hard_timeout_secs.is_some_and(|seconds| seconds < 1)
        || matches!((idle_timeout_secs, hard_timeout_secs), (Some(idle), Some(hard)) if hard < idle)
    {
        bail!("agent execution liveness deadlines are invalid");
    }
    let mut state_update = agent_execution_resource_state::Entity::update_many()
        .col_expr(
            agent_execution_resource_state::Column::Status,
            sea_orm::sea_query::Expr::value("running"),
        )
        .col_expr(
            agent_execution_resource_state::Column::PermitId,
            sea_orm::sea_query::Expr::value(Some(permit_id.to_owned())),
        )
        .col_expr(
            agent_execution_resource_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now.clone()),
        )
        .filter(agent_execution_resource_state::Column::Id.eq(state_id.to_owned()))
        .filter(agent_execution_resource_state::Column::ExecutionId.eq(execution_id.to_owned()))
        .filter(
            agent_execution_resource_state::Column::Status.is_in(["queued", "running", "paused"]),
        );
    if let Some(seconds) = idle_timeout_secs {
        state_update = state_update.col_expr(
            agent_execution_resource_state::Column::IdleDeadline,
            Func::coalesce([
                Expr::col(agent_execution_resource_state::Column::IdleDeadline),
                Expr::val(now + Duration::seconds(seconds)),
            ])
            .into(),
        );
    }
    if let Some(seconds) = hard_timeout_secs {
        state_update = state_update.col_expr(
            agent_execution_resource_state::Column::HardDeadline,
            Func::coalesce([
                Expr::col(agent_execution_resource_state::Column::HardDeadline),
                Expr::val(now + Duration::seconds(seconds)),
            ])
            .into(),
        );
    }
    let state_result = state_update
        .exec(db)
        .await
        .context("failed to bind task execution resource state to permit")?;
    if state_result.rows_affected != 1 {
        bail!(
            "agent execution resource state `{state_id}` was not found for execution `{execution_id}`"
        );
    }
    let occurrence_old_agent = agent_execution::Entity::find_by_id(execution_id)
        .lock_exclusive()
        .one(db)
        .await?;
    let execution_result = agent_execution::Entity::update_many()
        .col_expr(
            agent_execution::Column::Status,
            sea_orm::sea_query::Expr::value("running"),
        )
        .col_expr(
            agent_execution::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(agent_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(agent_execution::Column::Status.is_in([
            "created",
            "queued",
            "running",
            "recovering",
        ]))
        .exec(db)
        .await
        .context("failed to mark task agent execution running")?;
    let occurrence_new_agent = agent_execution::Entity::find_by_id(execution_id)
        .lock_exclusive()
        .one(db)
        .await?;
    changes
        .agent_changed(
            db,
            occurrence_old_agent.as_ref(),
            occurrence_new_agent.as_ref(),
        )
        .await?;
    if execution_result.rows_affected != 1 {
        bail!(
            "agent execution `{execution_id}` was not in a bindable state while acquiring permit `{permit_id}`"
        );
    }
    Ok(())
}

const AGENT_BACKGROUND_BATCH_LIMIT: u64 = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotedAgentExecution {
    pub execution_id: String,
    pub root_execution_id: String,
    pub queue_id: String,
}

/// Promote oldest eligible queued executions using durable root capacity.
/// At most one branch per root is promoted in a pass so a hot graph cannot
/// monopolize the global recovery worker. The queue claim, permit, resource
/// state and execution recovery status change in the caller transaction.
pub async fn promote_queued_agent_executions(
    db: &DatabaseTransaction,
    now: DateTimeWithTimeZone,
    limit: u64,
    idle_timeout_secs: i64,
    hard_timeout_secs: i64,
    changes: &impl ExecutionChangeSink,
) -> Result<Vec<PromotedAgentExecution>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if limit > AGENT_BACKGROUND_BATCH_LIMIT {
        bail!("Agent queue promotion batch exceeds its bounded limit");
    }
    if idle_timeout_secs < 1 || hard_timeout_secs < idle_timeout_secs {
        bail!("queued execution promotion requires valid liveness windows");
    }
    let candidate_ids = crate::agent_queue::candidates(db, now, limit).await?;
    if candidate_ids.is_empty() {
        return Ok(Vec::new());
    }
    let candidate_order = candidate_ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut candidates = agent_work_queue::Entity::find()
        .filter(agent_work_queue::Column::Id.is_in(candidate_ids.clone()))
        .all(db)
        .await
        .context("failed to load fair queued agent domain candidates")?;
    candidates.sort_by_key(|candidate| {
        candidate_order
            .get(candidate.id.as_str())
            .copied()
            .unwrap_or(usize::MAX)
    });
    let mut promoted = Vec::new();
    for queue in candidates {
        if promoted.len() >= usize::try_from(limit).unwrap_or(usize::MAX) {
            continue;
        }
        let root = agent_execution::Entity::find_by_id(&queue.root_execution_id)
            .select_only()
            .column(agent_execution::Column::Id)
            .lock_exclusive()
            .into_tuple::<String>()
            .one(db)
            .await?;
        if root.is_none() {
            continue;
        }
        let Some(scope) =
            agent_work_resource_scope::Entity::find_by_id(queue.root_execution_id.clone())
                .lock_exclusive()
                .one(db)
                .await
                .context("failed to load queued execution root scope")?
        else {
            continue;
        };
        if scope.status != "active" {
            continue;
        }
        let held = agent_running_permit::Entity::find()
            .filter(
                agent_running_permit::Column::RootExecutionId.eq(queue.root_execution_id.clone()),
            )
            .filter(agent_running_permit::Column::Status.eq("held"))
            .count(db)
            .await
            .context("failed to count permits during queued execution promotion")?;
        if held >= u64::try_from(scope.max_concurrency).unwrap_or_default() {
            continue;
        }
        // The candidate page precedes the root lock. Recheck the fair head
        // against the now-current queue and avoid treating a losing claim as
        // a fatal error for other roots in this transaction.
        if !load_fair_queued_candidate(db, &queue.root_execution_id, now)
            .await?
            .is_some_and(|current| current.id == queue.id)
        {
            continue;
        }
        let Some(state) = agent_execution_resource_state::Entity::find()
            .filter(
                agent_execution_resource_state::Column::ExecutionId.eq(queue.execution_id.clone()),
            )
            .filter(
                agent_execution_resource_state::Column::AttemptGeneration
                    .eq(queue.attempt_generation),
            )
            .one(db)
            .await
            .context("failed to load queued execution resource state")?
        else {
            continue;
        };
        if state.status != "queued" || state.permit_id.is_some() {
            continue;
        }
        let permit_id = canonical_agent_id(
            'P',
            &format!(
                "queue-promotion\0{}\0{}\0{}",
                queue.root_execution_id, queue.execution_id, queue.attempt_generation
            ),
        );
        let permit = acquire_agent_running_permit(
            db,
            permit_id.as_str(),
            queue.root_execution_id.as_str(),
            queue.execution_id.as_str(),
            queue.attempt_generation,
            now.clone(),
        )
        .await?;
        bind_running_state(
            db,
            state.id.as_str(),
            queue.execution_id.as_str(),
            permit.id.as_str(),
            now.clone(),
            Some(idle_timeout_secs),
            Some(hard_timeout_secs),
            changes,
        )
        .await?;
        let claim = agent_work_queue::Entity::update_many()
            .col_expr(
                agent_work_queue::Column::State,
                sea_orm::sea_query::Expr::value("claimed"),
            )
            .col_expr(
                agent_work_queue::Column::ClaimToken,
                sea_orm::sea_query::Expr::value(Some(permit.id.clone())),
            )
            .col_expr(
                agent_work_queue::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now.clone()),
            )
            .filter(agent_work_queue::Column::Id.eq(queue.id.clone()))
            .filter(agent_work_queue::Column::State.eq("queued"))
            .exec(db)
            .await
            .context("failed to claim promoted agent domain queue row")?;
        if claim.rows_affected != 1 {
            bail!("queued agent domain execution was promoted concurrently");
        }
        mark_agent_branch_scheduled(
            db,
            queue.root_execution_id.as_str(),
            queue.branch_key.as_str(),
            now.clone(),
        )
        .await?;
        // bind_running_state already advances the exact execution from queued
        // to running in this transaction. A second queued->recovering CAS here
        // can never succeed and would roll back every scheduler promotion.
        promoted.push(PromotedAgentExecution {
            execution_id: queue.execution_id,
            root_execution_id: queue.root_execution_id,
            queue_id: queue.id,
        });
    }
    Ok(promoted)
}

pub async fn commit_agent_action_resources(
    db: &DatabaseTransaction,
    resource: &AgentResourceCommitInput,
    now: DateTimeWithTimeZone,
    changes: &impl ExecutionChangeSink,
) -> Result<()> {
    let resource_state = insert_agent_resource_state(
        db,
        &AgentResourceStateInput {
            id: resource.resource_state_id.clone(),
            execution_id: resource.execution_id.clone(),
            attempt_generation: resource.attempt_generation,
            branch_key: resource.branch_key.clone(),
            fair_order: resource.fair_order,
            now,
        },
    )
    .await?;
    if let Some(permit_id) = resource.permit_id.as_deref() {
        let permit = acquire_agent_running_permit(
            db,
            permit_id,
            &resource.root_execution_id,
            &resource.execution_id,
            resource.attempt_generation,
            now,
        )
        .await?;
        bind_running_state(
            db,
            resource_state.id.as_str(),
            resource.execution_id.as_str(),
            permit.id.as_str(),
            now,
            resource.idle_timeout_secs,
            resource.hard_timeout_secs,
            changes,
        )
        .await?;
    } else if let (Some(queue_id), Some(enqueue_sequence)) =
        (resource.queue_id.as_deref(), resource.enqueue_sequence)
    {
        enqueue_agent_execution(
            db,
            &AgentQueueEntryInput {
                id: queue_id.to_owned(),
                root_execution_id: resource.root_execution_id.clone(),
                execution_id: resource.execution_id.clone(),
                attempt_generation: resource.attempt_generation,
                branch_key: resource.branch_key.clone(),
                enqueue_sequence,
                eligible_at: None,
                now,
            },
        )
        .await?;
    } else {
        bail!("resource commit must provide a permit or queue entry");
    }
    Ok(())
}

#[cfg(test)]
#[path = "agent_graph_launch_tests.rs"]
mod launch_tests;
