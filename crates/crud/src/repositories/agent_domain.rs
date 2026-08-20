//! Persistence primitives for agent domain.
//!
//! These functions deliberately keep identity, lineage, action receipts and
//! resource state in separate records.  Callers that need an all-or-nothing
//! mutation pass a `DatabaseTransaction`; no function infers an actor from a
//! provider, model, display label or the last message.

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration, Utc};
use pioneer_entity::{
    actor_nickname_index, agent_action, agent_action_outbox, agent_action_receipt,
    agent_action_timeline_target, agent_delegation_route, agent_delegation_route_event,
    agent_domain_commit, agent_execution, agent_execution_grant, agent_execution_resource_state,
    agent_identity, agent_presentation_snapshot, agent_running_permit,
    agent_turn_response_execution, agent_work_branch_schedule, agent_work_queue,
    agent_work_resource_scope, agent_work_scheduler_state, native_agent_config, task_delivery,
    task_occurrence_contract, task_run_execution, thread, thread_lineage, turn, turn_item,
};
use pioneer_protocol::{
    AgentDelegationRouteId, AgentDelegationRouteProjection, AgentExecutionId,
    AgentExecutionProfileId, AgentExecutionProfileProjection, AgentIdentityId,
    AgentIdentityProjection, AgentIdentitySourceKind, AgentPresentationSnapshot, AgentRouteAction,
    AgentRouteDisclosurePolicy, AgentRouteKind, AgentRouteStatus, AgentWorkGraphProjection,
    AgentWorkNodeProjection, AgentWorkNodeState, PersistedActorRef, SafeRouteProvenance,
    TaskActorContract, TaskOccurrenceContract, TaskOccurrenceStatus, Thread,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{OnConflict, Query};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, EntityTrait, ExprTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, Statement, TransactionTrait, TryGetable,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const SOURCE_NATIVE_AGENT: &str = "native_agent";
pub const SOURCE_CLI_RUNTIME_INSTANCE: &str = "cli_runtime_instance";
pub const SOURCE_EPHEMERAL: &str = "ephemeral";

pub const NICKNAME_OWNER_PRINCIPAL: &str = "principal";
pub const NICKNAME_OWNER_AGENT: &str = "agent";
pub const NICKNAME_OWNER_RESERVED: &str = "reserved";
pub const NICKNAME_ACTIVE: &str = "active";
pub const NICKNAME_TOMBSTONED: &str = "tombstoned";
pub const AGENT_ACTION_OUTBOX_MAX_ATTEMPTS: i64 = 8;
const AGENT_ACTION_OUTBOX_FAILURE_CLASS: &str = "outbox_delivery_failed";
pub const AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS: i64 = 30;
pub const AGENT_ROUTE_GRAPH_MAX_EDGES: usize = 2_048;
pub const ACTIVE_AGENT_IDENTITY_CATALOG_LIMIT: u64 =
    pioneer_protocol::ChildAgentLaunchGrantSet::MAX_IDENTITIES as u64;
const AGENT_PROJECTION_BATCH_LIMIT: usize = 200;
const AGENT_BACKGROUND_BATCH_LIMIT: u64 = 512;
pub(super) const AGENT_WORK_GRAPH_MAX_CONCURRENCY: i64 = 4_096;
pub(super) const AGENT_WORK_GRAPH_MAX_QUEUE_DEPTH: i64 = 2_048;
pub(super) const AGENT_WORK_GRAPH_MAX_DEPTH: i64 = 64;
pub(super) const AGENT_WORK_GRAPH_MAX_FAN_OUT: i64 = 128;
pub(super) const AGENT_WORK_GRAPH_MAX_TOTAL_NODES: i64 = 4_096;
const AGENT_ROUTE_EXPIRY_BATCH_SIZE: u64 = 512;
const AGENT_ACTION_OUTBOX_LEASE_SECONDS: i64 = 30;
const AGENT_ACTION_OUTBOX_MAX_RETRY_SECONDS: i64 = 30 * 60;
const AGENT_ACTION_OUTBOX_PERMIT_WAIT_CLASS: &str = "waiting_for_durable_permit";
pub(super) const AGENT_ACTION_COMPACTION_FORMAT: &str = "agent_action_response_v1";
pub(super) const AGENT_ACTION_RECEIPT_COMPACTION_FORMAT: &str = "agent_action_receipt_response_v1";
pub(super) const AGENT_ACTION_OUTBOX_COMPACTION_FORMAT: &str = "agent_action_outbox_payload_v1";

const ACTION_TIMELINE_TARGET_TURN_INPUT: &str = "turn_input";
const ACTION_TIMELINE_TARGET_TURN_ITEM: &str = "turn_item";

fn agent_work_resource_limits_are_bounded(
    max_concurrency: i64,
    max_queue_depth: i64,
    max_depth: i64,
    max_fan_out: i64,
    max_total_nodes: i64,
) -> bool {
    (1..=AGENT_WORK_GRAPH_MAX_CONCURRENCY).contains(&max_concurrency)
        && (1..=AGENT_WORK_GRAPH_MAX_QUEUE_DEPTH).contains(&max_queue_depth)
        && (1..=AGENT_WORK_GRAPH_MAX_DEPTH).contains(&max_depth)
        && (1..=AGENT_WORK_GRAPH_MAX_FAN_OUT).contains(&max_fan_out)
        && (1..=AGENT_WORK_GRAPH_MAX_TOTAL_NODES).contains(&max_total_nodes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentActionTimelineProjection {
    pub author: pioneer_protocol::TurnAuthorSnapshot,
    pub route: Option<SafeRouteProvenance>,
}

fn exact_timeline_target_keys(
    targets: &[(String, Option<String>)],
    item_rows: &[(String, String, String)],
) -> Result<Vec<String>> {
    let mut item_rows_by_target = BTreeMap::new();
    for (row_id, turn_id, item_id) in item_rows {
        let key = (turn_id.clone(), item_id.clone());
        if item_rows_by_target.insert(key, row_id.clone()).is_some() {
            bail!("multiple Turn items share one exact timeline target key");
        }
    }
    Ok(targets
        .iter()
        .filter_map(|(turn_id, item_id)| match item_id {
            Some(item_id) => item_rows_by_target
                .get(&(turn_id.clone(), item_id.clone()))
                .map(|row_id| format!("turn_item:{row_id}")),
            None => Some(format!("turn_input:{turn_id}")),
        })
        .collect())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentActionLedgerCompactionSummary {
    pub candidate_rows: u64,
    pub compacted_rows: u64,
    pub payload_bytes_released: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionAuthorProjection {
    pub author: pioneer_protocol::TurnAuthorSnapshot,
    pub presentation_snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeAgentConfigInput {
    pub id: String,
    pub workspace_id: String,
    pub system_key: Option<String>,
    pub display_name: String,
    pub nickname: String,
    pub enabled: bool,
    pub avatar_revision: Option<String>,
    pub config_revision: i64,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentIdentityInput {
    pub id: String,
    pub workspace_id: String,
    pub source_kind: String,
    pub source_id: String,
    pub source_revision: i64,
    pub source_fingerprint: String,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentationSnapshotInput {
    pub id: String,
    pub agent_identity_id: String,
    pub source_revision: i64,
    pub source_fingerprint: String,
    pub display_name: String,
    pub nickname: String,
    pub avatar_revision: Option<String>,
    pub role_label: Option<String>,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionInput {
    pub id: String,
    pub workspace_id: String,
    pub agent_identity_id: String,
    pub identity_source_revision: i64,
    pub identity_source_fingerprint: String,
    pub parent_execution_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub parent_thread_id: Option<String>,
    pub home_root_thread_id: String,
    pub work_graph_root_execution_id: String,
    pub requested_identity_selection_json: String,
    pub requested_profile_selection_json: String,
    pub resolved_profile_id: Option<String>,
    pub resolved_profile_fingerprint: Option<String>,
    pub presentation_snapshot_id: Option<String>,
    pub authorization_context_fingerprint: String,
    pub execution_generation: i64,
    pub status: String,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentActionInput {
    pub id: String,
    pub execution_id: String,
    pub action_kind: String,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommitInput {
    pub mutation_kind: String,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub actor_identity_id: String,
    pub action: AgentActionInput,
    pub receipt_id: String,
    pub outbox_id: String,
    pub action_response_json: Option<String>,
    pub receipt_response_json: Option<String>,
    pub route_receipt_json: Option<String>,
    pub outbox_payload_json: String,
    pub policy_fingerprint: String,
    pub execution_grant_fingerprint: String,
    pub execution_grant_policy_generation: i64,
    pub source_scope_id: String,
    pub destination_scope_id: Option<String>,
    pub subject_role_key: String,
    pub authorized_resource_action: String,
    pub source_policy_generation: i64,
    pub destination_policy_generation: Option<i64>,
    pub route_generation: Option<i64>,
    pub disclosure_class: String,
    /// Exact immutable execution generation bound into the runtime adapter.
    /// A recovered/replaced execution must not commit through a stale adapter.
    pub expected_execution_generation: i64,
    /// Current mutable source revision observed immediately before commit.
    /// This is separate from the execution's pinned authorship revision: a
    /// rename may advance the current source while the execution keeps its
    /// immutable snapshot, but a concurrent disable/retire/settings change
    /// must fence the prepared side effect.
    pub expected_current_identity_source_revision: i64,
    pub expected_current_identity_source_fingerprint: String,
    /// Exact resource-attempt generation that currently owns the held permit.
    /// This fences an old provider/runtime process after replacement even
    /// though the durable AgentExecution identity itself is unchanged.
    pub expected_attempt_generation: i64,
    /// Exact policy generation admitted by the execution-bound adapter. CRUD
    /// compares it with the current generation in the write transaction.
    pub expected_policy_generation: i64,
    /// Marks a cross-capsule write so CRUD revalidates the exact durable route
    /// inside the caller-owned transaction.
    pub requires_cross_capsule_route: bool,
    /// Optional root-scoped resource transition. When present, the resource
    /// state and either its permit or durable queue entry are committed with
    /// the action, receipt and outbox rather than in a second write path.
    pub resource: Option<AgentResourceCommitInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResourceCommitInput {
    pub root_execution_id: String,
    pub execution_id: String,
    pub attempt_generation: i64,
    pub branch_key: String,
    pub fair_order: i64,
    /// Stable server-owned resource-state identifier for this action attempt.
    /// It must be reused on response-loss retries instead of being generated
    /// inside the transaction.
    pub resource_state_id: String,
    pub permit_id: Option<String>,
    pub queue_id: Option<String>,
    pub enqueue_sequence: Option<i64>,
    /// Server-owned execution-local liveness windows. They are applied only
    /// when a permit starts an attempt; queued work remains without a running
    /// deadline until it is actually admitted.
    pub idle_timeout_secs: Option<i64>,
    pub hard_timeout_secs: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDelegationRouteInput {
    pub id: String,
    pub source_execution_id: String,
    pub destination_thread_id: String,
    pub source_capsule_id: Option<String>,
    pub destination_capsule_id: Option<String>,
    pub source_workspace_id: Option<String>,
    pub destination_workspace_id: Option<String>,
    pub source_gateway_id: Option<String>,
    pub destination_gateway_id: Option<String>,
    pub source_identity_id: Option<String>,
    pub destination_agent_identity_id: Option<String>,
    pub destination_profile_id: Option<String>,
    pub home_capsule_id: Option<String>,
    pub route_kind: String,
    pub authority_actor_json: String,
    pub authority_fingerprint: String,
    pub allowed_actions_json: String,
    pub disclosure_json: String,
    pub route_generation: i64,
    pub source_policy_generation: i64,
    pub destination_policy_generation: i64,
    pub hop_count: i64,
    pub max_hops: i64,
    pub return_route_id: Option<String>,
    pub grant_fingerprint: String,
    pub status: String,
    pub updated_at: DateTimeWithTimeZone,
    pub expires_at: Option<DateTimeWithTimeZone>,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone)]
pub struct AgentThreadCreationCommitInput<'a> {
    pub thread: &'a Thread,
    pub execution_id: AgentExecutionId,
    pub route: Option<AgentDelegationRouteInput>,
    pub lineage: Option<pioneer_protocol::TaskThreadLineage>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionGrantInput {
    pub id: String,
    pub execution_id: String,
    pub parent_execution_id: Option<String>,
    pub child_identity_id: String,
    pub grant_fingerprint: String,
    pub grant_json: String,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurnResponseInput {
    pub turn_id: String,
    pub execution_id: String,
    pub presentation_snapshot_id: String,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResourceStateInput {
    pub id: String,
    pub execution_id: String,
    pub attempt_generation: i64,
    pub branch_key: String,
    pub fair_order: i64,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionGraphCommitInput {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentQueueEntryInput {
    pub id: String,
    pub root_execution_id: String,
    pub execution_id: String,
    pub attempt_generation: i64,
    pub branch_key: String,
    pub enqueue_sequence: i64,
    pub eligible_at: Option<DateTimeWithTimeZone>,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotedAgentExecution {
    pub execution_id: String,
    pub root_execution_id: String,
    pub queue_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWorkGraphProjectionTarget {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentWorkGraphCancellationTarget {
    pub execution_id: String,
    pub turn_id: Option<String>,
    pub thread_id: Option<String>,
    pub parent_task_id: Option<String>,
}

pub async fn load_native_agent_config<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<native_agent_config::Model>> {
    native_agent_config::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to load native agent config")
}

pub async fn load_native_agent_config_by_system_key<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    system_key: &str,
) -> Result<Option<native_agent_config::Model>> {
    native_agent_config::Entity::find()
        .filter(native_agent_config::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(native_agent_config::Column::SystemKey.eq(system_key.to_owned()))
        .one(db)
        .await
        .context("failed to load native agent config by exact system key")
}

pub async fn ensure_native_agent_config<C: ConnectionTrait>(
    db: &C,
    input: &NativeAgentConfigInput,
) -> Result<native_agent_config::Model> {
    native_agent_config::Entity::insert(native_agent_config::ActiveModel {
        id: Set(input.id.clone()),
        workspace_id: Set(input.workspace_id.clone()),
        system_key: Set(input.system_key.clone()),
        display_name: Set(input.display_name.clone()),
        nickname: Set(input.nickname.clone()),
        enabled: Set(input.enabled),
        avatar_revision: Set(input.avatar_revision.clone()),
        config_revision: Set(input.config_revision),
        created_at: Set(input.now),
        updated_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::column(native_agent_config::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist native agent config")?;
    let config = load_native_agent_config(db, &input.id)
        .await?
        .context("native agent config disappeared after idempotent insert")?;
    if config.workspace_id != input.workspace_id
        || config.system_key != input.system_key
        || config.display_name != input.display_name
        || config.nickname != input.nickname
        || config.enabled != input.enabled
        || config.avatar_revision != input.avatar_revision
        || config.config_revision != input.config_revision
    {
        bail!("native agent config id was reused with different immutable facts");
    }
    Ok(config)
}

pub async fn load_agent_identity_by_source<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    source_kind: &str,
    source_id: &str,
) -> Result<Option<agent_identity::Model>> {
    agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_identity::Column::SourceKind.eq(source_kind.to_owned()))
        .filter(agent_identity::Column::SourceId.eq(source_id.to_owned()))
        .one(db)
        .await
        .context("failed to resolve agent identity by exact source")
}

pub async fn load_agent_identity<C: ConnectionTrait>(
    db: &C,
    identity_id: &str,
) -> Result<Option<agent_identity::Model>> {
    agent_identity::Entity::find_by_id(identity_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent identity")
}

pub async fn load_active_agent_identity<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    identity_id: &str,
) -> Result<Option<agent_identity::Model>> {
    agent_identity::Entity::find_by_id(identity_id.to_owned())
        .filter(agent_identity::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_identity::Column::Status.eq("active"))
        .filter(agent_identity::Column::RetiredAt.is_null())
        .one(db)
        .await
        .context("failed to load exact active workspace agent identity")
}

pub async fn load_active_agent_identity_by_source<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    source_kind: &str,
    source_id: &str,
) -> Result<Option<agent_identity::Model>> {
    agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_identity::Column::SourceKind.eq(source_kind.to_owned()))
        .filter(agent_identity::Column::SourceId.eq(source_id.to_owned()))
        .filter(agent_identity::Column::Status.eq("active"))
        .filter(agent_identity::Column::RetiredAt.is_null())
        .one(db)
        .await
        .context("failed to resolve exact active agent identity source")
}

pub async fn list_active_agent_identities<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Vec<agent_identity::Model>> {
    agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_identity::Column::Status.eq("active"))
        .filter(agent_identity::Column::RetiredAt.is_null())
        .order_by_asc(agent_identity::Column::Id)
        .limit(ACTIVE_AGENT_IDENTITY_CATALOG_LIMIT)
        .all(db)
        .await
        .context("failed to list active workspace agent identities")
}

pub async fn load_agent_presentation_snapshot<C: ConnectionTrait>(
    db: &C,
    snapshot_id: &str,
) -> Result<Option<agent_presentation_snapshot::Model>> {
    agent_presentation_snapshot::Entity::find_by_id(snapshot_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent presentation snapshot")
}

pub async fn load_current_agent_presentation_snapshot<C: ConnectionTrait>(
    db: &C,
    identity_id: &str,
    source_revision: i64,
    source_fingerprint: &str,
) -> Result<Option<agent_presentation_snapshot::Model>> {
    agent_presentation_snapshot::Entity::find()
        .filter(agent_presentation_snapshot::Column::AgentIdentityId.eq(identity_id.to_owned()))
        .filter(agent_presentation_snapshot::Column::SourceRevision.eq(source_revision))
        .filter(
            agent_presentation_snapshot::Column::SourceFingerprint
                .eq(source_fingerprint.to_owned()),
        )
        .order_by_desc(agent_presentation_snapshot::Column::CreatedAt)
        .order_by_desc(agent_presentation_snapshot::Column::Id)
        .one(db)
        .await
        .context("failed to load current agent presentation snapshot")
}

/// Converts the three normalized persistence rows into the one immutable
/// presentation contract consumed by conversation clients.  Mutable current
/// identity presentation is deliberately ignored; only source kind comes from
/// the identity row because it is not duplicated into the snapshot table.
pub fn agent_presentation_snapshot_from_rows(
    identity: &agent_identity::Model,
    execution: &agent_execution::Model,
    snapshot: &agent_presentation_snapshot::Model,
) -> Result<AgentPresentationSnapshot> {
    if execution.agent_identity_id != identity.id
        || snapshot.agent_identity_id != identity.id
        || execution.presentation_snapshot_id.as_deref() != Some(snapshot.id.as_str())
        || execution.identity_source_revision != snapshot.source_revision
        || execution.identity_source_fingerprint != snapshot.source_fingerprint
    {
        bail!("AgentExecution presentation rows do not form one exact immutable snapshot");
    }
    let source_kind = match identity.source_kind.as_str() {
        SOURCE_NATIVE_AGENT => AgentIdentitySourceKind::NativeAgent,
        SOURCE_CLI_RUNTIME_INSTANCE => AgentIdentitySourceKind::CliRuntimeInstance,
        SOURCE_EPHEMERAL => AgentIdentitySourceKind::Ephemeral,
        value => bail!("AgentIdentity has unsupported source kind `{value}`"),
    };
    let source_revision = u64::try_from(snapshot.source_revision)
        .context("Agent presentation source revision is negative")?;
    if source_revision == 0 {
        bail!("Agent presentation source revision must be positive");
    }
    let agent_identity_id = AgentIdentityId::new(identity.id.clone())
        .map_err(|error| anyhow!("AgentIdentity id is invalid: {error:?}"))?;
    let projection = AgentIdentityProjection::new(
        agent_identity_id.clone(),
        source_kind,
        snapshot.display_name.clone(),
        snapshot.nickname.clone(),
        snapshot.avatar_revision.clone(),
        snapshot.role_label.clone(),
        source_revision,
        snapshot.source_fingerprint.clone(),
    )
    .map_err(|error| anyhow!("Agent presentation snapshot is invalid: {error:?}"))?;
    Ok(AgentPresentationSnapshot {
        agent_identity_id,
        agent_execution_id: AgentExecutionId::new(execution.id.clone())
            .map_err(|error| anyhow!("AgentExecution id is invalid: {error:?}"))?,
        identity_source_kind: projection.source_kind,
        identity_source_revision: projection.source_revision,
        display_name: projection.display_name,
        nickname: projection.nickname,
        avatar_revision: projection.avatar_revision,
        role_label: projection.role_label,
    })
}

pub(crate) async fn revalidate_agent_presentation_for_execution<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    projection: &AgentPresentationSnapshot,
) -> Result<()> {
    let execution = agent_execution::Entity::find_by_id(execution_id.to_owned())
        .one(db)
        .await
        .context("failed to load Agent notification execution")?
        .context("Agent notification execution is missing")?;
    let identity = agent_identity::Entity::find_by_id(execution.agent_identity_id.clone())
        .one(db)
        .await
        .context("failed to load Agent notification identity")?
        .context("Agent notification identity is missing")?;
    let snapshot_id = execution
        .presentation_snapshot_id
        .as_deref()
        .context("Agent notification execution has no presentation snapshot")?;
    let snapshot = agent_presentation_snapshot::Entity::find_by_id(snapshot_id.to_owned())
        .one(db)
        .await
        .context("failed to load Agent notification presentation snapshot")?
        .context("Agent notification presentation snapshot is missing")?;
    let immutable = agent_presentation_snapshot_from_rows(&identity, &execution, &snapshot)?;
    if projection.agent_identity_id != immutable.agent_identity_id
        || projection.agent_execution_id != immutable.agent_execution_id
        || projection.identity_source_kind != immutable.identity_source_kind
        || projection.identity_source_revision != immutable.identity_source_revision
        || projection.display_name != immutable.display_name
        || projection.nickname != immutable.nickname
        || projection.avatar_revision != immutable.avatar_revision
        || projection.role_label != immutable.role_label
    {
        bail!("Agent notification author differs from its immutable execution presentation");
    }
    Ok(())
}

pub async fn ensure_agent_identity<C: ConnectionTrait>(
    db: &C,
    input: &AgentIdentityInput,
) -> Result<agent_identity::Model> {
    if input.source_revision < 1 {
        bail!("agent identity source revision must be positive");
    }
    agent_identity::Entity::insert(agent_identity::ActiveModel {
        id: Set(input.id.clone()),
        workspace_id: Set(input.workspace_id.clone()),
        source_kind: Set(input.source_kind.clone()),
        source_id: Set(input.source_id.clone()),
        source_revision: Set(input.source_revision),
        source_fingerprint: Set(input.source_fingerprint.clone()),
        status: Set("active".to_owned()),
        created_at: Set(input.now),
        updated_at: Set(input.now),
        retired_at: Set(None),
    })
    .on_conflict(
        OnConflict::columns([
            agent_identity::Column::WorkspaceId,
            agent_identity::Column::SourceKind,
            agent_identity::Column::SourceId,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist agent identity")?;
    let identity = load_agent_identity_by_source(
        db,
        &input.workspace_id,
        &input.source_kind,
        &input.source_id,
    )
    .await?
    .context("agent identity disappeared after idempotent insert")?;
    if identity.id != input.id
        || identity.source_revision != input.source_revision
        || identity.source_fingerprint != input.source_fingerprint
    {
        bail!("agent identity source was reused with different immutable facts");
    }
    Ok(identity)
}

pub async fn claim_actor_nickname<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    nickname_key: &str,
    owner_kind: &str,
    owner_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<actor_nickname_index::Model> {
    if !matches!(
        owner_kind,
        NICKNAME_OWNER_PRINCIPAL | NICKNAME_OWNER_AGENT | NICKNAME_OWNER_RESERVED
    ) {
        bail!("unsupported actor nickname owner kind `{owner_kind}`");
    }
    let normalized = nickname_key.trim().to_ascii_lowercase();
    if normalized.len() < 2
        || normalized.len() > 32
        || normalized.chars().any(|c| {
            !(c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'))
        })
    {
        bail!("invalid actor nickname key");
    }
    if let Some(existing) =
        actor_nickname_index::Entity::find_by_id((workspace_id.to_owned(), normalized.clone()))
            .one(db)
            .await
            .context("failed to inspect actor nickname claim")?
    {
        if existing.status != NICKNAME_ACTIVE
            || existing.owner_kind != owner_kind
            || existing.owner_id != owner_id
        {
            bail!("actor nickname is already owned or tombstoned");
        }
        return Ok(existing);
    }

    // A source rename keeps the owner identity but retires the previous
    // handle.  Tombstoning the old row before claiming the new key preserves
    // historical lookups while ensuring only one active nickname belongs to
    // an actor.  Callers performing a multi-row mutation pass a transaction,
    // so a failed new claim rolls the tombstone back with the source update.
    let previous = actor_nickname_index::Entity::find()
        .filter(actor_nickname_index::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(actor_nickname_index::Column::OwnerKind.eq(owner_kind.to_owned()))
        .filter(actor_nickname_index::Column::OwnerId.eq(owner_id.to_owned()))
        .filter(actor_nickname_index::Column::Status.eq(NICKNAME_ACTIVE))
        .limit(2)
        .all(db)
        .await
        .context("failed to inspect previous actor nickname claim")?;
    if previous.len() > 1 {
        bail!("actor has multiple active nickname claims");
    }
    if let Some(previous) = previous.into_iter().next() {
        if previous.nickname_key != normalized {
            let mut retired: actor_nickname_index::ActiveModel = previous.into();
            retired.status = Set(NICKNAME_TOMBSTONED.to_owned());
            retired.tombstoned_at = Set(Some(now.clone()));
            retired
                .update(db)
                .await
                .context("failed to tombstone previous actor nickname")?;
        }
    }

    let insert_result = actor_nickname_index::Entity::insert(actor_nickname_index::ActiveModel {
        workspace_id: Set(workspace_id.to_owned()),
        nickname_key: Set(normalized.clone()),
        owner_kind: Set(owner_kind.to_owned()),
        owner_id: Set(owner_id.to_owned()),
        status: Set(NICKNAME_ACTIVE.to_owned()),
        claimed_at: Set(now),
        tombstoned_at: Set(None),
    })
    .on_conflict(OnConflict::new().do_nothing().to_owned())
    .exec(db)
    .await;
    let row = actor_nickname_index::Entity::find_by_id((workspace_id.to_owned(), normalized))
        .one(db)
        .await
        .context("failed to reload actor nickname claim")?
        .context("actor nickname claim disappeared after insert")?;
    match insert_result {
        Ok(_) => {}
        Err(_error)
            if row.status == NICKNAME_ACTIVE
                && row.owner_kind == owner_kind
                && row.owner_id == owner_id =>
        {
            return Ok(row);
        }
        Err(error) => return Err(error).context("failed to claim actor nickname"),
    }
    if row.status != NICKNAME_ACTIVE || row.owner_kind != owner_kind || row.owner_id != owner_id {
        bail!("actor nickname is already owned or tombstoned");
    }
    Ok(row)
}

pub async fn insert_presentation_snapshot<C: ConnectionTrait>(
    db: &C,
    input: &PresentationSnapshotInput,
) -> Result<agent_presentation_snapshot::Model> {
    use pioneer_entity::agent_presentation_snapshot;
    let existing = agent_presentation_snapshot::Entity::find()
        .filter(
            agent_presentation_snapshot::Column::AgentIdentityId
                .eq(input.agent_identity_id.clone()),
        )
        .filter(agent_presentation_snapshot::Column::SourceRevision.eq(input.source_revision))
        .filter(
            agent_presentation_snapshot::Column::SourceFingerprint
                .eq(input.source_fingerprint.clone()),
        )
        .one(db)
        .await
        .context("failed to inspect presentation snapshot")?;
    if let Some(existing) = existing {
        if existing.id != input.id
            || existing.display_name != input.display_name
            || existing.nickname != input.nickname
            || existing.avatar_revision != input.avatar_revision
            || existing.role_label != input.role_label
        {
            bail!("presentation snapshot source was reused with different immutable facts");
        }
        return Ok(existing);
    }

    let insert_result =
        agent_presentation_snapshot::Entity::insert(agent_presentation_snapshot::ActiveModel {
            id: Set(input.id.clone()),
            agent_identity_id: Set(input.agent_identity_id.clone()),
            source_revision: Set(input.source_revision),
            source_fingerprint: Set(input.source_fingerprint.clone()),
            display_name: Set(input.display_name.clone()),
            nickname: Set(input.nickname.clone()),
            avatar_revision: Set(input.avatar_revision.clone()),
            role_label: Set(input.role_label.clone()),
            created_at: Set(input.now),
        })
        .on_conflict(
            OnConflict::columns([
                agent_presentation_snapshot::Column::AgentIdentityId,
                agent_presentation_snapshot::Column::SourceRevision,
                agent_presentation_snapshot::Column::SourceFingerprint,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec(db)
        .await;
    let row = agent_presentation_snapshot::Entity::find()
        .filter(
            agent_presentation_snapshot::Column::AgentIdentityId
                .eq(input.agent_identity_id.clone()),
        )
        .filter(agent_presentation_snapshot::Column::SourceRevision.eq(input.source_revision))
        .filter(
            agent_presentation_snapshot::Column::SourceFingerprint
                .eq(input.source_fingerprint.clone()),
        )
        .one(db)
        .await
        .context("failed to reload presentation snapshot")?
        .context("presentation snapshot disappeared after idempotent insert")?;
    if let Err(error) = insert_result {
        if row.id == input.id
            && row.source_revision == input.source_revision
            && row.source_fingerprint == input.source_fingerprint
            && row.display_name == input.display_name
            && row.nickname == input.nickname
            && row.avatar_revision == input.avatar_revision
            && row.role_label == input.role_label
        {
            return Ok(row);
        }
        return Err(error).context("failed to persist immutable agent presentation snapshot");
    }
    Ok(row)
}

pub async fn insert_agent_execution<C: ConnectionTrait>(
    db: &C,
    input: &AgentExecutionInput,
) -> Result<agent_execution::Model> {
    if input.execution_generation < 1 {
        bail!("execution generation must be positive");
    }
    agent_execution::Entity::insert(agent_execution::ActiveModel {
        id: Set(input.id.clone()),
        workspace_id: Set(input.workspace_id.clone()),
        agent_identity_id: Set(input.agent_identity_id.clone()),
        identity_source_revision: Set(input.identity_source_revision),
        identity_source_fingerprint: Set(input.identity_source_fingerprint.clone()),
        parent_execution_id: Set(input.parent_execution_id.clone()),
        parent_task_id: Set(input.parent_task_id.clone()),
        parent_thread_id: Set(input.parent_thread_id.clone()),
        home_root_thread_id: Set(input.home_root_thread_id.clone()),
        work_graph_root_execution_id: Set(input.work_graph_root_execution_id.clone()),
        requested_identity_selection_json: Set(input.requested_identity_selection_json.clone()),
        requested_profile_selection_json: Set(input.requested_profile_selection_json.clone()),
        resolved_profile_id: Set(input.resolved_profile_id.clone()),
        resolved_profile_fingerprint: Set(input.resolved_profile_fingerprint.clone()),
        presentation_snapshot_id: Set(input.presentation_snapshot_id.clone()),
        authorization_context_fingerprint: Set(input.authorization_context_fingerprint.clone()),
        execution_generation: Set(input.execution_generation),
        status: Set(input.status.clone()),
        created_at: Set(input.now),
        updated_at: Set(input.now),
        finished_at: Set(None),
    })
    .on_conflict(
        OnConflict::column(agent_execution::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist agent execution")?;
    let execution = agent_execution::Entity::find_by_id(input.id.clone())
        .one(db)
        .await
        .context("failed to reload agent execution")?
        .context("agent execution disappeared after idempotent insert")?;
    if execution.workspace_id != input.workspace_id
        || execution.agent_identity_id != input.agent_identity_id
        || execution.identity_source_revision != input.identity_source_revision
        || execution.identity_source_fingerprint != input.identity_source_fingerprint
        || execution.parent_execution_id != input.parent_execution_id
        || execution.parent_task_id != input.parent_task_id
        || execution.parent_thread_id != input.parent_thread_id
        || execution.home_root_thread_id != input.home_root_thread_id
        || execution.work_graph_root_execution_id != input.work_graph_root_execution_id
        || execution.requested_identity_selection_json != input.requested_identity_selection_json
        || execution.requested_profile_selection_json != input.requested_profile_selection_json
        || execution.resolved_profile_id != input.resolved_profile_id
        || execution.resolved_profile_fingerprint != input.resolved_profile_fingerprint
        || execution.presentation_snapshot_id != input.presentation_snapshot_id
        || execution.authorization_context_fingerprint != input.authorization_context_fingerprint
        || execution.execution_generation != input.execution_generation
    {
        bail!("agent execution id was reused with different immutable execution facts");
    }
    Ok(execution)
}

pub async fn load_agent_execution<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
) -> Result<Option<agent_execution::Model>> {
    agent_execution::Entity::find_by_id(execution_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent execution")
}

pub async fn load_agent_execution_resource_state<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
) -> Result<Option<agent_execution_resource_state::Model>> {
    agent_execution_resource_state::Entity::find()
        .filter(agent_execution_resource_state::Column::ExecutionId.eq(execution_id.to_owned()))
        .order_by_desc(agent_execution_resource_state::Column::AttemptGeneration)
        .one(db)
        .await
        .context("failed to load agent execution resource state")
}

/// Build the bounded, payload-safe work-graph projection for the exact root
/// Agent Turn. Descendant Turns intentionally return no duplicate projection.
pub async fn load_agent_work_graph_projection_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<AgentWorkGraphProjection>> {
    let Some(response) = load_agent_turn_response(db, turn_id).await? else {
        return Ok(None);
    };
    let execution = agent_execution::Entity::find_by_id(response.execution_id)
        .one(db)
        .await
        .context("failed to load work-graph Turn execution")?
        .context("work-graph Turn references a missing AgentExecution")?;
    if execution.id != execution.work_graph_root_execution_id {
        return Ok(None);
    }
    let scope = agent_work_resource_scope::Entity::find_by_id(execution.id.clone())
        .one(db)
        .await
        .context("failed to load root Agent work-graph scope")?
        .context("root Agent Turn has no work-graph resource scope")?;
    if !agent_work_resource_limits_are_bounded(
        scope.max_concurrency,
        scope.max_queue_depth,
        scope.max_depth,
        scope.max_fan_out,
        scope.max_total_nodes,
    ) {
        bail!("root Agent work graph has invalid bounded limits");
    }
    let row_limit = u64::try_from(scope.max_total_nodes)
        .context("Agent work-graph node limit is invalid")?
        .checked_add(1)
        .context("Agent work-graph node limit overflow")?;
    let executions = agent_execution::Entity::find()
        .filter(
            agent_execution::Column::WorkGraphRootExecutionId.eq(scope.root_execution_id.clone()),
        )
        .order_by_asc(agent_execution::Column::CreatedAt)
        .order_by_asc(agent_execution::Column::Id)
        .limit(row_limit)
        .all(db)
        .await
        .context("failed to load bounded Agent work-graph projection")?;
    if executions.len() > usize::try_from(scope.max_total_nodes).unwrap_or_default() {
        bail!("Agent work-graph projection exceeds its bounded node limit");
    }

    let mut latest_states = BTreeMap::new();
    for execution_chunk in executions.chunks(AGENT_PROJECTION_BATCH_LIMIT) {
        let execution_ids = execution_chunk
            .iter()
            .map(|execution| execution.id.clone())
            .collect::<Vec<_>>();
        let latest_attempts = agent_execution_resource_state::Entity::find()
            .select_only()
            .column(agent_execution_resource_state::Column::ExecutionId)
            .column_as(
                agent_execution_resource_state::Column::AttemptGeneration.max(),
                "latest_attempt_generation",
            )
            .filter(
                agent_execution_resource_state::Column::ExecutionId
                    .is_in(execution_ids.iter().cloned()),
            )
            .group_by(agent_execution_resource_state::Column::ExecutionId)
            .into_tuple::<(String, i64)>()
            .all(db)
            .await
            .context("failed to load latest Agent work-graph attempt generations")?;
        let mut exact_attempts = Condition::any();
        for (execution_id, attempt_generation) in latest_attempts {
            exact_attempts = exact_attempts.add(
                Condition::all()
                    .add(agent_execution_resource_state::Column::ExecutionId.eq(execution_id))
                    .add(
                        agent_execution_resource_state::Column::AttemptGeneration
                            .eq(attempt_generation),
                    ),
            );
        }
        if exact_attempts.is_empty() {
            continue;
        }
        for state in agent_execution_resource_state::Entity::find()
            .filter(exact_attempts)
            .all(db)
            .await
            .context("failed to load latest Agent work-graph resource states")?
        {
            if latest_states
                .insert(state.execution_id.clone(), state)
                .is_some()
            {
                bail!("Agent work-graph projection has duplicate latest resource state");
            }
        }
    }

    let mut queued_count = 0u64;
    let mut running_count = 0u64;
    let mut terminal_count = 0u64;
    let mut updated_at_unix_micros = scope.updated_at.timestamp_micros();
    let mut nodes = Vec::with_capacity(executions.len());
    for execution in executions {
        let state = latest_states
            .remove(execution.id.as_str())
            .with_context(|| {
                format!(
                    "Agent work-graph execution `{}` has no resource state",
                    execution.id
                )
            })?;
        let node_state = agent_work_node_state(execution.status.as_str(), state.status.as_str())?;
        updated_at_unix_micros = std::cmp::Ord::max(
            std::cmp::Ord::max(
                updated_at_unix_micros,
                execution.updated_at.timestamp_micros(),
            ),
            state.updated_at.timestamp_micros(),
        );
        match node_state {
            AgentWorkNodeState::Queued => queued_count = queued_count.saturating_add(1),
            AgentWorkNodeState::Running => running_count = running_count.saturating_add(1),
            AgentWorkNodeState::Completed
            | AgentWorkNodeState::Failed
            | AgentWorkNodeState::Cancelled
            | AgentWorkNodeState::Blocked => {
                terminal_count = terminal_count.saturating_add(1);
            }
        }
        nodes.push(AgentWorkNodeProjection {
            execution_id: AgentExecutionId::new(execution.id)
                .map_err(|error| anyhow!("invalid persisted AgentExecution ID: {error:?}"))?,
            state: node_state,
            progress_revision: u64::try_from(state.progress_sequence)
                .context("Agent work-graph progress sequence is invalid")?,
            progress_label: None,
        });
    }
    if !latest_states.is_empty() {
        bail!("Agent work-graph projection loaded resource state outside its exact graph");
    }

    Ok(Some(AgentWorkGraphProjection {
        root_execution_id: AgentExecutionId::new(scope.root_execution_id)
            .map_err(|error| anyhow!("invalid persisted root AgentExecution ID: {error:?}"))?,
        updated_at_unix_micros,
        queued_count,
        running_count,
        terminal_count,
        saturated: queued_count > 0,
        nodes,
    }))
}

/// Resolve the first canonical response Turn whose `TurnWorkBlock` owns this
/// graph's live projection. A Task occurrence can later answer revision Turns,
/// but those continuations do not move the graph projection between timeline
/// rows.
pub async fn load_agent_work_graph_projection_target<C: ConnectionTrait>(
    db: &C,
    root_execution_id: &str,
) -> Result<Option<AgentWorkGraphProjectionTarget>> {
    let Some(root) = agent_execution::Entity::find_by_id(root_execution_id.to_owned())
        .one(db)
        .await
        .context("failed to load Agent work-graph projection root")?
    else {
        return Ok(None);
    };
    if root.id != root.work_graph_root_execution_id || root.parent_execution_id.is_some() {
        bail!("Agent work-graph projection target is not an exact root execution");
    }
    let response = agent_turn_response_execution::Entity::find()
        .filter(agent_turn_response_execution::Column::ExecutionId.eq(root_execution_id.to_owned()))
        .order_by_asc(agent_turn_response_execution::Column::CreatedAt)
        .order_by_asc(agent_turn_response_execution::Column::TurnId)
        .one(db)
        .await
        .context("failed to resolve root Agent work-graph Turn")?;
    let Some(response) = response else {
        return Ok(None);
    };
    let turn = turn::Entity::find_by_id(response.turn_id.clone())
        .one(db)
        .await
        .context("failed to load root Agent work-graph Turn")?
        .context("root Agent work-graph response Turn is missing")?;
    let thread = thread::Entity::find_by_id(turn.thread_id.clone())
        .one(db)
        .await
        .context("failed to load root Agent work-graph Thread")?
        .context("root Agent work-graph Thread is missing")?;
    if thread.workspace_id != root.workspace_id {
        bail!("root Agent work-graph projection crosses workspace boundary");
    }
    Ok(Some(AgentWorkGraphProjectionTarget {
        workspace_id: root.workspace_id,
        thread_id: turn.thread_id,
        turn_id: turn.id,
    }))
}

fn agent_work_node_state(
    execution_status: &str,
    resource_status: &str,
) -> Result<AgentWorkNodeState> {
    let state = match execution_status {
        "completed" | "succeeded" => AgentWorkNodeState::Completed,
        "failed" | "timed_out" => AgentWorkNodeState::Failed,
        "cancelled" => AgentWorkNodeState::Cancelled,
        "blocked" => AgentWorkNodeState::Blocked,
        "created" | "queued" | "recovering" | "running" => match resource_status {
            "queued" | "paused" => AgentWorkNodeState::Queued,
            "running" => AgentWorkNodeState::Running,
            "completed" => AgentWorkNodeState::Completed,
            "failed" => AgentWorkNodeState::Failed,
            "cancelled" => AgentWorkNodeState::Cancelled,
            other => bail!("unknown Agent work-graph resource state `{other}`"),
        },
        other => bail!("unknown Agent work-graph execution state `{other}`"),
    };
    Ok(state)
}

pub async fn insert_agent_delegation_route(
    db: &DatabaseTransaction,
    input: &AgentDelegationRouteInput,
) -> Result<agent_delegation_route::Model> {
    if !matches!(input.status.as_str(), "prepared" | "active") {
        bail!("agent delegation route can only be created prepared or active");
    }
    if input.source_capsule_id.as_deref() != input.destination_capsule_id.as_deref()
        && input.source_capsule_id.is_some()
        && input.destination_capsule_id.is_some()
    {}
    validate_agent_delegation_route_input(db, input).await?;
    agent_delegation_route::Entity::insert(agent_delegation_route::ActiveModel {
        id: Set(input.id.clone()),
        source_execution_id: Set(input.source_execution_id.clone()),
        destination_thread_id: Set(input.destination_thread_id.clone()),
        source_capsule_id: Set(input.source_capsule_id.clone()),
        destination_capsule_id: Set(input.destination_capsule_id.clone()),
        source_workspace_id: Set(input.source_workspace_id.clone()),
        destination_workspace_id: Set(input.destination_workspace_id.clone()),
        source_gateway_id: Set(input.source_gateway_id.clone()),
        destination_gateway_id: Set(input.destination_gateway_id.clone()),
        source_identity_id: Set(input.source_identity_id.clone()),
        destination_agent_identity_id: Set(input.destination_agent_identity_id.clone()),
        destination_profile_id: Set(input.destination_profile_id.clone()),
        home_capsule_id: Set(input.home_capsule_id.clone()),
        route_kind: Set(input.route_kind.clone()),
        authority_actor_json: Set(Some(input.authority_actor_json.clone())),
        authority_fingerprint: Set(Some(input.authority_fingerprint.clone())),
        allowed_actions_json: Set(input.allowed_actions_json.clone()),
        disclosure_json: Set(input.disclosure_json.clone()),
        route_generation: Set(input.route_generation),
        source_policy_generation: Set(input.source_policy_generation),
        destination_policy_generation: Set(input.destination_policy_generation),
        hop_count: Set(input.hop_count),
        max_hops: Set(input.max_hops),
        return_route_id: Set(input.return_route_id.clone()),
        grant_fingerprint: Set(input.grant_fingerprint.clone()),
        status: Set(input.status.clone()),
        created_at: Set(input.now),
        updated_at: Set(input.updated_at),
        expires_at: Set(input.expires_at),
    })
    .on_conflict(
        OnConflict::column(agent_delegation_route::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist agent delegation route")?;
    let route = agent_delegation_route::Entity::find_by_id(input.id.clone())
        .one(db)
        .await
        .context("failed to reload agent delegation route")?
        .context("agent delegation route disappeared after idempotent insert")?;
    if route.source_execution_id != input.source_execution_id
        || route.destination_thread_id != input.destination_thread_id
        || route.source_capsule_id != input.source_capsule_id
        || route.destination_capsule_id != input.destination_capsule_id
        || route.source_workspace_id != input.source_workspace_id
        || route.destination_workspace_id != input.destination_workspace_id
        || route.source_gateway_id != input.source_gateway_id
        || route.destination_gateway_id != input.destination_gateway_id
        || route.source_identity_id != input.source_identity_id
        || route.destination_agent_identity_id != input.destination_agent_identity_id
        || route.destination_profile_id != input.destination_profile_id
        || route.home_capsule_id != input.home_capsule_id
        || route.route_kind != input.route_kind
        || route.authority_actor_json.as_deref() != Some(input.authority_actor_json.as_str())
        || route.authority_fingerprint.as_deref() != Some(input.authority_fingerprint.as_str())
        || route.allowed_actions_json != input.allowed_actions_json
        || route.disclosure_json != input.disclosure_json
        || route.route_generation != input.route_generation
        || route.source_policy_generation != input.source_policy_generation
        || route.destination_policy_generation != input.destination_policy_generation
        || route.hop_count != input.hop_count
        || route.max_hops != input.max_hops
        || route.return_route_id != input.return_route_id
        || route.grant_fingerprint != input.grant_fingerprint
    {
        bail!("agent delegation route id was reused with different route facts");
    }
    append_agent_route_event(
        db,
        route.id.as_str(),
        "created",
        input.route_generation,
        input.authority_actor_json.as_str(),
        input.authority_fingerprint.as_str(),
        input.now.clone(),
    )
    .await?;
    Ok(route)
}

async fn append_agent_route_event(
    db: &DatabaseTransaction,
    route_id: &str,
    event_kind: &str,
    route_generation: i64,
    authority_actor_json: &str,
    authority_fingerprint: &str,
    occurred_at: DateTimeWithTimeZone,
) -> Result<agent_delegation_route_event::Model> {
    if !matches!(event_kind, "created" | "revoked" | "expired") {
        bail!("agent route lifecycle event kind is invalid");
    }
    let _: PersistedActorRef = serde_json::from_str(authority_actor_json)
        .context("agent route lifecycle actor is invalid")?;
    if route_generation < 1 || authority_fingerprint.trim().is_empty() {
        bail!("agent route lifecycle authority is invalid");
    }
    let id = canonical_agent_id('V', &format!("route-event\0{route_id}\0{route_generation}"));
    agent_delegation_route_event::Entity::insert(agent_delegation_route_event::ActiveModel {
        id: Set(id.clone()),
        route_id: Set(route_id.to_owned()),
        event_kind: Set(event_kind.to_owned()),
        route_generation: Set(route_generation),
        authority_actor_json: Set(authority_actor_json.to_owned()),
        authority_fingerprint: Set(authority_fingerprint.to_owned()),
        occurred_at: Set(occurred_at),
    })
    .on_conflict(
        OnConflict::column(agent_delegation_route_event::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to append Agent route lifecycle event")?;
    let event = agent_delegation_route_event::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload Agent route lifecycle event")?
        .context("Agent route lifecycle event disappeared after append")?;
    if event.route_id != route_id
        || event.event_kind != event_kind
        || event.route_generation != route_generation
        || event.authority_actor_json != authority_actor_json
        || event.authority_fingerprint != authority_fingerprint
    {
        bail!("Agent route lifecycle event id was reused with different facts");
    }
    Ok(event)
}

async fn validate_agent_delegation_route_input<C: ConnectionTrait>(
    db: &C,
    input: &AgentDelegationRouteInput,
) -> Result<()> {
    let kind = match input.route_kind.as_str() {
        "execution_bound" => AgentRouteKind::ExecutionBound,
        "identity_bound" => AgentRouteKind::IdentityBound,
        _ => bail!("agent delegation route has an invalid binding kind"),
    };
    let authority_actor: PersistedActorRef = serde_json::from_str(&input.authority_actor_json)
        .context("agent delegation route authority actor is invalid")?;
    if input.authority_fingerprint.trim().is_empty() {
        bail!("agent delegation route authority fingerprint is required");
    }
    let status = match input.status.as_str() {
        "prepared" => AgentRouteStatus::Prepared,
        "active" => AgentRouteStatus::Active,
        _ => bail!("agent delegation route has an invalid creation status"),
    };
    if input.route_generation != 1 {
        bail!("agent delegation route must begin at generation one");
    }
    let source_capsule_id = input
        .source_capsule_id
        .clone()
        .or_else(|| input.home_capsule_id.clone())
        .context("agent delegation route has no source capsule")?;
    let destination_capsule_id = input
        .destination_capsule_id
        .clone()
        .context("agent delegation route has no destination capsule")?;
    let source_workspace_id = input
        .source_workspace_id
        .clone()
        .context("agent delegation route has no source workspace")?;
    let destination_workspace_id = input
        .destination_workspace_id
        .clone()
        .context("agent delegation route has no destination workspace")?;
    let source_gateway_id = input
        .source_gateway_id
        .clone()
        .context("agent delegation route has no source Gateway")?;
    let destination_gateway_id = input
        .destination_gateway_id
        .clone()
        .context("agent delegation route has no destination Gateway")?;
    let source_identity_id = input
        .source_identity_id
        .as_deref()
        .context("agent delegation route has no source identity")?;
    if let Some(profile_id) = input.destination_profile_id.as_deref() {
        AgentExecutionProfileId::new(profile_id.to_owned())
            .context("agent delegation route destination profile id is invalid")?;
    }
    let projection = AgentDelegationRouteProjection {
        id: AgentDelegationRouteId::new(input.id.clone())
            .context("agent delegation route id is invalid")?,
        source_execution_id: AgentExecutionId::new(input.source_execution_id.clone())
            .context("agent delegation route source execution id is invalid")?,
        source_capsule_id: source_capsule_id.clone(),
        destination_thread_id: input.destination_thread_id.clone(),
        destination_capsule_id: destination_capsule_id.clone(),
        kind,
        status,
        allowed_actions: serde_json::from_str(input.allowed_actions_json.as_str())
            .context("agent delegation route action subset is invalid")?,
        disclosure: serde_json::from_str(input.disclosure_json.as_str())
            .context("agent delegation route disclosure policy is invalid")?,
        source_agent_identity_id: AgentIdentityId::new(source_identity_id.to_owned())
            .context("agent delegation route source identity id is invalid")?,
        destination_agent_identity_id: input
            .destination_agent_identity_id
            .as_deref()
            .map(|id| AgentIdentityId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route destination identity id is invalid")?,
        destination_profile_id: input.destination_profile_id.clone(),
        source_workspace_id: source_workspace_id.clone(),
        destination_workspace_id: destination_workspace_id.clone(),
        source_gateway_id: source_gateway_id.clone(),
        destination_gateway_id: destination_gateway_id.clone(),
        generation: u64::try_from(input.route_generation)
            .context("agent delegation route generation is invalid")?,
        source_policy_generation: u64::try_from(input.source_policy_generation)
            .context("agent delegation route source policy generation is invalid")?,
        destination_policy_generation: u64::try_from(input.destination_policy_generation)
            .context("agent delegation route destination policy generation is invalid")?,
        hop_count: u16::try_from(input.hop_count)
            .context("agent delegation route hop count is invalid")?,
        max_hops: u16::try_from(input.max_hops)
            .context("agent delegation route max hops is invalid")?,
        grant_fingerprint: input.grant_fingerprint.clone(),
        expires_at: input
            .expires_at
            .as_ref()
            .map(|value| value.timestamp_millis()),
        return_route_id: input
            .return_route_id
            .as_deref()
            .map(|id| AgentDelegationRouteId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route return route id is invalid")?,
    };
    projection
        .validate(Some(input.now.timestamp_millis()))
        .map_err(|error| anyhow!("agent delegation route is invalid: {error:?}"))?;
    if projection.max_hops > 8 {
        bail!("agent delegation route exceeds the server hop boundary");
    }
    let current_policy_generation =
        crate::repositories::policy_generation::current_policy_generation_on(db).await?;
    let current_policy_generation = i64::try_from(current_policy_generation.get())
        .context("current policy generation exceeds persistence bounds")?;
    if input.source_policy_generation != current_policy_generation
        || input.destination_policy_generation != current_policy_generation
    {
        bail!("agent delegation route policy generation is stale");
    }
    if source_capsule_id == destination_capsule_id {
        bail!("same-capsule Agent actions must not manufacture route authority");
    }
    if source_capsule_id != destination_capsule_id
        && input.expires_at.is_none()
        && !matches!(
            (&kind, &authority_actor),
            (
                AgentRouteKind::ExecutionBound,
                PersistedActorRef::AgentExecution(actor_execution_id)
            ) if actor_execution_id.as_str() == input.source_execution_id
        )
    {
        bail!(
            "cross-capsule route without a timestamp boundary must be server-owned and execution-bound"
        );
    }
    let source_execution = agent_execution::Entity::find_by_id(input.source_execution_id.clone())
        .one(db)
        .await
        .context("failed to validate route source execution")?
        .context("agent delegation route source execution is missing")?;
    if source_execution.workspace_id != source_workspace_id
        || source_execution.agent_identity_id != source_identity_id
        || source_execution.home_root_thread_id != source_capsule_id
        || matches!(
            source_execution.status.as_str(),
            "completed" | "succeeded" | "failed" | "blocked" | "cancelled" | "timed_out"
        )
        || source_execution.finished_at.is_some()
    {
        bail!("agent delegation route source binding is inconsistent");
    }
    let source_identity = agent_identity::Entity::find_by_id(source_identity_id.to_owned())
        .one(db)
        .await
        .context("failed to validate route source identity")?
        .context("agent delegation route source identity is missing")?;
    if source_identity.workspace_id != source_workspace_id || source_identity.status != "active" {
        bail!("agent delegation route source identity is unavailable");
    }
    if let Some(destination_identity_id) = input.destination_agent_identity_id.as_deref() {
        let destination_identity =
            agent_identity::Entity::find_by_id(destination_identity_id.to_owned())
                .one(db)
                .await
                .context("failed to validate route destination identity")?
                .context("agent delegation route destination identity is missing")?;
        if destination_identity.workspace_id != destination_workspace_id
            || destination_identity.status != "active"
        {
            bail!("agent delegation route destination identity is unavailable");
        }
    }
    match authority_actor {
        PersistedActorRef::AgentExecution(actor_execution_id)
            if actor_execution_id.as_str() != input.source_execution_id =>
        {
            bail!("automatic agent route authority differs from its source execution");
        }
        PersistedActorRef::System => {
            bail!("System cannot create an agent delegation route");
        }
        PersistedActorRef::Principal(_) | PersistedActorRef::AgentExecution(_) => {}
    }
    let destination = thread::Entity::find_by_id(input.destination_thread_id.clone())
        .one(db)
        .await
        .context("failed to validate route destination thread")?
        .context("agent delegation route destination thread is missing")?;
    if destination.workspace_id != destination_workspace_id {
        bail!("agent delegation route destination workspace is inconsistent");
    }
    if source_capsule_id != destination_capsule_id {
        let persisted_destination_capsule = if destination.access_class == "internal" {
            thread_lineage::Entity::find_by_id(destination.id.clone())
                .one(db)
                .await
                .context("failed to validate route destination lineage")?
                .context("cross-capsule route destination has no internal lineage")?
                .root_thread_id
        } else {
            destination.id.clone()
        };
        if persisted_destination_capsule != destination_capsule_id {
            bail!("agent delegation route destination capsule is inconsistent");
        }
    }
    if let Some(return_route_id) = input.return_route_id.as_deref() {
        let return_route = agent_delegation_route::Entity::find_by_id(return_route_id.to_owned())
            .one(db)
            .await
            .context("failed to validate result return route")?
            .context("agent delegation route return edge is missing")?;
        let return_projection = agent_delegation_route_projection(&return_route)
            .context("agent delegation route return edge is invalid")?;
        if return_route.source_policy_generation != current_policy_generation
            || return_route.destination_policy_generation != current_policy_generation
            || !projection
                .can_return_result_via(&return_projection, Some(input.now.timestamp_millis()))
        {
            bail!("agent delegation route return edge does not authorize exact result delivery");
        }
    }
    let existing_routes = agent_delegation_route::Entity::find()
        .filter(agent_delegation_route::Column::Id.ne(input.id.clone()))
        .filter(agent_delegation_route::Column::Status.is_in(["prepared", "active"]))
        .filter(agent_delegation_route::Column::SourceWorkspaceId.eq(source_workspace_id.clone()))
        .filter(
            agent_delegation_route::Column::DestinationWorkspaceId
                .eq(destination_workspace_id.clone()),
        )
        .filter(agent_delegation_route::Column::SourceGatewayId.eq(source_gateway_id.clone()))
        .filter(
            agent_delegation_route::Column::DestinationGatewayId.eq(destination_gateway_id.clone()),
        )
        .filter(
            agent_delegation_route::Column::ExpiresAt
                .is_null()
                .or(agent_delegation_route::Column::ExpiresAt.gt(input.now.clone())),
        )
        .limit((AGENT_ROUTE_GRAPH_MAX_EDGES as u64).saturating_add(1))
        .all(db)
        .await
        .context("failed to load Agent route graph")?;
    if existing_routes.len() > AGENT_ROUTE_GRAPH_MAX_EDGES {
        bail!("Agent route graph exceeds the bounded edge limit");
    }
    let mut edges = Vec::with_capacity(existing_routes.len().saturating_add(1));
    for route in existing_routes {
        let actions =
            serde_json::from_str::<Vec<AgentRouteAction>>(route.allowed_actions_json.as_str())
                .with_context(|| {
                    format!("Agent route `{}` has an invalid action subset", route.id)
                })?;
        if actions == [AgentRouteAction::DeliverResult] {
            continue;
        }
        let source = route
            .source_capsule_id
            .context("Agent route graph edge has no source capsule")?;
        let destination = route
            .destination_capsule_id
            .context("Agent route graph edge has no destination capsule")?;
        if source != destination {
            edges.push((source, destination));
        }
    }
    if source_capsule_id != destination_capsule_id
        && projection.allowed_actions != [AgentRouteAction::DeliverResult]
    {
        edges.push((source_capsule_id, destination_capsule_id));
    }
    pioneer_protocol::validate_agent_route_graph(&edges, AGENT_ROUTE_GRAPH_MAX_EDGES, 8)
        .map_err(|error| anyhow!("agent delegation route graph is invalid: {error:?}"))?;
    Ok(())
}

pub async fn load_agent_delegation_route<C: ConnectionTrait>(
    db: &C,
    route_id: &str,
) -> Result<Option<agent_delegation_route::Model>> {
    agent_delegation_route::Entity::find_by_id(route_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent delegation route")
}

pub fn agent_delegation_route_projection(
    route: &agent_delegation_route::Model,
) -> Result<AgentDelegationRouteProjection> {
    let kind = match route.route_kind.as_str() {
        "execution_bound" => AgentRouteKind::ExecutionBound,
        "identity_bound" => AgentRouteKind::IdentityBound,
        other => bail!("unknown agent delegation route kind `{other}`"),
    };
    let status = match route.status.as_str() {
        "prepared" => AgentRouteStatus::Prepared,
        "active" => AgentRouteStatus::Active,
        "expired" => AgentRouteStatus::Expired,
        "revoked" => AgentRouteStatus::Revoked,
        other => bail!("unknown agent delegation route status `{other}`"),
    };
    route
        .source_capsule_id
        .as_deref()
        .or(route.home_capsule_id.as_deref())
        .context("agent delegation route has no source capsule")?;
    let destination_capsule_id = route
        .destination_capsule_id
        .as_deref()
        .context("agent delegation route has no destination capsule")?
        .to_owned();
    let source_capsule_id = route
        .source_capsule_id
        .as_deref()
        .or(route.home_capsule_id.as_deref())
        .context("agent delegation route has no source capsule")?
        .to_owned();
    let source_workspace_id = route
        .source_workspace_id
        .as_deref()
        .context("agent delegation route has no source workspace")?
        .to_owned();
    let destination_workspace_id = route
        .destination_workspace_id
        .as_deref()
        .context("agent delegation route has no destination workspace")?
        .to_owned();
    let source_gateway_id = route
        .source_gateway_id
        .as_deref()
        .context("agent delegation route has no source gateway")?
        .to_owned();
    let destination_gateway_id = route
        .destination_gateway_id
        .as_deref()
        .context("agent delegation route has no destination gateway")?
        .to_owned();
    let allowed_actions: Vec<AgentRouteAction> = serde_json::from_str(&route.allowed_actions_json)
        .context("agent delegation route has invalid action subset")?;
    let disclosure: AgentRouteDisclosurePolicy = serde_json::from_str(&route.disclosure_json)
        .context("agent delegation route has invalid disclosure policy")?;
    let projection = AgentDelegationRouteProjection {
        id: pioneer_protocol::AgentDelegationRouteId::new(route.id.clone())
            .context("agent delegation route has invalid id")?,
        source_execution_id: AgentExecutionId::new(route.source_execution_id.clone())
            .context("agent delegation route has invalid source execution")?,
        source_capsule_id,
        destination_thread_id: route.destination_thread_id.clone(),
        destination_capsule_id,
        kind,
        status,
        allowed_actions,
        disclosure,
        source_agent_identity_id: route
            .source_identity_id
            .as_deref()
            .map(|id| AgentIdentityId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route has invalid source identity")?
            .context("agent delegation route has no source identity")?,
        destination_agent_identity_id: route
            .destination_agent_identity_id
            .as_deref()
            .map(|id| AgentIdentityId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route has invalid destination identity")?,
        destination_profile_id: route.destination_profile_id.clone(),
        source_workspace_id,
        destination_workspace_id,
        source_gateway_id,
        destination_gateway_id,
        generation: u64::try_from(route.route_generation)
            .context("agent delegation route has invalid generation")?,
        source_policy_generation: u64::try_from(route.source_policy_generation)
            .context("agent delegation route has invalid source policy generation")?,
        destination_policy_generation: u64::try_from(route.destination_policy_generation)
            .context("agent delegation route has invalid destination policy generation")?,
        hop_count: u16::try_from(route.hop_count)
            .context("agent delegation route has invalid hop count")?,
        max_hops: u16::try_from(route.max_hops)
            .context("agent delegation route has invalid max hops")?,
        grant_fingerprint: route.grant_fingerprint.clone(),
        expires_at: route
            .expires_at
            .as_ref()
            .map(|value| value.timestamp_millis()),
        return_route_id: route
            .return_route_id
            .as_deref()
            .map(|id| pioneer_protocol::AgentDelegationRouteId::new(id.to_owned()))
            .transpose()
            .context("agent delegation route has invalid return route")?,
    };
    projection.validate(None).map_err(|error| {
        anyhow!("agent delegation route projection failed validation: {error:?}")
    })?;
    Ok(projection)
}

pub async fn list_agent_delegation_routes<C: ConnectionTrait>(
    db: &C,
    source_execution_id: &str,
    after: Option<(DateTimeWithTimeZone, String)>,
    limit: u64,
) -> Result<Vec<agent_delegation_route::Model>> {
    if limit == 0 || limit > AGENT_PROJECTION_BATCH_LIMIT as u64 + 1 {
        bail!("Agent route list batch exceeds its bounded limit");
    }
    let mut query = agent_delegation_route::Entity::find().filter(
        agent_delegation_route::Column::SourceExecutionId.eq(source_execution_id.to_owned()),
    );
    if let Some((created_at, route_id)) = after {
        query = query.filter(
            Condition::any()
                .add(agent_delegation_route::Column::CreatedAt.gt(created_at.clone()))
                .add(
                    Condition::all()
                        .add(agent_delegation_route::Column::CreatedAt.eq(created_at))
                        .add(agent_delegation_route::Column::Id.gt(route_id)),
                ),
        );
    }
    query
        .order_by_asc(agent_delegation_route::Column::CreatedAt)
        .order_by_asc(agent_delegation_route::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list agent delegation routes")
}

/// List every route that can be bound to this exact execution: direct
/// execution-bound edges plus identity-bound capsule policy for its immutable
/// source identity. Callers still revalidate status, expiry, generations and
/// destination policy before projecting an opaque target option.
pub async fn list_agent_delegation_routes_for_source<C: ConnectionTrait>(
    db: &C,
    source_execution_id: &str,
    source_identity_id: &str,
) -> Result<Vec<agent_delegation_route::Model>> {
    agent_delegation_route::Entity::find()
        .filter(
            Condition::any()
                .add(
                    agent_delegation_route::Column::SourceExecutionId
                        .eq(source_execution_id.to_owned()),
                )
                .add(
                    Condition::all()
                        .add(agent_delegation_route::Column::RouteKind.eq("identity_bound"))
                        .add(
                            agent_delegation_route::Column::SourceIdentityId
                                .eq(source_identity_id.to_owned()),
                        ),
                ),
        )
        .filter(agent_delegation_route::Column::Status.eq("active"))
        .filter(
            agent_delegation_route::Column::ExpiresAt
                .is_null()
                .or(agent_delegation_route::Column::ExpiresAt.gt(utc_now())),
        )
        .order_by_asc(agent_delegation_route::Column::CreatedAt)
        .limit((AGENT_ROUTE_GRAPH_MAX_EDGES as u64).saturating_add(1))
        .all(db)
        .await
        .context("failed to list Agent routes for source binding")
        .and_then(|routes| {
            if routes.len() > AGENT_ROUTE_GRAPH_MAX_EDGES {
                bail!("Agent route binding exceeds the bounded edge limit");
            }
            Ok(routes)
        })
}

/// Revoke only the currently active/prepared generation. The affected-row
/// count is the CAS result, so a concurrent revoke or expiry cannot be
/// mistaken for a successful route mutation.
pub async fn revoke_agent_delegation_route(
    db: &DatabaseTransaction,
    route_id: &str,
    expected_generation: i64,
    expected_policy_generation: i64,
    authority_actor_json: &str,
    authority_fingerprint: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let actor: PersistedActorRef =
        serde_json::from_str(authority_actor_json).context("route revocation actor is invalid")?;
    if !matches!(actor, PersistedActorRef::Principal(_)) {
        bail!("only an authenticated principal may revoke an Agent route");
    }
    if authority_fingerprint.trim().is_empty() {
        bail!("route revocation authority fingerprint is required");
    }
    let current_policy_generation =
        crate::repositories::policy_generation::current_policy_generation_on(db).await?;
    if i64::try_from(current_policy_generation.get())
        .context("current policy generation exceeds persistence bounds")?
        != expected_policy_generation
    {
        bail!("route revocation authority is stale");
    }
    let result = agent_delegation_route::Entity::update_many()
        .col_expr(
            agent_delegation_route::Column::Status,
            sea_orm::sea_query::Expr::value("revoked"),
        )
        .col_expr(
            agent_delegation_route::Column::RouteGeneration,
            sea_orm::sea_query::Expr::cust("route_generation + 1"),
        )
        .col_expr(
            agent_delegation_route::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            agent_delegation_route::Column::AuthorityActorJson,
            sea_orm::sea_query::Expr::value(Some(authority_actor_json.to_owned())),
        )
        .col_expr(
            agent_delegation_route::Column::AuthorityFingerprint,
            sea_orm::sea_query::Expr::value(Some(authority_fingerprint.to_owned())),
        )
        .filter(agent_delegation_route::Column::Id.eq(route_id.to_owned()))
        .filter(agent_delegation_route::Column::RouteGeneration.eq(expected_generation))
        .filter(agent_delegation_route::Column::RouteGeneration.lt(i64::MAX))
        .filter(agent_delegation_route::Column::Status.is_in(["prepared", "active"]))
        .filter(
            agent_delegation_route::Column::ExpiresAt
                .is_null()
                .or(agent_delegation_route::Column::ExpiresAt.gt(now)),
        )
        .exec(db)
        .await
        .context("failed to revoke agent delegation route")?;
    if result.rows_affected != 1 {
        return Ok(false);
    }
    append_agent_route_event(
        db,
        route_id,
        "revoked",
        expected_generation
            .checked_add(1)
            .context("route generation overflow")?,
        authority_actor_json,
        authority_fingerprint,
        now,
    )
    .await?;
    Ok(true)
}

pub async fn expire_agent_delegation_routes(
    db: &DatabaseTransaction,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let candidates = agent_delegation_route::Entity::find()
        .filter(agent_delegation_route::Column::Status.is_in(["prepared", "active"]))
        .filter(agent_delegation_route::Column::ExpiresAt.lte(now))
        .order_by_asc(agent_delegation_route::Column::ExpiresAt)
        .order_by_asc(agent_delegation_route::Column::Id)
        .limit(AGENT_ROUTE_EXPIRY_BATCH_SIZE)
        .all(db)
        .await
        .context("failed to load expiring agent delegation routes")?;
    let authority_actor_json = serde_json::to_string(&PersistedActorRef::System)
        .context("failed to serialize System actor for route expiry")?;
    let authority_fingerprint = "agent-route-expiry";
    let mut expired = 0u64;
    for route in candidates {
        let result = agent_delegation_route::Entity::update_many()
            .col_expr(
                agent_delegation_route::Column::Status,
                sea_orm::sea_query::Expr::value("expired"),
            )
            .col_expr(
                agent_delegation_route::Column::RouteGeneration,
                sea_orm::sea_query::Expr::cust("route_generation + 1"),
            )
            .col_expr(
                agent_delegation_route::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now.clone()),
            )
            .col_expr(
                agent_delegation_route::Column::AuthorityActorJson,
                sea_orm::sea_query::Expr::value(Some(authority_actor_json.clone())),
            )
            .col_expr(
                agent_delegation_route::Column::AuthorityFingerprint,
                sea_orm::sea_query::Expr::value(Some(authority_fingerprint.to_owned())),
            )
            .filter(agent_delegation_route::Column::Id.eq(route.id.clone()))
            .filter(agent_delegation_route::Column::RouteGeneration.eq(route.route_generation))
            .filter(agent_delegation_route::Column::RouteGeneration.lt(i64::MAX))
            .filter(agent_delegation_route::Column::Status.is_in(["prepared", "active"]))
            .filter(agent_delegation_route::Column::ExpiresAt.lte(now.clone()))
            .exec(db)
            .await
            .context("failed to expire agent delegation route")?;
        if result.rows_affected == 0 {
            continue;
        }
        let next_generation = route
            .route_generation
            .checked_add(1)
            .context("route generation overflow")?;
        append_agent_route_event(
            db,
            route.id.as_str(),
            "expired",
            next_generation,
            authority_actor_json.as_str(),
            authority_fingerprint,
            now.clone(),
        )
        .await?;
        expired = expired.saturating_add(1);
    }
    Ok(expired)
}

/// Atomically materialize a server-resolved thread and its execution-scoped
/// continuation route. Callers pass a DatabaseTransaction when this is part
/// of an AgentAction commit; no user-selected participant/ACL data is accepted
/// here.
pub async fn commit_agent_thread_creation(
    db: &DatabaseTransaction,
    input: &AgentThreadCreationCommitInput<'_>,
) -> Result<()> {
    let execution = agent_execution::Entity::find_by_id(input.execution_id.to_string())
        .one(db)
        .await
        .context("failed to validate agent Thread creator execution")?
        .context("agent Thread creator execution is missing")?;
    if execution.workspace_id != input.thread.workspace_id {
        bail!("agent Thread creator execution belongs to another workspace");
    }
    match (&input.lineage, &input.route) {
        (Some(lineage), None) => {
            if lineage.child_thread_id != input.thread.id
                || lineage.root_thread_id != execution.home_root_thread_id
                || lineage.created_by_turn_id.is_none()
                || lineage.created_by_thread_id.as_deref()
                    != Some(lineage.parent_thread_id.as_str())
                || lineage.depth < 1
            {
                bail!("agent internal Thread lineage differs from its exact creator capsule");
            }
            let parent = thread::Entity::find_by_id(lineage.parent_thread_id.clone())
                .one(db)
                .await
                .context("failed to validate agent internal Thread parent")?
                .context("agent internal Thread parent is missing")?;
            if parent.workspace_id != execution.workspace_id {
                bail!("agent internal Thread parent belongs to another workspace");
            }
            if lineage.parent_thread_id == execution.home_root_thread_id {
                if lineage.depth != 1 {
                    bail!("agent internal Thread root-child depth is invalid");
                }
            } else {
                let parent_lineage =
                    thread_lineage::Entity::find_by_id(lineage.parent_thread_id.clone())
                        .one(db)
                        .await
                        .context("failed to validate agent internal Thread parent lineage")?
                        .context("agent internal Thread parent has no durable lineage")?;
                if parent_lineage.root_thread_id != execution.home_root_thread_id
                    || parent_lineage.depth.checked_add(1) != Some(lineage.depth)
                {
                    bail!("agent internal Thread parent lineage is outside its capsule");
                }
            }
        }
        (None, Some(route))
            if route.destination_thread_id == input.thread.id
                && route.source_execution_id == execution.id
                && route.source_capsule_id.as_deref()
                    == Some(execution.home_root_thread_id.as_str())
                && route.destination_capsule_id.as_deref() == Some(input.thread.id.as_str()) => {}
        _ => bail!("agent Thread creation must persist exact internal lineage or a new-root route"),
    }
    let creator = PersistedActorRef::AgentExecution(input.execution_id.clone());
    let access_class = if input.lineage.is_some() {
        crate::PersistedThreadAccessClass::Internal
    } else {
        crate::PersistedThreadAccessClass::Workspace
    };
    crate::repositories::thread::upsert_agent_thread_with_creator(
        db,
        input.thread,
        &creator,
        access_class,
        input.created_at,
        input.updated_at,
    )
    .await
    .context("failed to persist agent-authored thread")?;
    if let Some(lineage) = input.lineage.as_ref() {
        crate::repositories::thread_lineage::upsert_lineage(db, lineage)
            .await
            .context("failed to persist agent internal Thread lineage")?;
    }
    if let Some(route) = input.route.as_ref() {
        insert_agent_delegation_route(db, route)
            .await
            .context("failed to persist agent thread continuation route")?;
    }
    Ok(())
}

pub async fn insert_agent_execution_grant<C: ConnectionTrait>(
    db: &C,
    input: &AgentExecutionGrantInput,
) -> Result<agent_execution_grant::Model> {
    let (identity, profile, child_launch_grant) =
        parse_agent_execution_launch_grant(input.grant_json.as_str())?;
    if input.grant_fingerprint != agent_execution_grant_fingerprint(input.grant_json.as_str())?
        || identity.id.as_str() != input.child_identity_id
        || !child_launch_grant
            .identities
            .iter()
            .any(|candidate| candidate == &identity)
        || !child_launch_grant
            .profiles
            .iter()
            .any(|candidate| candidate == &profile)
    {
        bail!("agent execution grant identity/profile is outside its child launch ceiling");
    }
    agent_execution_grant::Entity::insert(agent_execution_grant::ActiveModel {
        id: Set(input.id.clone()),
        execution_id: Set(input.execution_id.clone()),
        parent_execution_id: Set(input.parent_execution_id.clone()),
        child_identity_id: Set(input.child_identity_id.clone()),
        grant_fingerprint: Set(input.grant_fingerprint.clone()),
        grant_json: Set(input.grant_json.clone()),
        created_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::column(agent_execution_grant::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist agent execution grant")?;
    let grant = agent_execution_grant::Entity::find_by_id(input.id.clone())
        .one(db)
        .await
        .context("failed to reload agent execution grant")?
        .context("agent execution grant disappeared after idempotent insert")?;
    if grant.execution_id != input.execution_id
        || grant.parent_execution_id != input.parent_execution_id
        || grant.child_identity_id != input.child_identity_id
        || grant.grant_fingerprint != input.grant_fingerprint
        || grant.grant_json != input.grant_json
    {
        bail!("agent execution grant id was reused with different immutable facts");
    }
    Ok(grant)
}

pub fn agent_execution_grant_fingerprint(grant_json: &str) -> Result<String> {
    let value: serde_json::Value =
        serde_json::from_str(grant_json).context("agent execution grant JSON is invalid")?;
    let canonical =
        serde_json::to_vec(&value).context("agent execution grant could not be canonicalized")?;
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:execution-grant:v1\0");
    digest.update(canonical);
    Ok(hex::encode(digest.finalize()))
}

fn parse_agent_execution_launch_grant(
    grant_json: &str,
) -> Result<(
    pioneer_protocol::AgentIdentityProjection,
    pioneer_protocol::AgentExecutionProfileProjection,
    pioneer_protocol::ChildAgentLaunchGrantSet,
)> {
    let grant_json: serde_json::Value =
        serde_json::from_str(grant_json).context("agent execution grant JSON is invalid")?;
    let identity: pioneer_protocol::AgentIdentityProjection = serde_json::from_value(
        grant_json
            .get("identity")
            .cloned()
            .context("agent execution grant has no exact identity")?,
    )
    .context("agent execution grant identity is invalid")?;
    let profile: pioneer_protocol::AgentExecutionProfileProjection = serde_json::from_value(
        grant_json
            .get("profile")
            .cloned()
            .context("agent execution grant has no exact profile")?,
    )
    .context("agent execution grant profile is invalid")?;
    let child_launch_grant: pioneer_protocol::ChildAgentLaunchGrantSet = serde_json::from_value(
        grant_json
            .get("child_launch_grant")
            .cloned()
            .context("agent execution grant has no immutable child launch ceiling")?,
    )
    .context("agent execution child launch ceiling is invalid")?;
    child_launch_grant.validate().map_err(|error| {
        anyhow::anyhow!("agent execution child launch ceiling is invalid: {error:?}")
    })?;
    Ok((identity, profile, child_launch_grant))
}

pub async fn load_agent_execution_grant<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
) -> Result<Option<agent_execution_grant::Model>> {
    agent_execution_grant::Entity::find()
        .filter(agent_execution_grant::Column::ExecutionId.eq(execution_id.to_owned()))
        .one(db)
        .await
        .context("failed to load agent execution grant")
}

pub async fn insert_agent_turn_response<C: ConnectionTrait>(
    db: &C,
    input: &AgentTurnResponseInput,
) -> Result<agent_turn_response_execution::Model> {
    if input.turn_id.trim().is_empty() {
        bail!("responding AgentExecution requires a Turn id");
    }
    let execution = agent_execution::Entity::find_by_id(input.execution_id.clone())
        .one(db)
        .await
        .context("failed to validate responding AgentExecution")?
        .context("responding AgentExecution does not exist")?;
    if execution.presentation_snapshot_id.as_deref()
        != Some(input.presentation_snapshot_id.as_str())
    {
        bail!("responding AgentExecution presentation snapshot is inconsistent");
    }
    agent_turn_response_execution::Entity::insert(agent_turn_response_execution::ActiveModel {
        turn_id: Set(input.turn_id.clone()),
        execution_id: Set(input.execution_id.clone()),
        presentation_snapshot_id: Set(input.presentation_snapshot_id.clone()),
        created_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::column(agent_turn_response_execution::Column::TurnId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist responding AgentExecution")?;
    let response = agent_turn_response_execution::Entity::find_by_id(input.turn_id.clone())
        .one(db)
        .await
        .context("failed to reload responding AgentExecution")?
        .context("responding AgentExecution disappeared after idempotent insert")?;
    if response.execution_id != input.execution_id
        || response.presentation_snapshot_id != input.presentation_snapshot_id
    {
        bail!("Agent Turn response was reused with different immutable facts");
    }
    Ok(response)
}

pub async fn load_agent_turn_response<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<agent_turn_response_execution::Model>> {
    agent_turn_response_execution::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to load responding AgentExecution")
}

pub async fn load_agent_turn_responses_for_turns<C: ConnectionTrait>(
    db: &C,
    turn_ids: &[String],
) -> Result<Vec<agent_turn_response_execution::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }
    if turn_ids.len() > AGENT_PROJECTION_BATCH_LIMIT {
        bail!("Agent Turn response projection batch exceeds its bounded limit");
    }
    agent_turn_response_execution::Entity::find()
        .filter(agent_turn_response_execution::Column::TurnId.is_in(turn_ids.iter().cloned()))
        .all(db)
        .await
        .context("failed to batch-load responding AgentExecutions")
}

pub async fn load_agent_authors_for_executions<C: ConnectionTrait>(
    db: &C,
    execution_ids: &[String],
) -> Result<BTreeMap<String, AgentExecutionAuthorProjection>> {
    if execution_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    if execution_ids.len() > AGENT_PROJECTION_BATCH_LIMIT {
        bail!("Agent author projection batch exceeds its bounded limit");
    }
    let executions = agent_execution::Entity::find()
        .filter(agent_execution::Column::Id.is_in(execution_ids.iter().cloned()))
        .all(db)
        .await
        .context("failed to batch-load author AgentExecutions")?;
    let identity_ids = executions
        .iter()
        .map(|execution| execution.agent_identity_id.clone())
        .collect::<Vec<_>>();
    let snapshot_ids = executions
        .iter()
        .filter_map(|execution| execution.presentation_snapshot_id.clone())
        .collect::<Vec<_>>();
    let identities = agent_identity::Entity::find()
        .filter(agent_identity::Column::Id.is_in(identity_ids))
        .all(db)
        .await
        .context("failed to batch-load author AgentIdentities")?;
    let snapshots = agent_presentation_snapshot::Entity::find()
        .filter(agent_presentation_snapshot::Column::Id.is_in(snapshot_ids))
        .all(db)
        .await
        .context("failed to batch-load author presentation snapshots")?;
    let identities_by_id = identities
        .iter()
        .map(|identity| (identity.id.as_str(), identity))
        .collect::<BTreeMap<_, _>>();
    let snapshots_by_id = snapshots
        .iter()
        .map(|snapshot| (snapshot.id.as_str(), snapshot))
        .collect::<BTreeMap<_, _>>();
    let mut result = BTreeMap::new();
    for execution in executions {
        let identity = identities_by_id
            .get(execution.agent_identity_id.as_str())
            .context("author AgentExecution identity is missing")?;
        let snapshot_id = execution
            .presentation_snapshot_id
            .as_deref()
            .context("author AgentExecution has no presentation snapshot")?;
        let snapshot = snapshots_by_id
            .get(snapshot_id)
            .context("author AgentExecution presentation snapshot is missing")?;
        let author = agent_presentation_snapshot_from_rows(identity, &execution, snapshot)?
            .to_turn_author_snapshot();
        result.insert(
            execution.id,
            AgentExecutionAuthorProjection {
                author,
                presentation_snapshot_id: snapshot_id.to_owned(),
            },
        );
    }
    Ok(result)
}

pub async fn insert_agent_resource_state<C: ConnectionTrait>(
    db: &C,
    input: &AgentResourceStateInput,
) -> Result<agent_execution_resource_state::Model> {
    if input.attempt_generation < 1 {
        bail!("resource attempt generation must be positive");
    }
    agent_execution_resource_state::Entity::insert(agent_execution_resource_state::ActiveModel {
        id: Set(input.id.clone()),
        execution_id: Set(input.execution_id.clone()),
        attempt_generation: Set(input.attempt_generation),
        progress_sequence: Set(0),
        progress_frontier_json: Set("{}".to_owned()),
        last_progress_at: Set(None),
        last_heartbeat_at: Set(None),
        idle_deadline: Set(None),
        hard_deadline: Set(None),
        local_usage_json: Set("{}".to_owned()),
        permit_id: Set(None),
        branch_key: Set(input.branch_key.clone()),
        fair_order: Set(input.fair_order),
        status: Set("queued".to_owned()),
        fencing_generation: Set(1),
        fenced_at: Set(None),
        created_at: Set(input.now),
        updated_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::columns([
            agent_execution_resource_state::Column::ExecutionId,
            agent_execution_resource_state::Column::AttemptGeneration,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist execution resource state")?;
    let state = agent_execution_resource_state::Entity::find()
        .filter(agent_execution_resource_state::Column::ExecutionId.eq(input.execution_id.clone()))
        .filter(
            agent_execution_resource_state::Column::AttemptGeneration.eq(input.attempt_generation),
        )
        .one(db)
        .await
        .context("failed to reload execution resource state")?
        .context("execution resource state disappeared after idempotent insert")?;
    if state.id != input.id
        || state.branch_key != input.branch_key
        || state.fair_order != input.fair_order
    {
        bail!("agent resource state was reused with different immutable facts");
    }
    Ok(state)
}

pub async fn enqueue_agent_execution<C: ConnectionTrait>(
    db: &C,
    input: &AgentQueueEntryInput,
) -> Result<agent_work_queue::Model> {
    ensure_agent_branch_schedule(
        db,
        input.root_execution_id.as_str(),
        input.branch_key.as_str(),
        input.now.clone(),
    )
    .await?;
    agent_work_queue::Entity::insert(agent_work_queue::ActiveModel {
        id: Set(input.id.clone()),
        root_execution_id: Set(input.root_execution_id.clone()),
        execution_id: Set(input.execution_id.clone()),
        attempt_generation: Set(input.attempt_generation),
        branch_key: Set(input.branch_key.clone()),
        enqueue_sequence: Set(input.enqueue_sequence),
        state: Set("queued".to_owned()),
        eligible_at: Set(input.eligible_at),
        claim_token: Set(None),
        created_at: Set(input.now),
        updated_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::columns([
            agent_work_queue::Column::RootExecutionId,
            agent_work_queue::Column::ExecutionId,
            agent_work_queue::Column::AttemptGeneration,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to enqueue agent execution")?;
    let queue = agent_work_queue::Entity::find()
        .filter(agent_work_queue::Column::RootExecutionId.eq(input.root_execution_id.clone()))
        .filter(agent_work_queue::Column::ExecutionId.eq(input.execution_id.clone()))
        .filter(agent_work_queue::Column::AttemptGeneration.eq(input.attempt_generation))
        .one(db)
        .await
        .context("failed to reload agent work queue entry")?
        .context("agent work queue entry disappeared after idempotent insert")?;
    if queue.id != input.id
        || queue.branch_key != input.branch_key
        || queue.enqueue_sequence != input.enqueue_sequence
        || queue.eligible_at != input.eligible_at
    {
        bail!("agent work queue entry was reused with different immutable facts");
    }
    Ok(queue)
}

pub(super) async fn ensure_agent_branch_schedule<C: ConnectionTrait>(
    db: &C,
    root_execution_id: &str,
    branch_key: &str,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    if branch_key.trim().is_empty() || branch_key.len() > 255 {
        bail!("agent work branch key is invalid");
    }
    agent_work_branch_schedule::Entity::insert(agent_work_branch_schedule::ActiveModel {
        root_execution_id: Set(root_execution_id.to_owned()),
        branch_key: Set(branch_key.to_owned()),
        last_scheduled_sequence: Set(0),
        last_scheduled_at: Set(None),
        created_at: Set(now.clone()),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            agent_work_branch_schedule::Column::RootExecutionId,
            agent_work_branch_schedule::Column::BranchKey,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist agent work branch schedule")?;
    Ok(())
}

async fn mark_agent_branch_scheduled(
    db: &DatabaseTransaction,
    root_execution_id: &str,
    branch_key: &str,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    ensure_agent_branch_schedule(db, root_execution_id, branch_key, now.clone()).await?;
    let sequence = next_agent_schedule_sequence(db, now.clone()).await?;
    let updated = agent_work_branch_schedule::Entity::update_many()
        .col_expr(
            agent_work_branch_schedule::Column::LastScheduledSequence,
            sea_orm::sea_query::Expr::value(sequence),
        )
        .col_expr(
            agent_work_branch_schedule::Column::LastScheduledAt,
            sea_orm::sea_query::Expr::value(Some(now.clone())),
        )
        .col_expr(
            agent_work_branch_schedule::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(
            agent_work_branch_schedule::Column::RootExecutionId.eq(root_execution_id.to_owned()),
        )
        .filter(agent_work_branch_schedule::Column::BranchKey.eq(branch_key.to_owned()))
        .exec(db)
        .await
        .context("failed to advance agent work branch scheduling cursor")?;
    if updated.rows_affected != 1 {
        bail!("agent work branch schedule disappeared during admission");
    }
    let updated = agent_work_resource_scope::Entity::update_many()
        .col_expr(
            agent_work_resource_scope::Column::LastScheduledSequence,
            sea_orm::sea_query::Expr::value(sequence),
        )
        .col_expr(
            agent_work_resource_scope::Column::LastScheduledAt,
            sea_orm::sea_query::Expr::value(Some(now.clone())),
        )
        .col_expr(
            agent_work_resource_scope::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(agent_work_resource_scope::Column::RootExecutionId.eq(root_execution_id.to_owned()))
        .filter(agent_work_resource_scope::Column::Status.eq("active"))
        .exec(db)
        .await
        .context("failed to advance root work-graph scheduling cursor")?;
    if updated.rows_affected != 1 {
        bail!("agent work root scope changed during scheduling");
    }
    Ok(())
}

async fn next_agent_schedule_sequence(
    db: &DatabaseTransaction,
    now: DateTimeWithTimeZone,
) -> Result<i64> {
    const GLOBAL_SCHEDULER: &str = "global";
    agent_work_scheduler_state::Entity::insert(agent_work_scheduler_state::ActiveModel {
        scheduler_key: Set(GLOBAL_SCHEDULER.to_owned()),
        schedule_generation: Set(0),
        updated_at: Set(now.clone()),
    })
    .on_conflict(
        OnConflict::column(agent_work_scheduler_state::Column::SchedulerKey)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to seed global Agent work scheduler cursor")?;
    let updated = agent_work_scheduler_state::Entity::update_many()
        .col_expr(
            agent_work_scheduler_state::Column::ScheduleGeneration,
            sea_orm::sea_query::Expr::col(agent_work_scheduler_state::Column::ScheduleGeneration)
                .add(1),
        )
        .col_expr(
            agent_work_scheduler_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(agent_work_scheduler_state::Column::SchedulerKey.eq(GLOBAL_SCHEDULER))
        .filter(agent_work_scheduler_state::Column::ScheduleGeneration.lt(i64::MAX))
        .exec(db)
        .await
        .context("failed to advance global Agent work scheduler cursor")?;
    if updated.rows_affected != 1 {
        bail!("global Agent work scheduler generation is exhausted");
    }
    agent_work_scheduler_state::Entity::find_by_id(GLOBAL_SCHEDULER)
        .one(db)
        .await
        .context("failed to reload global Agent work scheduler cursor")?
        .map(|state| state.schedule_generation)
        .context("global Agent work scheduler cursor disappeared")
}

pub async fn acquire_agent_running_permit<C: ConnectionTrait>(
    db: &C,
    permit_id: &str,
    root_execution_id: &str,
    execution_id: &str,
    attempt_generation: i64,
    now: DateTimeWithTimeZone,
) -> Result<agent_running_permit::Model> {
    if attempt_generation < 1 {
        bail!("permit attempt generation must be positive");
    }
    agent_running_permit::Entity::insert(agent_running_permit::ActiveModel {
        id: Set(permit_id.to_owned()),
        root_execution_id: Set(root_execution_id.to_owned()),
        execution_id: Set(execution_id.to_owned()),
        attempt_generation: Set(attempt_generation),
        lease_generation: Set(1),
        status: Set("held".to_owned()),
        acquired_at: Set(now),
        released_at: Set(None),
        fenced_at: Set(None),
    })
    .on_conflict(
        OnConflict::columns([
            agent_running_permit::Column::RootExecutionId,
            agent_running_permit::Column::ExecutionId,
            agent_running_permit::Column::AttemptGeneration,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist running permit")?;
    let permit = agent_running_permit::Entity::find()
        .filter(agent_running_permit::Column::RootExecutionId.eq(root_execution_id.to_owned()))
        .filter(agent_running_permit::Column::ExecutionId.eq(execution_id.to_owned()))
        .filter(agent_running_permit::Column::AttemptGeneration.eq(attempt_generation))
        .one(db)
        .await
        .context("failed to reload running permit")?
        .context("running permit disappeared after idempotent insert")?;
    if permit.id != permit_id
        || permit.lease_generation != 1
        || permit.root_execution_id != root_execution_id
        || permit.execution_id != execution_id
        || permit.attempt_generation != attempt_generation
    {
        bail!("running permit was reused with different immutable facts");
    }
    if permit.status != "held" {
        bail!("running permit for this execution attempt is no longer held");
    }
    Ok(permit)
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
    let candidate_rows = db
        .query_all_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "WITH branch_heads AS (\
               SELECT q.id, q.root_execution_id, q.branch_key, q.created_at, \
                      q.enqueue_sequence, branch.last_scheduled_sequence, \
                      branch.created_at AS branch_created_at, \
                      ROW_NUMBER() OVER (\
                        PARTITION BY q.root_execution_id, q.branch_key \
                        ORDER BY q.created_at, q.enqueue_sequence, q.id\
                      ) AS branch_rank \
               FROM agent_work_queue q \
               JOIN agent_work_branch_schedule branch \
                 ON branch.root_execution_id = q.root_execution_id \
                AND branch.branch_key = q.branch_key \
               WHERE q.state = 'queued' \
                 AND (q.eligible_at IS NULL OR q.eligible_at <= ?)\
             ), root_heads AS (\
               SELECT id, root_execution_id, created_at, enqueue_sequence, \
                      ROW_NUMBER() OVER (\
                        PARTITION BY root_execution_id \
                        ORDER BY last_scheduled_sequence, \
                                 created_at, enqueue_sequence, id\
                      ) AS root_rank \
               FROM branch_heads WHERE branch_rank = 1\
             ) \
             SELECT candidate.id FROM root_heads candidate \
             JOIN agent_work_resource_scope scope \
               ON scope.root_execution_id = candidate.root_execution_id \
             WHERE candidate.root_rank = 1 AND scope.status = 'active' \
               AND (SELECT COUNT(*) FROM agent_running_permit permit \
                    WHERE permit.root_execution_id = candidate.root_execution_id \
                      AND permit.status = 'held') < scope.max_concurrency \
             ORDER BY scope.last_scheduled_sequence, \
                      candidate.created_at, candidate.enqueue_sequence, candidate.id \
             LIMIT ?"
                .to_owned(),
            [
                now.clone().into(),
                i64::try_from(limit).unwrap_or(i64::MAX).into(),
            ],
        ))
        .await
        .context("failed to list queued agent domain executions")?;
    let candidate_ids = candidate_rows
        .into_iter()
        .map(|row| {
            String::try_get(&row, "", "id")
                .map_err(|error| anyhow!("invalid queued execution ID row: {error:?}"))
        })
        .collect::<Result<Vec<_>>>()?;
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
        let Some(scope) =
            agent_work_resource_scope::Entity::find_by_id(queue.root_execution_id.clone())
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

async fn load_fair_queued_candidate<C: ConnectionTrait>(
    db: &C,
    root_execution_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_work_queue::Model>> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "WITH branch_heads AS (\
               SELECT q.id, q.branch_key, q.created_at, q.enqueue_sequence, \
                      branch.last_scheduled_sequence, branch.created_at AS branch_created_at, \
                      ROW_NUMBER() OVER (\
                        PARTITION BY q.branch_key \
                        ORDER BY q.created_at, q.enqueue_sequence, q.id\
                      ) AS branch_rank \
               FROM agent_work_queue q \
               JOIN agent_work_branch_schedule branch \
                 ON branch.root_execution_id = q.root_execution_id \
                AND branch.branch_key = q.branch_key \
               WHERE q.root_execution_id = ? AND q.state = 'queued' \
                 AND (q.eligible_at IS NULL OR q.eligible_at <= ?)\
             ) \
             SELECT id FROM branch_heads WHERE branch_rank = 1 \
             ORDER BY last_scheduled_sequence, \
                      created_at, enqueue_sequence, id LIMIT 1"
                .to_owned(),
            [root_execution_id.into(), now.into()],
        ))
        .await
        .context("failed to select fair queued branch candidate")?;
    let Some(row) = row else {
        return Ok(None);
    };
    let id = String::try_get(&row, "", "id")
        .map_err(|error| anyhow!("invalid fair queued execution ID row: {error:?}"))?;
    agent_work_queue::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to load fair queued branch candidate")
}

pub async fn insert_agent_action_idempotent<C: ConnectionTrait>(
    db: &C,
    input: &AgentActionInput,
) -> Result<agent_action::Model> {
    if let Some(existing) = agent_action::Entity::find()
        .filter(agent_action::Column::ExecutionId.eq(input.execution_id.clone()))
        .filter(agent_action::Column::IdempotencyKey.eq(input.idempotency_key.clone()))
        .one(db)
        .await
        .context("failed to load idempotent agent action")?
    {
        if existing.id != input.id
            || existing.action_kind != input.action_kind
            || existing.request_fingerprint != input.request_fingerprint
        {
            bail!("idempotency key was reused with different immutable action facts");
        }
        return Ok(existing);
    }
    agent_action::Entity::insert(agent_action::ActiveModel {
        id: Set(input.id.clone()),
        execution_id: Set(input.execution_id.clone()),
        action_kind: Set(input.action_kind.clone()),
        idempotency_key: Set(input.idempotency_key.clone()),
        request_fingerprint: Set(input.request_fingerprint.clone()),
        status: Set("prepared".to_owned()),
        created_at: Set(input.now),
        committed_at: Set(None),
        response_json: Set(None),
    })
    .on_conflict(
        OnConflict::columns([
            agent_action::Column::ExecutionId,
            agent_action::Column::IdempotencyKey,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist idempotent agent action")?;
    let action = agent_action::Entity::find()
        .filter(agent_action::Column::ExecutionId.eq(input.execution_id.clone()))
        .filter(agent_action::Column::IdempotencyKey.eq(input.idempotency_key.clone()))
        .one(db)
        .await
        .context("failed to reload agent action")?
        .context("agent action disappeared after idempotent insert")?;
    if action.id != input.id
        || action.action_kind != input.action_kind
        || action.request_fingerprint != input.request_fingerprint
    {
        bail!("idempotency key was reused with different immutable action facts");
    }
    Ok(action)
}

pub async fn load_agent_action<C: ConnectionTrait>(
    db: &C,
    action_id: &str,
) -> Result<Option<agent_action::Model>> {
    agent_action::Entity::find_by_id(action_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent action")
}

pub async fn load_agent_action_receipt<C: ConnectionTrait>(
    db: &C,
    action_id: &str,
) -> Result<Option<agent_action_receipt::Model>> {
    agent_action_receipt::Entity::find()
        .filter(agent_action_receipt::Column::ActionId.eq(action_id.to_owned()))
        .one(db)
        .await
        .context("failed to load agent action receipt")
}

pub async fn bind_agent_action_timeline_target<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: Option<&str>,
    action_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<agent_action_timeline_target::Model> {
    let action = agent_action::Entity::find_by_id(action_id.to_owned())
        .one(db)
        .await
        .context("failed to validate timeline target action")?
        .context("agent action timeline target action does not exist")?;
    if action.status != "committed" {
        bail!("agent action timeline target requires a committed action");
    }
    let (target_key, target_kind, turn_item_id) = if let Some(item_id) = item_id {
        if action.action_kind != "deliver_result" {
            bail!("only a delivery action may target a result item");
        }
        let item = turn_item::Entity::find()
            .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
            .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
            .one(db)
            .await
            .context("failed to resolve agent action timeline item target")?
            .context("agent action timeline item target does not exist")?;
        if item.item_type != "agent_message" {
            let receipt = load_agent_action_receipt(db, action_id)
                .await?
                .context("routed delivery diagnostic has no exact action receipt")?;
            if item.item_type != "system_event" || receipt.route_receipt_json.is_none() {
                bail!("agent action timeline item target is not a deliverable result item");
            }
        }
        (
            format!("turn_item:{}", item.id),
            ACTION_TIMELINE_TARGET_TURN_ITEM,
            Some(item.id),
        )
    } else {
        if !matches!(action.action_kind.as_str(), "send_message" | "start_agent") {
            bail!("only a message or Agent start action may target a Turn input");
        }
        let turn = turn::Entity::find_by_id(turn_id.to_owned())
            .one(db)
            .await
            .context("failed to validate agent action timeline Turn target")?
            .context("agent action timeline Turn target does not exist")?;
        let collaboration = super::turn::collaboration_from_model(&turn)
            .context("agent action timeline Turn target has invalid collaboration facts")?;
        let exact_author = collaboration.author.as_ref().is_some_and(|author| {
            matches!(
                &author.actor,
                PersistedActorRef::AgentExecution(execution_id)
                    if execution_id.as_str() == action.execution_id
            ) && author.agent.is_some()
        });
        if !exact_author {
            bail!("agent action timeline Turn target has no exact immutable action author");
        }
        (
            format!("turn_input:{turn_id}"),
            ACTION_TIMELINE_TARGET_TURN_INPUT,
            None,
        )
    };
    agent_action_timeline_target::Entity::insert(agent_action_timeline_target::ActiveModel {
        target_key: Set(target_key.clone()),
        action_id: Set(action_id.to_owned()),
        turn_id: Set(turn_id.to_owned()),
        turn_item_id: Set(turn_item_id.clone()),
        target_kind: Set(target_kind.to_owned()),
        created_at: Set(now),
    })
    .on_conflict(OnConflict::new().do_nothing().to_owned())
    .exec_without_returning(db)
    .await
    .context("failed to bind agent action to its exact timeline target")?;
    let row = agent_action_timeline_target::Entity::find_by_id(target_key)
        .one(db)
        .await
        .context("failed to reload agent action timeline target")?
        .context("agent action timeline target disappeared after insert")?;
    if row.action_id != action_id
        || row.turn_id != turn_id
        || row.turn_item_id != turn_item_id
        || row.target_kind != target_kind
    {
        bail!("timeline target was reused by a different immutable agent action");
    }
    let action_rows = agent_action_timeline_target::Entity::find()
        .filter(agent_action_timeline_target::Column::ActionId.eq(action_id.to_owned()))
        .limit(2)
        .all(db)
        .await
        .context("failed to validate unique agent action timeline target")?;
    if action_rows.len() != 1 || action_rows[0] != row {
        bail!("agent action was reused by a different immutable timeline target");
    }
    Ok(row)
}

/// Batch-projects exact immutable authorship and non-disclosing route
/// provenance for action-produced Turn inputs and items. Source titles and raw
/// route IDs are deliberately absent; an authorized detail surface may enrich
/// this only after a separate current source-read check.
pub async fn load_agent_action_timeline_projections_for_targets<C: ConnectionTrait>(
    db: &C,
    targets: &[(String, Option<String>)],
) -> Result<BTreeMap<(String, Option<String>), AgentActionTimelineProjection>> {
    if targets.is_empty() {
        return Ok(BTreeMap::new());
    }
    if targets.len() > AGENT_PROJECTION_BATCH_LIMIT {
        bail!("Agent timeline projection batch exceeds its bounded limit");
    }
    let requested_item_targets = targets
        .iter()
        .filter_map(|(turn_id, item_id)| {
            item_id
                .as_ref()
                .map(|item_id| (turn_id.clone(), item_id.clone()))
        })
        .collect::<Vec<_>>();
    let candidate_items = if requested_item_targets.is_empty() {
        Vec::new()
    } else {
        let exact_item_targets = requested_item_targets.iter().fold(
            Condition::any(),
            |condition, (turn_id, item_id)| {
                condition.add(
                    Condition::all()
                        .add(turn_item::Column::TurnId.eq(turn_id.clone()))
                        .add(turn_item::Column::ItemId.eq(item_id.clone())),
                )
            },
        );
        let rows = turn_item::Entity::find()
            .filter(exact_item_targets)
            .limit((AGENT_PROJECTION_BATCH_LIMIT + 1) as u64)
            .all(db)
            .await
            .context("failed to resolve exact timeline target item rows")?;
        if rows.len() > AGENT_PROJECTION_BATCH_LIMIT {
            bail!("Agent timeline item target resolution exceeds its bounded limit");
        }
        rows
    };
    let item_rows = candidate_items
        .iter()
        .map(|item| (item.id.clone(), item.turn_id.clone(), item.item_id.clone()))
        .collect::<Vec<_>>();
    let target_keys = exact_timeline_target_keys(targets, item_rows.as_slice())?;
    if target_keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let bindings = agent_action_timeline_target::Entity::find()
        .filter(agent_action_timeline_target::Column::TargetKey.is_in(target_keys))
        .all(db)
        .await
        .context("failed to load agent action timeline targets")?;
    if bindings.is_empty() {
        return Ok(BTreeMap::new());
    }
    let action_ids = bindings
        .iter()
        .map(|binding| binding.action_id.clone())
        .collect::<Vec<_>>();
    let actions = agent_action::Entity::find()
        .filter(agent_action::Column::Id.is_in(action_ids.clone()))
        .all(db)
        .await
        .context("failed to load timeline target actions")?;
    let actions_by_id = actions
        .iter()
        .map(|action| (action.id.as_str(), action))
        .collect::<BTreeMap<_, _>>();
    let receipts = agent_action_receipt::Entity::find()
        .filter(agent_action_receipt::Column::ActionId.is_in(action_ids))
        .all(db)
        .await
        .context("failed to load timeline target action receipts")?;
    let receipts_by_action = receipts
        .iter()
        .map(|receipt| (receipt.action_id.as_str(), receipt))
        .collect::<BTreeMap<_, _>>();
    let items_by_id = candidate_items
        .iter()
        .map(|item| (item.id.as_str(), item))
        .collect::<BTreeMap<_, _>>();
    let execution_ids = actions
        .iter()
        .map(|action| action.execution_id.clone())
        .collect::<Vec<_>>();
    let executions = agent_execution::Entity::find()
        .filter(agent_execution::Column::Id.is_in(execution_ids))
        .all(db)
        .await
        .context("failed to load timeline target AgentExecutions")?;
    let executions_by_id = executions
        .iter()
        .map(|execution| (execution.id.as_str(), execution))
        .collect::<BTreeMap<_, _>>();
    let identity_ids = executions
        .iter()
        .map(|execution| execution.agent_identity_id.clone())
        .collect::<Vec<_>>();
    let identities = agent_identity::Entity::find()
        .filter(agent_identity::Column::Id.is_in(identity_ids))
        .all(db)
        .await
        .context("failed to load timeline target AgentIdentities")?;
    let identities_by_id = identities
        .iter()
        .map(|identity| (identity.id.as_str(), identity))
        .collect::<BTreeMap<_, _>>();
    let snapshot_ids = executions
        .iter()
        .filter_map(|execution| execution.presentation_snapshot_id.clone())
        .collect::<Vec<_>>();
    let snapshots = agent_presentation_snapshot::Entity::find()
        .filter(agent_presentation_snapshot::Column::Id.is_in(snapshot_ids))
        .all(db)
        .await
        .context("failed to load timeline target presentation snapshots")?;
    let snapshots_by_id = snapshots
        .iter()
        .map(|snapshot| (snapshot.id.as_str(), snapshot))
        .collect::<BTreeMap<_, _>>();

    let mut result = BTreeMap::new();
    for binding in bindings {
        let action = actions_by_id
            .get(binding.action_id.as_str())
            .context("agent action timeline target has no action")?;
        if action.status != "committed" {
            bail!("agent action timeline target references an uncommitted action");
        }
        let execution = executions_by_id
            .get(action.execution_id.as_str())
            .context("timeline target action has no exact AgentExecution")?;
        let identity = identities_by_id
            .get(execution.agent_identity_id.as_str())
            .context("timeline target AgentExecution has no identity")?;
        let snapshot_id = execution
            .presentation_snapshot_id
            .as_deref()
            .context("timeline target AgentExecution has no presentation snapshot")?;
        let snapshot = snapshots_by_id
            .get(snapshot_id)
            .context("timeline target AgentExecution presentation snapshot is missing")?;
        let author = agent_presentation_snapshot_from_rows(identity, execution, snapshot)?
            .to_turn_author_snapshot();
        let item_id = match (
            binding.target_kind.as_str(),
            binding.turn_item_id.as_deref(),
        ) {
            (ACTION_TIMELINE_TARGET_TURN_INPUT, None) => None,
            (ACTION_TIMELINE_TARGET_TURN_ITEM, Some(row_id)) => {
                let item = items_by_id
                    .get(row_id)
                    .context("agent action timeline target item is missing")?;
                if item.turn_id != binding.turn_id || item.item_type != "agent_message" {
                    bail!("agent action timeline target item has inconsistent immutable facts");
                }
                Some(item.item_id.clone())
            }
            _ => bail!("agent action timeline target has an invalid shape"),
        };
        let receipt = receipts_by_action
            .get(binding.action_id.as_str())
            .context("timeline target action has no exact receipt")?;
        let route = receipt
            .route_receipt_json
            .as_deref()
            .map(|json| safe_route_provenance_from_receipt(json, action.action_kind.as_str()))
            .transpose()?;
        let key = (binding.turn_id, item_id);
        if result
            .insert(key, AgentActionTimelineProjection { author, route })
            .is_some()
        {
            bail!("multiple actions claim one immutable timeline target");
        }
    }
    Ok(result)
}

pub(super) fn safe_route_provenance_from_receipt(
    json: &str,
    expected_action_kind: &str,
) -> Result<SafeRouteProvenance> {
    let value: serde_json::Value =
        serde_json::from_str(json).context("persisted agent action route receipt is invalid")?;
    let route_id = value
        .get("routeId")
        .and_then(serde_json::Value::as_str)
        .context("persisted agent action route receipt has no route id")?;
    if route_id.trim().is_empty() {
        bail!("persisted agent action route receipt has an empty route id");
    }
    for generation in [
        "routeGeneration",
        "sourcePolicyGeneration",
        "destinationPolicyGeneration",
    ] {
        if value
            .get(generation)
            .and_then(serde_json::Value::as_u64)
            .is_none_or(|generation| generation == 0)
        {
            bail!("persisted agent action route receipt has invalid `{generation}`");
        }
    }
    let action_name = value
        .get("action")
        .and_then(serde_json::Value::as_str)
        .context("persisted agent action route receipt has no action")?;
    if action_name != expected_action_kind {
        bail!("persisted agent action route receipt differs from its action");
    }
    let action = match action_name {
        "send_message" => AgentRouteAction::SendMessage,
        "start_agent" => AgentRouteAction::StartAgent,
        "create_task" => AgentRouteAction::CreateTask,
        "schedule_task" => AgentRouteAction::ScheduleTask,
        "review_task_result" => AgentRouteAction::ReviewTaskResult,
        "deliver_result" => AgentRouteAction::DeliverResult,
        action => bail!("persisted agent action route receipt has unsupported action `{action}`"),
    };
    Ok(SafeRouteProvenance::delegated_action(action))
}

pub async fn load_agent_action_outbox<C: ConnectionTrait>(
    db: &C,
    action_id: &str,
) -> Result<Option<agent_action_outbox::Model>> {
    agent_action_outbox::Entity::find()
        .filter(agent_action_outbox::Column::ActionId.eq(action_id.to_owned()))
        .one(db)
        .await
        .context("failed to load agent action outbox")
}

fn agent_action_decision_fingerprint(input: &AgentCommitInput) -> String {
    let mut digest = Sha256::new();
    for value in [
        input.action.execution_id.as_str(),
        input.mutation_kind.as_str(),
        input.request_fingerprint.as_str(),
        input.policy_fingerprint.as_str(),
        input.execution_grant_fingerprint.as_str(),
        input.source_scope_id.as_str(),
        input.destination_scope_id.as_deref().unwrap_or(""),
        input.subject_role_key.as_str(),
        input.authorized_resource_action.as_str(),
        input.disclosure_class.as_str(),
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    for generation in [
        Some(input.expected_execution_generation),
        Some(input.execution_grant_policy_generation),
        Some(input.source_policy_generation),
        input.destination_policy_generation,
        input.route_generation,
    ] {
        digest.update(generation.unwrap_or_default().to_be_bytes());
        digest.update([generation.is_some() as u8]);
    }
    hex::encode(digest.finalize())
}

/// Commit action, receipt, domain commit and outbox in one caller-owned transaction.
pub async fn commit_agent_action(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
) -> Result<agent_domain_commit::Model> {
    if input.idempotency_key != input.action.idempotency_key
        || input.request_fingerprint != input.action.request_fingerprint
        || input.mutation_kind != input.action.action_kind
    {
        bail!("domain commit differs from its exact action intent");
    }
    if input.source_scope_id.trim().is_empty()
        || input.subject_role_key.trim().is_empty()
        || input.authorized_resource_action.trim().is_empty()
        || input.policy_fingerprint.len() != 64
        || !input
            .policy_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || input.execution_grant_fingerprint.len() != 64
        || !input
            .execution_grant_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || input.execution_grant_policy_generation < 1
        || input.source_policy_generation < 1
        || input.expected_execution_generation < 1
        || input
            .destination_policy_generation
            .is_some_and(|generation| generation < 1)
        || input
            .route_generation
            .is_some_and(|generation| generation < 1)
        || (!matches!(
            input.disclosure_class.as_str(),
            "source_capsule" | "same_capsule" | "delegated_root"
        ) && !is_routed_disclosure_class(input.disclosure_class.as_str()))
    {
        bail!("agent action receipt facts are invalid");
    }
    // An exact replay of a previously committed domain mutation is returned
    // before current admission policy is evaluated again.
    if let Some(existing) = agent_domain_commit::Entity::find()
        .filter(agent_domain_commit::Column::IdempotencyKey.eq(input.idempotency_key.clone()))
        .filter(agent_domain_commit::Column::ExecutionId.eq(input.action.execution_id.clone()))
        .one(db)
        .await
        .context("failed to load idempotent domain commit")?
    {
        validate_committed_agent_action_replay(db, input, &existing).await?;
        return Ok(existing);
    }
    let current_policy_generation =
        crate::repositories::policy_generation::current_policy_generation_on(db).await?;
    if i64::try_from(current_policy_generation.get())
        .context("current policy generation exceeds persistence bounds")?
        != input.expected_policy_generation
        || input.source_policy_generation != input.expected_policy_generation
    {
        bail!("Agent action policy changed before commit");
    }
    let actor_execution = revalidate_agent_execution_for_commit(db, input).await?;
    revalidate_agent_receipt_authority(db, input, &actor_execution).await?;
    revalidate_agent_route_for_commit(db, input, &actor_execution).await?;
    // The canonical commit boundary derives the actor from the pinned
    // execution.  Actor fields are intentionally absent from
    // `AgentCommitInput`, so no adapter/model input can substitute a
    // principal or System actor.
    let actor_kind = "agent_execution";
    let actor_id = Some(input.action.execution_id.clone());
    let action = insert_agent_action_idempotent(db, &input.action).await?;
    if action.request_fingerprint != input.request_fingerprint {
        bail!("action and domain commit fingerprints do not match");
    }
    if action.status == "committed"
        && !compacted_agent_action_value_matches(
            action.response_json.as_deref(),
            input.action_response_json.as_deref(),
            AGENT_ACTION_COMPACTION_FORMAT,
        )
    {
        bail!("committed agent action was replayed with a different response");
    }
    if !matches!(action.status.as_str(), "prepared" | "committed") {
        bail!("agent action is not in a committable state");
    }
    let now = input.action.now;
    let decision_fingerprint = agent_action_decision_fingerprint(input);
    agent_action::Entity::update_many()
        .col_expr(
            agent_action::Column::Status,
            sea_orm::sea_query::Expr::value("committed"),
        )
        .col_expr(
            agent_action::Column::CommittedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            agent_action::Column::ResponseJson,
            sea_orm::sea_query::Expr::value(input.action_response_json.clone()),
        )
        .filter(agent_action::Column::Id.eq(action.id.clone()))
        .exec(db)
        .await
        .context("failed to commit agent action")?;
    if let Some(resource) = input.resource.as_ref() {
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
    }
    agent_action_receipt::Entity::insert(agent_action_receipt::ActiveModel {
        id: Set(input.receipt_id.clone()),
        action_id: Set(action.id.clone()),
        actor_kind: Set(actor_kind.to_owned()),
        actor_id: Set(actor_id),
        decision: Set("allowed".to_owned()),
        policy_fingerprint: Set(input.policy_fingerprint.clone()),
        execution_grant_fingerprint: Set(Some(input.execution_grant_fingerprint.clone())),
        execution_grant_policy_generation: Set(Some(input.execution_grant_policy_generation)),
        source_scope_id: Set(Some(input.source_scope_id.clone())),
        destination_scope_id: Set(input.destination_scope_id.clone()),
        action_kind: Set(Some(input.mutation_kind.clone())),
        authorized_resource_action: Set(Some(input.authorized_resource_action.clone())),
        subject_role_key: Set(Some(input.subject_role_key.clone())),
        execution_generation: Set(Some(input.expected_execution_generation)),
        source_policy_generation: Set(Some(input.source_policy_generation)),
        destination_policy_generation: Set(input.destination_policy_generation),
        route_generation: Set(input.route_generation),
        disclosure_class: Set(Some(input.disclosure_class.clone())),
        decision_fingerprint: Set(Some(decision_fingerprint.clone())),
        committed_at: Set(now),
        response_json: Set(input.receipt_response_json.clone()),
        route_receipt_json: Set(input.route_receipt_json.clone()),
    })
    .on_conflict(
        OnConflict::column(agent_action_receipt::Column::ActionId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist action receipt")?;
    let receipt = load_agent_action_receipt(db, action.id.as_str())
        .await?
        .context("action receipt disappeared after idempotent insert")?;
    if receipt.id != input.receipt_id
        || receipt.actor_kind != actor_kind
        || receipt.actor_id.as_deref() != Some(input.action.execution_id.as_str())
        || receipt.decision != "allowed"
        || receipt.policy_fingerprint != input.policy_fingerprint
        || receipt.execution_grant_fingerprint.as_deref()
            != Some(input.execution_grant_fingerprint.as_str())
        || receipt.execution_grant_policy_generation
            != Some(input.execution_grant_policy_generation)
        || receipt.source_scope_id.as_deref() != Some(input.source_scope_id.as_str())
        || receipt.destination_scope_id != input.destination_scope_id
        || receipt.action_kind.as_deref() != Some(input.mutation_kind.as_str())
        || receipt.authorized_resource_action.as_deref()
            != Some(input.authorized_resource_action.as_str())
        || receipt.subject_role_key.as_deref() != Some(input.subject_role_key.as_str())
        || receipt.execution_generation != Some(input.expected_execution_generation)
        || receipt.source_policy_generation != Some(input.source_policy_generation)
        || receipt.destination_policy_generation != input.destination_policy_generation
        || receipt.route_generation != input.route_generation
        || receipt.disclosure_class.as_deref() != Some(input.disclosure_class.as_str())
        || receipt.decision_fingerprint.as_deref() != Some(decision_fingerprint.as_str())
        || !compacted_agent_action_value_matches(
            receipt.response_json.as_deref(),
            input.receipt_response_json.as_deref(),
            AGENT_ACTION_RECEIPT_COMPACTION_FORMAT,
        )
        || receipt.route_receipt_json != input.route_receipt_json
    {
        bail!("action receipt was reused with different immutable commit facts");
    }
    agent_action_outbox::Entity::insert(agent_action_outbox::ActiveModel {
        id: Set(input.outbox_id.clone()),
        action_id: Set(action.id.clone()),
        owner_execution_id: Set(input.action.execution_id.clone()),
        payload_json: Set(input.outbox_payload_json.clone()),
        status: Set("pending".to_owned()),
        attempts: Set(0),
        next_attempt_at: Set(None),
        delivered_at: Set(None),
        last_error: Set(None),
        created_at: Set(now),
    })
    .on_conflict(
        OnConflict::column(agent_action_outbox::Column::ActionId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist action outbox")?;
    let outbox = load_agent_action_outbox(db, action.id.as_str())
        .await?
        .context("action outbox disappeared after idempotent insert")?;
    if outbox.id != input.outbox_id
        || outbox.owner_execution_id != input.action.execution_id
        || !compacted_agent_action_value_matches(
            Some(outbox.payload_json.as_str()),
            Some(input.outbox_payload_json.as_str()),
            AGENT_ACTION_OUTBOX_COMPACTION_FORMAT,
        )
    {
        bail!("action outbox was reused with different immutable commit facts");
    }
    let model = agent_domain_commit::ActiveModel {
        id: Set(canonical_agent_id(
            'D',
            &format!(
                "domain-commit\0{}\0{}",
                input.action.execution_id, input.idempotency_key
            ),
        )),
        mutation_kind: Set(input.mutation_kind.clone()),
        idempotency_key: Set(input.idempotency_key.clone()),
        execution_id: Set(input.action.execution_id.clone()),
        request_fingerprint: Set(input.request_fingerprint.clone()),
        actor_identity_id: Set(input.actor_identity_id.clone()),
        receipt_id: Set(input.receipt_id.clone()),
        outbox_id: Set(input.outbox_id.clone()),
        status: Set("committed".to_owned()),
        committed_at: Set(now),
    };
    agent_domain_commit::Entity::insert(model)
        .on_conflict(
            OnConflict::columns([
                agent_domain_commit::Column::ExecutionId,
                agent_domain_commit::Column::IdempotencyKey,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context("failed to persist domain commit")?;
    let commit = agent_domain_commit::Entity::find()
        .filter(agent_domain_commit::Column::ExecutionId.eq(input.action.execution_id.clone()))
        .filter(agent_domain_commit::Column::IdempotencyKey.eq(input.idempotency_key.clone()))
        .one(db)
        .await
        .context("failed to reload idempotent domain commit")?
        .context("domain commit disappeared after idempotent insert")?;
    validate_committed_agent_action_replay(db, input, &commit).await?;
    Ok(commit)
}

async fn validate_committed_agent_action_replay<C: ConnectionTrait>(
    db: &C,
    input: &AgentCommitInput,
    commit: &agent_domain_commit::Model,
) -> Result<()> {
    if commit.mutation_kind != input.mutation_kind
        || commit.request_fingerprint != input.request_fingerprint
        || commit.actor_identity_id != input.actor_identity_id
        || commit.receipt_id != input.receipt_id
        || commit.outbox_id != input.outbox_id
        || commit.status != "committed"
    {
        bail!("domain idempotency key was reused with different immutable commit facts");
    }
    let action = agent_action::Entity::find()
        .filter(agent_action::Column::ExecutionId.eq(input.action.execution_id.clone()))
        .filter(agent_action::Column::IdempotencyKey.eq(input.action.idempotency_key.clone()))
        .one(db)
        .await
        .context("failed to reload replayed agent action")?
        .context("domain commit has no exact agent action")?;
    if action.id != input.action.id
        || action.action_kind != input.action.action_kind
        || action.request_fingerprint != input.action.request_fingerprint
        || action.status != "committed"
        || !compacted_agent_action_value_matches(
            action.response_json.as_deref(),
            input.action_response_json.as_deref(),
            AGENT_ACTION_COMPACTION_FORMAT,
        )
    {
        bail!("domain commit replay differs from its exact agent action");
    }
    let receipt = load_agent_action_receipt(db, action.id.as_str())
        .await?
        .context("domain commit has no exact action receipt")?;
    let decision_fingerprint = agent_action_decision_fingerprint(input);
    if receipt.id != input.receipt_id
        || receipt.actor_kind != "agent_execution"
        || receipt.actor_id.as_deref() != Some(input.action.execution_id.as_str())
        || receipt.decision != "allowed"
        || receipt.policy_fingerprint != input.policy_fingerprint
        || receipt.execution_grant_fingerprint.as_deref()
            != Some(input.execution_grant_fingerprint.as_str())
        || receipt.execution_grant_policy_generation
            != Some(input.execution_grant_policy_generation)
        || receipt.source_scope_id.as_deref() != Some(input.source_scope_id.as_str())
        || receipt.destination_scope_id != input.destination_scope_id
        || receipt.action_kind.as_deref() != Some(input.mutation_kind.as_str())
        || receipt.authorized_resource_action.as_deref()
            != Some(input.authorized_resource_action.as_str())
        || receipt.subject_role_key.as_deref() != Some(input.subject_role_key.as_str())
        || receipt.execution_generation != Some(input.expected_execution_generation)
        || receipt.source_policy_generation != Some(input.source_policy_generation)
        || receipt.destination_policy_generation != input.destination_policy_generation
        || receipt.route_generation != input.route_generation
        || receipt.disclosure_class.as_deref() != Some(input.disclosure_class.as_str())
        || receipt.decision_fingerprint.as_deref() != Some(decision_fingerprint.as_str())
        || !compacted_agent_action_value_matches(
            receipt.response_json.as_deref(),
            input.receipt_response_json.as_deref(),
            AGENT_ACTION_RECEIPT_COMPACTION_FORMAT,
        )
        || receipt.route_receipt_json != input.route_receipt_json
    {
        bail!("domain commit replay differs from its exact action receipt");
    }
    let outbox = load_agent_action_outbox(db, action.id.as_str())
        .await?
        .context("domain commit has no exact action outbox")?;
    if outbox.id != input.outbox_id
        || outbox.owner_execution_id != input.action.execution_id
        || !compacted_agent_action_value_matches(
            Some(outbox.payload_json.as_str()),
            Some(input.outbox_payload_json.as_str()),
            AGENT_ACTION_OUTBOX_COMPACTION_FORMAT,
        )
    {
        bail!("domain commit replay differs from its exact action outbox");
    }
    Ok(())
}

async fn revalidate_agent_execution_for_commit(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
) -> Result<agent_execution::Model> {
    let execution = agent_execution::Entity::find_by_id(input.action.execution_id.clone())
        .one(db)
        .await
        .context("failed to revalidate committing AgentExecution")?
        .context("committing AgentExecution disappeared")?;
    let terminal_status = matches!(
        execution.status.as_str(),
        "completed" | "succeeded" | "failed" | "blocked" | "cancelled" | "timed_out"
    );
    if terminal_status != execution.finished_at.is_some() {
        bail!("committing AgentExecution has an inconsistent terminal fence");
    }
    if input.actor_identity_id != execution.agent_identity_id
        || input.expected_execution_generation != execution.execution_generation
        || input.source_scope_id != execution.home_root_thread_id
    {
        bail!("committing AgentExecution is stale or has a different identity");
    }
    if terminal_status {
        revalidate_terminal_task_delivery_action(db, input, &execution).await?;
    }
    let identity = agent_identity::Entity::find_by_id(execution.agent_identity_id.clone())
        .one(db)
        .await
        .context("failed to revalidate committing Agent identity")?
        .context("committing Agent identity disappeared")?;
    if identity.workspace_id != execution.workspace_id || identity.status != "active" {
        bail!("committing Agent identity is unavailable");
    }
    if identity.source_revision != input.expected_current_identity_source_revision
        || identity.source_fingerprint != input.expected_current_identity_source_fingerprint
    {
        bail!("committing Agent identity source changed before commit");
    }
    let resource_state = load_agent_execution_resource_state(db, execution.id.as_str())
        .await?
        .context("committing AgentExecution has no current resource attempt")?;
    if input.expected_attempt_generation < 1
        || resource_state.attempt_generation != input.expected_attempt_generation
    {
        bail!("committing AgentExecution resource attempt is stale");
    }
    if terminal_status {
        if resource_state.status != "completed" {
            bail!("terminal Task delivery has no exact terminal resource attempt");
        }
        let permit_id = resource_state
            .permit_id
            .as_deref()
            .context("terminal Task delivery resource attempt has no original permit")?;
        let permit = agent_running_permit::Entity::find_by_id(permit_id.to_owned())
            .one(db)
            .await
            .context("failed to revalidate terminal Task delivery permit")?
            .context("terminal Task delivery permit disappeared")?;
        if permit.status != "released"
            || permit.execution_id != execution.id
            || permit.root_execution_id != execution.work_graph_root_execution_id
            || permit.attempt_generation != resource_state.attempt_generation
            || permit.released_at.is_none()
        {
            bail!("terminal Task delivery has no exact released execution permit");
        }
        return Ok(execution);
    }
    if resource_state.status != "running" {
        bail!("committing AgentExecution resource attempt is not running");
    }
    let permit_id = resource_state
        .permit_id
        .as_deref()
        .context("committing AgentExecution resource attempt has no permit")?;
    let permit = agent_running_permit::Entity::find_by_id(permit_id.to_owned())
        .one(db)
        .await
        .context("failed to revalidate committing AgentExecution permit")?
        .context("committing AgentExecution permit disappeared")?;
    if permit.status != "held"
        || permit.execution_id != execution.id
        || permit.root_execution_id != execution.work_graph_root_execution_id
        || permit.attempt_generation != resource_state.attempt_generation
    {
        bail!("committing AgentExecution no longer owns its exact permit");
    }
    Ok(execution)
}

/// Result delivery is the only post-terminal Agent action. The Task runtime
/// releases its attempt permit as part of the terminal run transaction, while
/// the independently retried delivery must still retain the exact occurrence
/// actor. Treating every terminal execution as live would be an authority
/// bypass, so bind this exception to the deterministic TaskDelivery/action
/// tuple and its already-terminal resource attempt.
async fn revalidate_terminal_task_delivery_action(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
    execution: &agent_execution::Model,
) -> Result<()> {
    if input.mutation_kind != "deliver_result"
        || input.action.action_kind != "deliver_result"
        || input.resource.is_some()
        || !matches!(execution.status.as_str(), "completed" | "succeeded")
    {
        bail!("terminal AgentExecution cannot commit this delivery action");
    }
    let parent_task_id = execution
        .parent_task_id
        .as_deref()
        .context("terminal delivery execution has no parent Task")?;
    let result_reference = agent_delivery_result_reference(input)?;
    let delivery_id = pioneer_protocol::task_delivery_id_from_result_item_id(&result_reference)
        .context("terminal Task delivery action has an invalid result reference")?;
    let delivery = task_delivery::Entity::find_by_id(delivery_id.to_owned())
        .one(db)
        .await
        .context("failed to revalidate terminal Task delivery")?
        .context("terminal Task delivery disappeared before action commit")?;
    let expected_action_id =
        canonical_agent_id('A', &format!("task-delivery-action\0{}", delivery.id));
    if delivery.task_id != parent_task_id
        || delivery.status != "delivering"
        || delivery.error_snapshot_json.is_some()
        || delivery.delivery_key != input.idempotency_key
        || input.action.id != expected_action_id
    {
        bail!("terminal Task delivery action differs from its durable delivery attempt");
    }
    let occurrence = task_occurrence_contract::Entity::find()
        .filter(task_occurrence_contract::Column::RunId.eq(delivery.run_id.clone()))
        .one(db)
        .await
        .context("failed to revalidate terminal Task occurrence")?
        .context("terminal Task delivery has no occurrence contract")?;
    if occurrence.task_id != parent_task_id
        || occurrence.agent_execution_id.as_deref() != Some(execution.id.as_str())
        || occurrence.status != "delivered"
    {
        bail!("terminal Task delivery differs from its exact occurrence actor");
    }
    let task_execution = task_run_execution::Entity::find_by_id(execution.id.clone())
        .one(db)
        .await
        .context("failed to revalidate terminal Task run execution")?
        .context("terminal Task delivery has no TaskRunExecution")?;
    if task_execution.task_id != parent_task_id
        || task_execution.task_run_id != delivery.run_id
        || task_execution.status != "succeeded"
    {
        bail!("terminal Task delivery differs from its exact terminal TaskRunExecution");
    }
    Ok(())
}

pub(crate) fn agent_delivery_result_reference(input: &AgentCommitInput) -> Result<String> {
    if input.mutation_kind != "deliver_result" || input.action.action_kind != "deliver_result" {
        bail!("Agent action is not a result delivery");
    }
    let payload: serde_json::Value = serde_json::from_str(input.outbox_payload_json.as_str())
        .context("Agent result delivery outbox is invalid")?;
    let normalized: pioneer_protocol::NormalizedAgentAction = serde_json::from_value(
        payload
            .get("normalized")
            .cloned()
            .context("Agent result delivery outbox has no normalized action")?,
    )
    .context("Agent result delivery normalized action is invalid")?;
    if normalized.kind != pioneer_protocol::AgentActionKind::DeliverResult
        || normalized.action_id.as_str() != input.action.id
        || normalized.execution_id.as_str() != input.action.execution_id
        || normalized.idempotency_key != input.idempotency_key
    {
        bail!("Agent result delivery outbox differs from its exact action");
    }
    normalized
        .opaque_resource_id
        .context("Agent result delivery action has no result reference")
}

async fn revalidate_agent_receipt_authority(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
    execution: &agent_execution::Model,
) -> Result<()> {
    let grants = agent_execution_grant::Entity::find()
        .filter(agent_execution_grant::Column::ExecutionId.eq(execution.id.clone()))
        .limit(2)
        .all(db)
        .await
        .context("failed to revalidate Agent action execution grant")?;
    if grants.len() != 1 {
        bail!("Agent action execution must own one exact immutable grant");
    }
    let grant: serde_json::Value = serde_json::from_str(grants[0].grant_json.as_str())
        .context("Agent action execution grant is invalid")?;
    let expected_policy_fingerprint = grant
        .get("agent_authorization_fingerprint")
        .and_then(serde_json::Value::as_str)
        .context("Agent action execution grant has no exact authorization fingerprint")?;
    if input.execution_grant_fingerprint != expected_policy_fingerprint {
        bail!("Agent action execution grant fingerprint differs from its immutable grant");
    }
    let expected_role = grant
        .get("role_key")
        .and_then(serde_json::Value::as_str)
        .context("Agent action execution grant has no exact subject role")?;
    if input.subject_role_key != expected_role {
        bail!("Agent action subject role differs from its immutable execution grant");
    }
    let grant_policy_generation = grant
        .get("agent_policy_generation")
        .and_then(serde_json::Value::as_u64)
        .and_then(|generation| i64::try_from(generation).ok())
        .context("Agent action execution grant has no exact policy generation")?;
    if input.execution_grant_policy_generation != grant_policy_generation {
        bail!("Agent action execution grant generation differs from its immutable grant");
    }
    let allowed_actions = grant
        .get("allowed_actions")
        .and_then(serde_json::Value::as_array)
        .context("Agent action execution grant has no exact action ceiling")?;
    if !allowed_actions
        .iter()
        .any(|action| action.as_str() == Some(input.authorized_resource_action.as_str()))
    {
        bail!("Agent action resource action exceeds its immutable execution grant");
    }
    match input.disclosure_class.as_str() {
        "source_capsule"
            if input.destination_scope_id.is_none()
                && input.route_receipt_json.is_none()
                && input.destination_policy_generation.is_none()
                && input.route_generation.is_none() => {}
        "same_capsule"
            if input.destination_scope_id.as_deref() == Some(input.source_scope_id.as_str())
                && input.route_receipt_json.is_none()
                && input.destination_policy_generation.is_none()
                && input.route_generation.is_none() => {}
        "delegated_root"
            if input.mutation_kind == "create_thread"
                && input
                    .destination_scope_id
                    .as_deref()
                    .is_some_and(|destination| destination != input.source_scope_id)
                && input.route_receipt_json.is_none()
                && input.destination_policy_generation.is_none()
                && input.route_generation.is_none() => {}
        disclosure_class
            if is_routed_disclosure_class(disclosure_class)
                && input.route_receipt_json.is_some() => {}
        _ => bail!("Agent action receipt scope and disclosure facts are inconsistent"),
    }
    Ok(())
}

async fn revalidate_agent_route_for_commit(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
    execution: &agent_execution::Model,
) -> Result<()> {
    let Some(receipt_json) = input.route_receipt_json.as_deref() else {
        if input.requires_cross_capsule_route {
            bail!("cross-capsule Agent action has no exact route receipt");
        }
        if input.route_generation.is_some()
            || input.destination_policy_generation.is_some()
            || is_routed_disclosure_class(input.disclosure_class.as_str())
        {
            bail!("same-capsule Agent action has forged routed receipt facts");
        }
        return Ok(());
    };
    let receipt: serde_json::Value =
        serde_json::from_str(receipt_json).context("Agent route receipt is invalid")?;
    let route_id = receipt
        .get("routeId")
        .and_then(serde_json::Value::as_str)
        .context("Agent route receipt has no route id")?;
    let expected_generation = receipt
        .get("routeGeneration")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| i64::try_from(value).ok())
        .context("Agent route receipt has an invalid generation")?;
    let expected_source_generation = receipt
        .get("sourcePolicyGeneration")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| i64::try_from(value).ok())
        .context("Agent route receipt has an invalid source policy generation")?;
    let expected_destination_generation = receipt
        .get("destinationPolicyGeneration")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| i64::try_from(value).ok())
        .context("Agent route receipt has an invalid destination policy generation")?;
    let current_policy_generation =
        crate::repositories::policy_generation::current_policy_generation_on(db).await?;
    let current_policy_generation = i64::try_from(current_policy_generation.get())
        .context("current policy generation exceeds persistence bounds")?;
    if expected_source_generation != current_policy_generation
        || expected_destination_generation != current_policy_generation
    {
        bail!("Agent route policy changed before commit");
    }
    let receipt_action = receipt
        .get("action")
        .and_then(serde_json::Value::as_str)
        .context("Agent route receipt has no action")?;
    if receipt_action != input.mutation_kind {
        bail!("Agent route receipt action differs from the committed mutation");
    }
    let route = agent_delegation_route::Entity::find_by_id(route_id.to_owned())
        .one(db)
        .await
        .context("failed to revalidate Agent route at commit")?
        .context("Agent route disappeared before commit")?;
    let now = input.action.now;
    if route.status != "active"
        || route.route_generation != expected_generation
        || route.source_policy_generation != expected_source_generation
        || route.destination_policy_generation != expected_destination_generation
        || route
            .expires_at
            .as_ref()
            .is_some_and(|expires_at| expires_at <= &now)
    {
        bail!("Agent route changed before commit");
    }
    if input.destination_scope_id.as_deref() != route.destination_capsule_id.as_deref()
        || input.destination_policy_generation != Some(expected_destination_generation)
        || input.route_generation != Some(expected_generation)
        || !is_routed_disclosure_class(input.disclosure_class.as_str())
    {
        bail!("Agent route receipt facts differ from the committed route");
    }
    if route.source_identity_id.as_deref() != Some(execution.agent_identity_id.as_str()) {
        bail!("Agent route source identity differs from the committing execution");
    }
    match route.route_kind.as_str() {
        "execution_bound" if route.source_execution_id != execution.id => {
            bail!("Agent route is bound to a different source execution")
        }
        "execution_bound" | "identity_bound" => {}
        _ => bail!("Agent route has an invalid binding kind"),
    }
    let allowed_actions: Vec<pioneer_protocol::AgentRouteAction> =
        serde_json::from_str(route.allowed_actions_json.as_str())
            .context("Agent route action subset is invalid")?;
    let required_action = match receipt_action {
        "send_message" => pioneer_protocol::AgentRouteAction::SendMessage,
        "start_agent" => pioneer_protocol::AgentRouteAction::StartAgent,
        "create_task" => pioneer_protocol::AgentRouteAction::CreateTask,
        "schedule_task" => pioneer_protocol::AgentRouteAction::ScheduleTask,
        "review_task_result" => pioneer_protocol::AgentRouteAction::ReviewTaskResult,
        "deliver_result" => pioneer_protocol::AgentRouteAction::DeliverResult,
        _ => bail!("Agent route receipt names a non-routable action"),
    };
    if !allowed_actions.contains(&required_action) {
        bail!("Agent route no longer permits the committed action");
    }
    let disclosure: AgentRouteDisclosurePolicy =
        serde_json::from_str(route.disclosure_json.as_str())
            .context("Agent route disclosure policy is invalid")?;
    if !route_disclosure_allows_commit_class(
        receipt_action,
        input.disclosure_class.as_str(),
        disclosure,
    ) {
        bail!("Agent route no longer permits the committed disclosure class");
    }
    if route.source_workspace_id.as_deref() != Some(execution.workspace_id.as_str())
        || route.source_capsule_id.as_deref() != Some(execution.home_root_thread_id.as_str())
    {
        bail!("Agent route source capsule differs from the committing execution");
    }
    let destination = thread::Entity::find_by_id(route.destination_thread_id.clone())
        .one(db)
        .await
        .context("failed to revalidate Agent route destination")?
        .context("Agent route destination disappeared before commit")?;
    if route.destination_workspace_id.as_deref() != Some(destination.workspace_id.as_str())
        || destination.workspace_id != execution.workspace_id
    {
        bail!("Agent route destination workspace changed before commit");
    }
    if route.source_capsule_id != route.destination_capsule_id {
        let destination_capsule = if destination.access_class == "internal" {
            thread_lineage::Entity::find_by_id(destination.id.clone())
                .one(db)
                .await
                .context("failed to revalidate route destination lineage")?
                .context("routed internal destination lost its lineage")?
                .root_thread_id
        } else {
            destination.id.clone()
        };
        if route.destination_capsule_id.as_deref() != Some(destination_capsule.as_str()) {
            bail!("Agent route destination capsule changed before commit");
        }
    }
    if let Some(destination_identity_id) = route.destination_agent_identity_id.as_deref() {
        let destination_identity = agent_identity::Entity::find_by_id(destination_identity_id)
            .one(db)
            .await
            .context("failed to revalidate route destination identity")?
            .context("route destination identity disappeared before commit")?;
        if destination_identity.workspace_id != execution.workspace_id
            || destination_identity.status != "active"
        {
            bail!("route destination identity is unavailable");
        }
    }
    let outbox: serde_json::Value = serde_json::from_str(input.outbox_payload_json.as_str())
        .context("routed Agent outbox payload is invalid")?;
    if outbox.get("route_id").and_then(serde_json::Value::as_str) != Some(route.id.as_str())
        || outbox
            .get("destination_thread_id")
            .and_then(serde_json::Value::as_str)
            != Some(route.destination_thread_id.as_str())
    {
        bail!("Agent route target differs from the committed outbox");
    }
    Ok(())
}

pub(super) fn is_routed_disclosure_class(value: &str) -> bool {
    matches!(
        value,
        "routed_text"
            | "routed_artifacts"
            | "routed_text_artifacts"
            | "routed_empty"
            | "routed_task_input"
            | "routed_result_summary"
            | "routed_result_full"
            | "routed_control"
    )
}

pub(super) fn route_disclosure_allows_commit_class(
    action: &str,
    class: &str,
    disclosure: AgentRouteDisclosurePolicy,
) -> bool {
    match (action, class) {
        ("send_message" | "start_agent", "routed_text") => disclosure.text,
        ("send_message" | "start_agent", "routed_artifacts") => disclosure.artifacts,
        ("send_message" | "start_agent", "routed_text_artifacts") => {
            disclosure.text && disclosure.artifacts
        }
        ("send_message" | "start_agent", "routed_empty") => true,
        ("create_task" | "schedule_task", "routed_task_input") => disclosure.user_input,
        ("deliver_result", "routed_result_summary") => matches!(
            disclosure.result_return,
            pioneer_protocol::AgentResultReturnPolicy::SummaryOnly
                | pioneer_protocol::AgentResultReturnPolicy::FullResult
        ),
        ("deliver_result", "routed_result_full") => matches!(
            disclosure.result_return,
            pioneer_protocol::AgentResultReturnPolicy::FullResult
        ),
        ("review_task_result", "routed_control") => disclosure.allows_anything(),
        _ => false,
    }
}

/// The delivery writer is intentionally typed because its Task notification
/// is committed in the same transaction. The mutation kind is checked before
/// the shared idempotent transaction runs.
async fn commit_agent_action_kind(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
    expected_kind: &'static str,
) -> Result<agent_domain_commit::Model> {
    if input.mutation_kind != expected_kind {
        bail!(
            "typed agent domain commit expected `{expected_kind}`, got `{}`",
            input.mutation_kind
        );
    }
    commit_agent_action(db, input).await
}

pub async fn commit_agent_delivery_action(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
) -> Result<agent_domain_commit::Model> {
    commit_agent_action_kind(db, input, "deliver_result").await
}

/// Persist a native identity source, its nickname claim and immutable
/// presentation in one transaction.
pub async fn commit_native_agent_config_mutation(
    db: &DatabaseTransaction,
    config: &NativeAgentConfigInput,
    identity: &AgentIdentityInput,
    presentation: &PresentationSnapshotInput,
) -> Result<(
    native_agent_config::Model,
    agent_identity::Model,
    agent_presentation_snapshot::Model,
)> {
    if config.workspace_id != identity.workspace_id
        || identity.source_kind != SOURCE_NATIVE_AGENT
        || identity.source_id != config.id
    {
        bail!("native config and identity workspace differ");
    }
    if identity.id != presentation.agent_identity_id
        || config.config_revision != identity.source_revision
        || identity.source_revision != presentation.source_revision
        || identity.source_fingerprint != presentation.source_fingerprint
        || config.display_name != presentation.display_name
        || config.nickname != presentation.nickname
        || config.avatar_revision != presentation.avatar_revision
        || config.config_revision < 1
    {
        bail!("native config presentation is bound to a different identity");
    }
    let existing_config = load_native_agent_config(db, config.id.as_str()).await?;
    let config_row = if let Some(existing) = existing_config {
        if existing.workspace_id != config.workspace_id || existing.system_key != config.system_key
        {
            bail!("native agent config source ownership is immutable");
        }
        let source_changed = existing.display_name != config.display_name
            || existing.nickname != config.nickname
            || existing.enabled != config.enabled
            || existing.avatar_revision != config.avatar_revision;
        if existing.system_key.as_deref() == Some("pioneer") && source_changed {
            bail!("reserved Pioneer native config cannot be modified");
        }
        if source_changed {
            let expected_revision = existing
                .config_revision
                .checked_add(1)
                .context("native agent config revision exhausted")?;
            if config.config_revision != expected_revision {
                bail!("native agent config changed concurrently");
            }
            let updated = native_agent_config::Entity::update_many()
                .col_expr(
                    native_agent_config::Column::DisplayName,
                    sea_orm::sea_query::Expr::value(config.display_name.clone()),
                )
                .col_expr(
                    native_agent_config::Column::Nickname,
                    sea_orm::sea_query::Expr::value(config.nickname.clone()),
                )
                .col_expr(
                    native_agent_config::Column::Enabled,
                    sea_orm::sea_query::Expr::value(config.enabled),
                )
                .col_expr(
                    native_agent_config::Column::AvatarRevision,
                    sea_orm::sea_query::Expr::value(config.avatar_revision.clone()),
                )
                .col_expr(
                    native_agent_config::Column::ConfigRevision,
                    sea_orm::sea_query::Expr::value(config.config_revision),
                )
                .col_expr(
                    native_agent_config::Column::UpdatedAt,
                    sea_orm::sea_query::Expr::value(config.now.clone()),
                )
                .filter(native_agent_config::Column::Id.eq(config.id.clone()))
                .filter(native_agent_config::Column::ConfigRevision.eq(existing.config_revision))
                .exec(db)
                .await
                .context("failed to update native agent config")?;
            if updated.rows_affected != 1 {
                bail!("native agent config changed concurrently");
            }
            load_native_agent_config(db, config.id.as_str())
                .await?
                .context("native agent config disappeared after update")?
        } else {
            if existing.config_revision != config.config_revision {
                bail!("idempotent native agent config mutation has a different revision");
            }
            existing
        }
    } else {
        if config.config_revision != 1 {
            bail!("new native agent config must start at revision one");
        }
        ensure_native_agent_config(db, config).await?
    };

    let existing_identity = load_agent_identity_by_source(
        db,
        config.workspace_id.as_str(),
        SOURCE_NATIVE_AGENT,
        config.id.as_str(),
    )
    .await?;
    let identity_row = if let Some(existing) = existing_identity {
        if existing.id != identity.id
            || existing.workspace_id != identity.workspace_id
            || existing.status != "active"
            || existing.retired_at.is_some()
        {
            bail!("native config maps to a different or retired identity");
        }
        if existing.source_revision == identity.source_revision
            && existing.source_fingerprint == identity.source_fingerprint
        {
            existing
        } else {
            if identity.source_revision
                != existing
                    .source_revision
                    .checked_add(1)
                    .context("native identity source revision exhausted")?
                || config_row.config_revision != identity.source_revision
            {
                bail!("native identity source changed concurrently");
            }
            let updated = agent_identity::Entity::update_many()
                .col_expr(
                    agent_identity::Column::SourceRevision,
                    sea_orm::sea_query::Expr::value(identity.source_revision),
                )
                .col_expr(
                    agent_identity::Column::SourceFingerprint,
                    sea_orm::sea_query::Expr::value(identity.source_fingerprint.clone()),
                )
                .col_expr(
                    agent_identity::Column::UpdatedAt,
                    sea_orm::sea_query::Expr::value(config.now.clone()),
                )
                .filter(agent_identity::Column::Id.eq(existing.id.clone()))
                .filter(agent_identity::Column::SourceRevision.eq(existing.source_revision))
                .filter(agent_identity::Column::Status.eq("active"))
                .exec(db)
                .await
                .context("failed to update native identity source revision")?;
            if updated.rows_affected != 1 {
                bail!("native identity source changed concurrently");
            }
            load_agent_identity(db, existing.id.as_str())
                .await?
                .context("native identity disappeared after update")?
        }
    } else {
        if identity.source_revision != 1 || config_row.config_revision != 1 {
            bail!("new native identity source must start at revision one");
        }
        ensure_agent_identity(db, identity).await?
    };
    claim_actor_nickname(
        db,
        config.workspace_id.as_str(),
        config.nickname.as_str(),
        NICKNAME_OWNER_AGENT,
        identity_row.id.as_str(),
        config.now.clone(),
    )
    .await?;
    let presentation_row = insert_presentation_snapshot(db, presentation).await?;
    Ok((config_row, identity_row, presentation_row))
}

/// Compact bulky response/dispatch material only after the explicit replay
/// window and only for a terminal, complete action tuple. Immutable action,
/// receipt and outbox ownership facts remain queryable; exact payload hashes
/// still fence a late direct replay without retaining provider-sized blobs.
pub async fn compact_terminal_agent_action_ledger(
    db: &DatabaseConnection,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<AgentActionLedgerCompactionSummary> {
    if limit == 0 {
        return Ok(AgentActionLedgerCompactionSummary::default());
    }
    if limit > AGENT_BACKGROUND_BATCH_LIMIT {
        bail!("Agent action compaction batch exceeds its bounded limit");
    }
    let cutoff = now - Duration::days(AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS);
    let terminal = Condition::any()
        .add(
            Condition::all()
                .add(agent_action_outbox::Column::Status.eq("delivered"))
                .add(agent_action_outbox::Column::DeliveredAt.is_not_null()),
        )
        .add(
            Condition::all()
                .add(agent_action_outbox::Column::Status.eq("failed"))
                .add(agent_action_outbox::Column::Attempts.gte(AGENT_ACTION_OUTBOX_MAX_ATTEMPTS))
                .add(agent_action_outbox::Column::NextAttemptAt.is_null()),
        );
    let candidates = agent_action_outbox::Entity::find()
        .filter(terminal)
        .filter(agent_action_outbox::Column::CreatedAt.lte(cutoff))
        .filter(agent_action_outbox::Column::PayloadJson.not_like("%\"_pioneer_compacted\"%"))
        .order_by_asc(agent_action_outbox::Column::CreatedAt)
        .order_by_asc(agent_action_outbox::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to select terminal agent domain action ledger rows")?;
    let mut summary = AgentActionLedgerCompactionSummary {
        candidate_rows: u64::try_from(candidates.len()).unwrap_or(u64::MAX),
        ..Default::default()
    };
    for candidate in candidates {
        let transaction = db
            .begin()
            .await
            .context("failed to begin agent domain action ledger compaction")?;
        let action = agent_action::Entity::find_by_id(candidate.action_id.clone())
            .one(&transaction)
            .await
            .context("failed to load compactable agent domain action")?
            .context("compactable agent domain outbox has no action")?;
        let receipt = agent_action_receipt::Entity::find()
            .filter(agent_action_receipt::Column::ActionId.eq(candidate.action_id.clone()))
            .one(&transaction)
            .await
            .context("failed to load compactable agent domain action receipt")?
            .context("compactable agent domain outbox has no receipt")?;
        if action.status != "committed"
            || action
                .committed_at
                .is_none_or(|committed_at| committed_at > cutoff)
            || receipt.committed_at > cutoff
            || receipt.decision != "allowed"
            || receipt.actor_kind != "agent_execution"
            || receipt.actor_id.as_deref() != Some(action.execution_id.as_str())
            || candidate.owner_execution_id != action.execution_id
        {
            bail!("terminal agent domain action ledger tuple is not safe to compact");
        }

        let compacted_outbox = compacted_agent_action_outbox_payload(&candidate, &action)?;
        let mut released = compacted_bytes(candidate.payload_json.as_str(), &compacted_outbox);

        if let Some(original) = action.response_json.as_deref()
            && !is_agent_action_compaction_marker(original, AGENT_ACTION_COMPACTION_FORMAT)
        {
            let compacted = agent_action_compaction_marker(
                AGENT_ACTION_COMPACTION_FORMAT,
                original,
                serde_json::Map::new(),
            );
            let updated = agent_action::Entity::update_many()
                .col_expr(
                    agent_action::Column::ResponseJson,
                    sea_orm::sea_query::Expr::value(Some(compacted.clone())),
                )
                .filter(agent_action::Column::Id.eq(action.id.clone()))
                .filter(agent_action::Column::Status.eq("committed"))
                .filter(agent_action::Column::ResponseJson.eq(Some(original.to_owned())))
                .exec(&transaction)
                .await
                .context("failed to compact agent domain action response")?;
            if updated.rows_affected != 1 {
                bail!("agent domain action response changed during compaction");
            }
            released = released.saturating_add(compacted_bytes(original, &compacted));
        }
        if let Some(original) = receipt.response_json.as_deref()
            && !is_agent_action_compaction_marker(original, AGENT_ACTION_RECEIPT_COMPACTION_FORMAT)
        {
            let compacted = agent_action_compaction_marker(
                AGENT_ACTION_RECEIPT_COMPACTION_FORMAT,
                original,
                serde_json::Map::new(),
            );
            let updated = agent_action_receipt::Entity::update_many()
                .col_expr(
                    agent_action_receipt::Column::ResponseJson,
                    sea_orm::sea_query::Expr::value(Some(compacted.clone())),
                )
                .filter(agent_action_receipt::Column::Id.eq(receipt.id.clone()))
                .filter(agent_action_receipt::Column::Decision.eq("allowed"))
                .filter(agent_action_receipt::Column::ResponseJson.eq(Some(original.to_owned())))
                .exec(&transaction)
                .await
                .context("failed to compact agent domain receipt response")?;
            if updated.rows_affected != 1 {
                bail!("agent domain receipt response changed during compaction");
            }
            released = released.saturating_add(compacted_bytes(original, &compacted));
        }
        let updated = agent_action_outbox::Entity::update_many()
            .col_expr(
                agent_action_outbox::Column::PayloadJson,
                sea_orm::sea_query::Expr::value(compacted_outbox),
            )
            .filter(agent_action_outbox::Column::Id.eq(candidate.id.clone()))
            .filter(agent_action_outbox::Column::PayloadJson.eq(candidate.payload_json.clone()))
            .filter(agent_action_outbox::Column::Status.eq(candidate.status.clone()))
            .filter(agent_action_outbox::Column::Attempts.eq(candidate.attempts))
            .exec(&transaction)
            .await
            .context("failed to compact agent domain action outbox payload")?;
        if updated.rows_affected != 1 {
            bail!("agent domain action outbox changed during compaction");
        }
        transaction
            .commit()
            .await
            .context("failed to commit agent domain action ledger compaction")?;
        summary.compacted_rows = summary.compacted_rows.saturating_add(1);
        summary.payload_bytes_released = summary.payload_bytes_released.saturating_add(released);
    }
    Ok(summary)
}

fn compacted_agent_action_outbox_payload(
    outbox: &agent_action_outbox::Model,
    action: &agent_action::Model,
) -> Result<String> {
    let original: serde_json::Value = serde_json::from_str(outbox.payload_json.as_str())
        .context("terminal agent domain action outbox payload is invalid")?;
    if original
        .get("action_id")
        .and_then(serde_json::Value::as_str)
        != Some(action.id.as_str())
        || original
            .get("execution_id")
            .and_then(serde_json::Value::as_str)
            != Some(action.execution_id.as_str())
        || original.get("kind").and_then(serde_json::Value::as_str)
            != Some(action.action_kind.as_str())
    {
        bail!("terminal agent domain action outbox payload differs from its action");
    }
    let mut retained = serde_json::Map::new();
    retained.insert(
        "action_id".to_owned(),
        serde_json::Value::String(action.id.clone()),
    );
    retained.insert(
        "execution_id".to_owned(),
        serde_json::Value::String(action.execution_id.clone()),
    );
    retained.insert(
        "kind".to_owned(),
        serde_json::Value::String(action.action_kind.clone()),
    );
    if let Some(spawned_execution_id) = original.get("spawned_execution_id") {
        retained.insert(
            "spawned_execution_id".to_owned(),
            spawned_execution_id.clone(),
        );
    }
    Ok(agent_action_compaction_marker(
        AGENT_ACTION_OUTBOX_COMPACTION_FORMAT,
        outbox.payload_json.as_str(),
        retained,
    ))
}

fn agent_action_compaction_marker(
    format: &str,
    original: &str,
    mut retained: serde_json::Map<String, serde_json::Value>,
) -> String {
    retained.insert(
        "_pioneer_compacted".to_owned(),
        serde_json::json!({
            "format": format,
            "original_bytes": original.len(),
            "original_sha256": hex::encode(Sha256::digest(original.as_bytes())),
        }),
    );
    serde_json::Value::Object(retained).to_string()
}

fn compacted_bytes(original: &str, compacted: &str) -> u64 {
    u64::try_from(original.len().saturating_sub(compacted.len())).unwrap_or(u64::MAX)
}

pub(super) fn is_agent_action_compaction_marker(value: &str, expected_format: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.get("_pioneer_compacted").cloned())
        .is_some_and(|marker| {
            marker.get("format").and_then(serde_json::Value::as_str) == Some(expected_format)
                && marker
                    .get("original_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && marker
                    .get("original_sha256")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(is_sha256_hex)
        })
}

fn compacted_agent_action_value_matches(
    persisted: Option<&str>,
    expected: Option<&str>,
    format: &str,
) -> bool {
    match (persisted, expected) {
        (None, None) => true,
        (Some(persisted), Some(expected)) if persisted == expected => true,
        (Some(persisted), Some(expected)) => {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(persisted) else {
                return false;
            };
            let Some(marker) = value.get("_pioneer_compacted") else {
                return false;
            };
            let expected_sha256 = hex::encode(Sha256::digest(expected.as_bytes()));
            marker.get("format").and_then(serde_json::Value::as_str) == Some(format)
                && marker
                    .get("original_bytes")
                    .and_then(serde_json::Value::as_u64)
                    == u64::try_from(expected.len()).ok()
                && marker
                    .get("original_sha256")
                    .and_then(serde_json::Value::as_str)
                    == Some(expected_sha256.as_str())
        }
        _ => false,
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Claim a bounded batch of post-commit action outbox rows.  The schema keeps
/// the public status set intentionally small; `next_attempt_at` is the
/// lease/fencing field that prevents two workers from processing the same row
/// concurrently without introducing a second state machine.
pub async fn claim_agent_action_outbox<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<agent_action_outbox::Model>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if limit > AGENT_BACKGROUND_BATCH_LIMIT {
        bail!("Agent action outbox batch exceeds its bounded limit");
    }
    // A worker can disappear after taking its final lease. Once that lease
    // expires there is no acknowledgement from which to derive an external
    // error, so close the row with a bounded diagnostic instead of leaving a
    // permanently pending, unclaimable outbox entry.
    agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value("failed"),
        )
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value::<Option<DateTimeWithTimeZone>>(None),
        )
        .col_expr(
            agent_action_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(
                "outbox delivery lease expired after the retry limit".to_owned(),
            )),
        )
        .filter(agent_action_outbox::Column::Status.eq("pending"))
        .filter(agent_action_outbox::Column::Attempts.gte(AGENT_ACTION_OUTBOX_MAX_ATTEMPTS))
        .filter(agent_action_outbox::Column::NextAttemptAt.lte(now.clone()))
        .exec(db)
        .await
        .context("failed to dead-letter expired agent domain outbox leases")?;

    let candidates = agent_action_outbox::Entity::find()
        .filter(agent_action_outbox::Column::Status.is_in(["pending", "failed"]))
        .filter(agent_action_outbox::Column::Attempts.lt(AGENT_ACTION_OUTBOX_MAX_ATTEMPTS))
        .filter(
            agent_action_outbox::Column::NextAttemptAt
                .is_null()
                .or(agent_action_outbox::Column::NextAttemptAt.lte(now.clone())),
        )
        .order_by_asc(agent_action_outbox::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list agent domain action outbox rows")?;
    let lease_until = now.clone() + Duration::seconds(AGENT_ACTION_OUTBOX_LEASE_SECONDS);
    let mut claimed = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let result = agent_action_outbox::Entity::update_many()
            .col_expr(
                agent_action_outbox::Column::Attempts,
                sea_orm::sea_query::Expr::cust("attempts + 1"),
            )
            .col_expr(
                agent_action_outbox::Column::Status,
                sea_orm::sea_query::Expr::value("pending"),
            )
            .col_expr(
                agent_action_outbox::Column::NextAttemptAt,
                sea_orm::sea_query::Expr::value(lease_until.clone()),
            )
            .col_expr(
                agent_action_outbox::Column::LastError,
                sea_orm::sea_query::Expr::value::<Option<String>>(None),
            )
            .filter(agent_action_outbox::Column::Id.eq(candidate.id.clone()))
            .filter(agent_action_outbox::Column::Status.is_in(["pending", "failed"]))
            .filter(agent_action_outbox::Column::Attempts.lt(AGENT_ACTION_OUTBOX_MAX_ATTEMPTS))
            .filter(
                agent_action_outbox::Column::NextAttemptAt
                    .is_null()
                    .or(agent_action_outbox::Column::NextAttemptAt.lte(now.clone())),
            )
            .exec(db)
            .await
            .context("failed to claim agent domain action outbox row")?;
        if result.rows_affected == 1
            && let Some(row) = agent_action_outbox::Entity::find_by_id(candidate.id)
                .one(db)
                .await
                .context("failed to reload claimed agent domain outbox row")?
        {
            claimed.push(row);
        }
    }
    Ok(claimed)
}

pub async fn mark_agent_action_outbox_delivered<C: ConnectionTrait>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    delivered_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value("delivered"),
        )
        .col_expr(
            agent_action_outbox::Column::DeliveredAt,
            sea_orm::sea_query::Expr::value(Some(delivered_at)),
        )
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value::<Option<DateTimeWithTimeZone>>(None),
        )
        .filter(agent_action_outbox::Column::Id.eq(outbox_id.to_owned()))
        .filter(agent_action_outbox::Column::Status.eq("pending"))
        .filter(agent_action_outbox::Column::Attempts.eq(expected_attempts))
        .exec(db)
        .await
        .context("failed to mark agent domain outbox row delivered")?;
    Ok(result.rows_affected == 1)
}

pub async fn mark_agent_action_outbox_failed<C: ConnectionTrait>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    failed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let next_attempt_at = agent_action_outbox_retry_at(expected_attempts, failed_at.clone())?;
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value("failed"),
        )
        .col_expr(
            agent_action_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(AGENT_ACTION_OUTBOX_FAILURE_CLASS.to_owned())),
        )
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value(next_attempt_at),
        )
        .filter(agent_action_outbox::Column::Id.eq(outbox_id.to_owned()))
        .filter(agent_action_outbox::Column::Status.eq("pending"))
        .filter(agent_action_outbox::Column::Attempts.eq(expected_attempts))
        .exec(db)
        .await
        .context("failed to mark agent domain outbox row failed")?;
    Ok(result.rows_affected == 1)
}

/// Release an outbox claim whose execution is still legitimately queued.
/// Waiting for root capacity is not a delivery attempt and therefore must not
/// consume the bounded failure budget. The scheduler explicitly wakes this
/// row when it grants the execution a permit; the short fallback deadline
/// also keeps recovery independent from an in-memory notification.
pub async fn defer_agent_action_outbox_for_permit<C: ConnectionTrait>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    deferred_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let restored_attempts = expected_attempts
        .checked_sub(1)
        .filter(|attempts| *attempts >= 0)
        .context("outbox permit deferral has an invalid attempt fence")?;
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value("failed"),
        )
        .col_expr(
            agent_action_outbox::Column::Attempts,
            sea_orm::sea_query::Expr::value(restored_attempts),
        )
        .col_expr(
            agent_action_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(AGENT_ACTION_OUTBOX_PERMIT_WAIT_CLASS.to_owned())),
        )
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value(Some(
                deferred_at + Duration::seconds(AGENT_ACTION_OUTBOX_LEASE_SECONDS),
            )),
        )
        .filter(agent_action_outbox::Column::Id.eq(outbox_id.to_owned()))
        .filter(agent_action_outbox::Column::Status.eq("pending"))
        .filter(agent_action_outbox::Column::Attempts.eq(expected_attempts))
        .exec(db)
        .await
        .context("failed to defer agent domain outbox for a durable permit")?;
    Ok(result.rows_affected == 1)
}

/// Make a capacity-deferred StartAgent row immediately eligible after the
/// exact execution is promoted. The failure-class fence prevents scheduler
/// activity from disturbing an unrelated delivery retry.
pub async fn wake_agent_action_outbox_for_execution<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let execution = agent_execution::Entity::find_by_id(execution_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve promoted AgentExecution")?
        .context("promoted AgentExecution is missing")?;
    if execution.parent_task_id.is_some() {
        // Task candidate/reviewer executions are resumed by the Task runtime,
        // not by a StartAgent action outbox. They may legitimately respond to
        // multiple initial/revision Turns during one occurrence.
        return Ok(0);
    }
    let responses = agent_turn_response_execution::Entity::find()
        .filter(agent_turn_response_execution::Column::ExecutionId.eq(execution_id.to_owned()))
        .limit(2)
        .all(db)
        .await
        .context("failed to resolve promoted execution response Turn")?;
    if responses.is_empty() {
        return Ok(0);
    }
    if responses.len() != 1 {
        bail!("promoted non-Task execution responds to multiple Turns");
    }
    let target = agent_action_timeline_target::Entity::find_by_id(format!(
        "turn_input:{}",
        responses[0].turn_id
    ))
    .one(db)
    .await
    .context("failed to resolve promoted execution StartAgent action")?;
    let Some(target) = target else {
        return Ok(0);
    };
    if target.target_kind != ACTION_TIMELINE_TARGET_TURN_INPUT
        || target.turn_id != responses[0].turn_id
    {
        bail!("promoted execution has an inconsistent action timeline target");
    }
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value(Some(now)),
        )
        .filter(agent_action_outbox::Column::ActionId.eq(target.action_id))
        .filter(agent_action_outbox::Column::Status.eq("failed"))
        .filter(
            agent_action_outbox::Column::LastError
                .eq(AGENT_ACTION_OUTBOX_PERMIT_WAIT_CLASS.to_owned()),
        )
        .exec(db)
        .await
        .context("failed to wake agent domain outbox after permit promotion")?;
    Ok(result.rows_affected)
}

fn agent_action_outbox_retry_at(
    attempt: i64,
    failed_at: DateTimeWithTimeZone,
) -> Result<Option<DateTimeWithTimeZone>> {
    if !(1..=AGENT_ACTION_OUTBOX_MAX_ATTEMPTS).contains(&attempt) {
        bail!("outbox failure acknowledgement has an invalid attempt fence");
    }
    if attempt == AGENT_ACTION_OUTBOX_MAX_ATTEMPTS {
        return Ok(None);
    }
    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or_default();
    let delay = std::cmp::Ord::min(
        AGENT_ACTION_OUTBOX_LEASE_SECONDS.saturating_mul(2_i64.saturating_pow(exponent)),
        AGENT_ACTION_OUTBOX_MAX_RETRY_SECONDS,
    );
    Ok(Some(failed_at + Duration::seconds(delay)))
}

pub async fn ensure_root_resource_scope<C: ConnectionTrait>(
    db: &C,
    root_execution_id: &str,
    max_concurrency: i32,
    max_queue_depth: i32,
    max_depth: i32,
    max_fan_out: i32,
    max_total_nodes: i32,
    now: DateTimeWithTimeZone,
) -> Result<agent_work_resource_scope::Model> {
    let max_concurrency = i64::from(max_concurrency);
    let max_queue_depth = i64::from(max_queue_depth);
    let max_depth = i64::from(max_depth);
    let max_fan_out = i64::from(max_fan_out);
    let max_total_nodes = i64::from(max_total_nodes);
    if !agent_work_resource_limits_are_bounded(
        max_concurrency,
        max_queue_depth,
        max_depth,
        max_fan_out,
        max_total_nodes,
    ) {
        bail!("root resource scope limits exceed their bounded policy");
    }
    agent_work_resource_scope::Entity::insert(agent_work_resource_scope::ActiveModel {
        root_execution_id: Set(root_execution_id.to_owned()),
        scope_generation: Set(1),
        max_concurrency: Set(max_concurrency),
        max_queue_depth: Set(max_queue_depth),
        max_depth: Set(max_depth),
        max_fan_out: Set(max_fan_out),
        max_total_nodes: Set(max_total_nodes),
        aggregate_usage_json: Set("{}".to_owned()),
        queue_generation: Set(0),
        last_scheduled_sequence: Set(0),
        last_scheduled_at: Set(None),
        status: Set("active".to_owned()),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::column(agent_work_resource_scope::Column::RootExecutionId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist root resource scope")?;
    let scope = agent_work_resource_scope::Entity::find_by_id(root_execution_id.to_owned())
        .one(db)
        .await
        .context("failed to reload root resource scope")?
        .context("root resource scope disappeared after idempotent insert")?;
    if scope.status == "closed" {
        bail!("root resource scope is already closed");
    }
    if !agent_work_resource_limits_are_bounded(
        scope.max_concurrency,
        scope.max_queue_depth,
        scope.max_depth,
        scope.max_fan_out,
        scope.max_total_nodes,
    ) {
        bail!("persisted root resource scope exceeds its bounded policy");
    }
    // A retry/reconnect must reuse the root's persisted policy snapshot. The
    // current default may have changed since first admission.
    let _ = (
        max_concurrency,
        max_queue_depth,
        max_depth,
        max_fan_out,
        max_total_nodes,
    );
    Ok(scope)
}

/// Atomically materialize the agent domain execution graph used by a durable
/// agent Task occurrence.  The graph rows are immutable/idempotent; only the
/// child resource state and its permit/queue placement are changed when this
/// is the first materialization.  A retry therefore cannot create a second
/// identity, execution, grant or permit.
pub async fn commit_agent_execution_graph(
    db: &DatabaseTransaction,
    input: &AgentExecutionGraphCommitInput,
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
    let identity = ensure_agent_identity(db, &input.identity).await?;
    if identity.id != input.presentation.agent_identity_id
        || identity.source_revision != input.presentation.source_revision
        || identity.source_fingerprint != input.presentation.source_fingerprint
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
    claim_actor_nickname(
        db,
        input.identity.workspace_id.as_str(),
        input.presentation.nickname.as_str(),
        nickname_owner_kind,
        nickname_owner_id.as_str(),
        input.presentation.now.clone(),
    )
    .await?;
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
        insert_agent_execution(db, root_execution).await?
    } else {
        agent_execution::Entity::find_by_id(input.root_execution_id.clone())
            .one(db)
            .await
            .context("failed to load inherited work-graph root")?
            .context("inherited work-graph root does not exist")?
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
        insert_agent_execution(db, &input.child_execution).await?
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
    if let Some(parent_execution_id) = child.parent_execution_id.as_deref() {
        let parent = agent_execution::Entity::find_by_id(parent_execution_id.to_owned())
            .one(db)
            .await
            .context("failed to load exact parent execution")?
            .context("exact parent execution does not exist")?;
        if parent.workspace_id != child.workspace_id
            || parent.work_graph_root_execution_id != input.root_execution_id
        {
            bail!("exact parent execution is outside the child work graph");
        }
        let parent_grant = load_agent_execution_grant(db, parent_execution_id)
            .await?
            .context("exact parent execution has no immutable launch grant")?;
        let (_, _, parent_launch_grant) =
            parse_agent_execution_launch_grant(parent_grant.grant_json.as_str())?;
        let ephemeral_child =
            grant_identity.source_kind == pioneer_protocol::AgentIdentitySourceKind::Ephemeral;
        let ephemeral_profile_is_monotonic = ephemeral_child
            && derived_ephemeral_profile_is_no_wider(
                &grant_profile,
                parent_launch_grant.profiles.as_slice(),
                &grant_identity.id,
            );
        let identity_ceiling_is_monotonic = child_launch_grant.identities.iter().all(|candidate| {
            parent_launch_grant
                .identities
                .iter()
                .any(|parent| parent == candidate)
                || (ephemeral_child && candidate == &grant_identity)
        });
        let profile_ceiling_is_monotonic = child_launch_grant.profiles.iter().all(|candidate| {
            parent_launch_grant
                .profiles
                .iter()
                .any(|parent| parent == candidate)
                || (ephemeral_profile_is_monotonic && candidate == &grant_profile)
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
            || !profile_ceiling_is_monotonic
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
        )
        .await?;
        return commit_task_graph_contracts(
            db,
            input,
            AgentExecutionGraphCommitResult {
                root_execution_id: root_execution_id.to_owned(),
                execution_id: execution_id.to_owned(),
                queued: false,
                queue_position: None,
            },
        )
        .await;
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
            return commit_task_graph_contracts(
                db,
                input,
                AgentExecutionGraphCommitResult {
                    root_execution_id: root_execution_id.to_owned(),
                    execution_id: execution_id.to_owned(),
                    queued: true,
                    queue_position: Some(u64::try_from(queue.enqueue_sequence).unwrap_or(u64::MAX)),
                },
            )
            .await;
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
        return commit_task_graph_contracts(
            db,
            input,
            AgentExecutionGraphCommitResult {
                root_execution_id: root_execution_id.to_owned(),
                execution_id: execution_id.to_owned(),
                queued: false,
                queue_position: None,
            },
        )
        .await;
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
        if execution_result.rows_affected != 1 {
            bail!("fair queued task agent execution changed concurrently");
        }
        return commit_task_graph_contracts(
            db,
            input,
            AgentExecutionGraphCommitResult {
                root_execution_id: root_execution_id.to_owned(),
                execution_id: execution_id.to_owned(),
                queued: true,
                queue_position: Some(u64::try_from(enqueue_sequence).unwrap_or(u64::MAX)),
            },
        )
        .await;
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
        )
        .await?;
        mark_agent_branch_scheduled(
            db,
            root_execution_id,
            input.child_resource_state.branch_key.as_str(),
            now,
        )
        .await?;
        return commit_task_graph_contracts(
            db,
            input,
            AgentExecutionGraphCommitResult {
                root_execution_id: root_execution_id.to_owned(),
                execution_id: execution_id.to_owned(),
                queued: false,
                queue_position: None,
            },
        )
        .await;
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
    if execution_result.rows_affected != 1 {
        bail!("queued task agent execution changed concurrently");
    }
    commit_task_graph_contracts(
        db,
        input,
        AgentExecutionGraphCommitResult {
            root_execution_id: root_execution_id.to_owned(),
            execution_id: execution_id.to_owned(),
            queued: true,
            queue_position: Some(u64::try_from(enqueue_sequence).unwrap_or(u64::MAX)),
        },
    )
    .await
}

fn derived_ephemeral_profile_is_no_wider(
    derived: &AgentExecutionProfileProjection,
    parent_profiles: &[AgentExecutionProfileProjection],
    ephemeral_identity_id: &AgentIdentityId,
) -> bool {
    parent_profiles.iter().any(|parent| {
        let mut compatible_agent_identity_ids = parent.compatible_agent_identity_ids.clone();
        if !compatible_agent_identity_ids.contains(ephemeral_identity_id) {
            compatible_agent_identity_ids.push(ephemeral_identity_id.clone());
            compatible_agent_identity_ids.sort();
        }
        let expected_fingerprint = hex::encode(Sha256::digest(
            format!(
                "agent-derived-profile\0{}\0{}",
                parent.fingerprint,
                ephemeral_identity_id.as_str()
            )
            .as_bytes(),
        ));
        derived.id == parent.id
            && derived.compatible_agent_identity_ids == compatible_agent_identity_ids
            && derived.backend == parent.backend
            && derived.provider_id == parent.provider_id
            && derived.model_id == parent.model_id
            && derived.provider_display_name == parent.provider_display_name
            && derived.model_display_name == parent.model_display_name
            && derived.allowed_reasoning == parent.allowed_reasoning
            && derived.allowed_permission_profiles == parent.allowed_permission_profiles
            && derived.catalog_generation == parent.catalog_generation
            && derived.policy_generation == parent.policy_generation
            && derived.fingerprint == expected_fingerprint
    })
}

async fn validate_agent_graph_bounds(
    db: &DatabaseTransaction,
    child: &agent_execution::Model,
    scope: &agent_work_resource_scope::Model,
) -> Result<()> {
    if !agent_work_resource_limits_are_bounded(
        scope.max_concurrency,
        scope.max_queue_depth,
        scope.max_depth,
        scope.max_fan_out,
        scope.max_total_nodes,
    ) {
        bail!("agent work graph has invalid persisted bounded limits");
    }
    let node_count = agent_execution::Entity::find()
        .filter(
            agent_execution::Column::WorkGraphRootExecutionId.eq(scope.root_execution_id.clone()),
        )
        .count(db)
        .await
        .context("failed to count Agent work-graph nodes")?;
    if node_count > u64::try_from(scope.max_total_nodes).unwrap_or_default() {
        bail!("agent execution graph total-node boundary exceeded");
    }
    let nodes = agent_execution::Entity::find()
        .filter(
            agent_execution::Column::WorkGraphRootExecutionId.eq(scope.root_execution_id.clone()),
        )
        .limit(
            u64::try_from(scope.max_total_nodes)
                .unwrap_or_default()
                .saturating_add(1),
        )
        .all(db)
        .await
        .context("failed to load bounded Agent work-graph lineage")?;
    if nodes.len() > usize::try_from(scope.max_total_nodes).unwrap_or_default() {
        bail!("agent execution graph total-node boundary exceeded");
    }
    let nodes_by_id = nodes
        .iter()
        .map(|execution| (execution.id.as_str(), execution))
        .collect::<std::collections::BTreeMap<_, _>>();
    if !nodes_by_id.contains_key(child.id.as_str()) {
        bail!("new AgentExecution is outside its persisted work graph");
    }
    if let Some(parent_execution_id) = child.parent_execution_id.as_deref() {
        let sibling_count = nodes
            .iter()
            .filter(|execution| {
                execution.parent_execution_id.as_deref() == Some(parent_execution_id)
            })
            .count();
        if sibling_count > usize::try_from(scope.max_fan_out).unwrap_or_default() {
            bail!("agent execution graph parent fan-out boundary exceeded");
        }
    } else if child.id != scope.root_execution_id {
        bail!("non-root AgentExecution has no exact parent");
    }

    let mut current = child;
    let mut depth = 0usize;
    let mut visited = std::collections::BTreeSet::new();
    loop {
        if !visited.insert(current.id.as_str()) {
            bail!("agent execution graph contains a lineage cycle");
        }
        if current.workspace_id != child.workspace_id
            || current.work_graph_root_execution_id != scope.root_execution_id
        {
            bail!("agent execution graph lineage crosses its root boundary");
        }
        let Some(parent_execution_id) = current.parent_execution_id.as_deref() else {
            if current.id != scope.root_execution_id {
                bail!("agent execution graph lineage does not terminate at its exact root");
            }
            break;
        };
        depth = depth.saturating_add(1);
        if depth > usize::try_from(scope.max_depth).unwrap_or_default() {
            bail!("agent execution graph depth boundary exceeded");
        }
        current = nodes_by_id
            .get(parent_execution_id)
            .copied()
            .context("agent execution graph parent is outside the bounded root lineage")?;
    }
    Ok(())
}

async fn commit_task_graph_contracts(
    db: &DatabaseTransaction,
    input: &AgentExecutionGraphCommitInput,
    result: AgentExecutionGraphCommitResult,
) -> Result<AgentExecutionGraphCommitResult> {
    if let Some(actor) = input.task_actor_contract.as_ref() {
        crate::repositories::task_actor_contract::upsert_task_actor_contract(
            db,
            actor,
            input.contract_now,
        )
        .await?;
    }
    if let Some(mut occurrence) = input.task_occurrence_contract.clone() {
        occurrence.agent_execution_id = Some(result.execution_id.clone());
        occurrence.work_graph_root_execution_id = Some(result.root_execution_id.clone());
        occurrence.root_resource_scope_id = Some(result.root_execution_id.clone());
        occurrence.status = if result.queued {
            TaskOccurrenceStatus::Queued
        } else {
            TaskOccurrenceStatus::Running
        };
        occurrence.queue_position = result.queue_position;
        crate::repositories::task_actor_contract::upsert_task_occurrence_contract(
            db,
            &occurrence,
            input.contract_now,
        )
        .await?;
    }
    Ok(result)
}

/// Close one durable agent domain execution and release only the resources
/// owned by that execution attempt.  This is deliberately idempotent: task
/// reconciliation may observe the same terminal child more than once after a
/// restart.  A sibling execution in the same root is never changed.
pub async fn finalize_agent_execution<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    terminal_status: &str,
    finished_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if !matches!(
        terminal_status,
        "completed" | "succeeded" | "failed" | "blocked" | "cancelled" | "timed_out"
    ) {
        bail!("invalid terminal agent execution status `{terminal_status}`");
    }
    let resource_terminal_status = match terminal_status {
        "completed" | "succeeded" => "completed",
        "failed" | "blocked" | "timed_out" => "failed",
        "cancelled" => "cancelled",
        _ => unreachable!("terminal status was validated above"),
    };

    let execution = agent_execution::Entity::find_by_id(execution_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent execution for finalization")?
        .with_context(|| format!("agent execution `{execution_id}` is missing"))?;

    let terminal_statuses = [
        "completed",
        "succeeded",
        "failed",
        "blocked",
        "cancelled",
        "timed_out",
    ];
    agent_execution::Entity::update_many()
        .col_expr(
            agent_execution::Column::Status,
            sea_orm::sea_query::Expr::value(terminal_status.to_owned()),
        )
        .col_expr(
            agent_execution::Column::FinishedAt,
            sea_orm::sea_query::Expr::value(Some(finished_at)),
        )
        .col_expr(
            agent_execution::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(finished_at),
        )
        .filter(agent_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(agent_execution::Column::Status.is_not_in(terminal_statuses))
        .exec(db)
        .await
        .context("failed to finalize agent execution")?;

    let states = agent_execution_resource_state::Entity::find()
        .filter(agent_execution_resource_state::Column::ExecutionId.eq(execution_id.to_owned()))
        .filter(agent_execution_resource_state::Column::Status.is_in([
            "queued",
            "running",
            "paused",
            "recovering",
        ]))
        .limit(2)
        .all(db)
        .await
        .context("failed to load agent resource states for finalization")?;
    if states.len() > 1 {
        bail!("AgentExecution has multiple active resource attempts");
    }
    for state in states {
        agent_execution_resource_state::Entity::update_many()
            .col_expr(
                agent_execution_resource_state::Column::Status,
                sea_orm::sea_query::Expr::value(resource_terminal_status),
            )
            .col_expr(
                agent_execution_resource_state::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(finished_at),
            )
            .filter(agent_execution_resource_state::Column::Id.eq(state.id.clone()))
            .exec(db)
            .await
            .context("failed to finalize agent resource state")?;

        if let Some(permit_id) = state.permit_id {
            agent_running_permit::Entity::update_many()
                .col_expr(
                    agent_running_permit::Column::Status,
                    sea_orm::sea_query::Expr::value("released"),
                )
                .col_expr(
                    agent_running_permit::Column::ReleasedAt,
                    sea_orm::sea_query::Expr::value(Some(finished_at)),
                )
                .filter(agent_running_permit::Column::Id.eq(permit_id))
                .filter(agent_running_permit::Column::Status.eq("held"))
                .exec(db)
                .await
                .context("failed to release agent execution permit")?;
        }
    }

    agent_work_queue::Entity::update_many()
        .col_expr(
            agent_work_queue::Column::State,
            sea_orm::sea_query::Expr::value("cancelled"),
        )
        .col_expr(
            agent_work_queue::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(finished_at),
        )
        .filter(agent_work_queue::Column::ExecutionId.eq(execution_id.to_owned()))
        .filter(agent_work_queue::Column::State.is_in(["queued", "recovering"]))
        .exec(db)
        .await
        .context("failed to cancel queued agent execution")?;
    agent_work_queue::Entity::update_many()
        .col_expr(
            agent_work_queue::Column::State,
            sea_orm::sea_query::Expr::value("released"),
        )
        .col_expr(
            agent_work_queue::Column::ClaimToken,
            sea_orm::sea_query::Expr::value::<Option<String>>(None),
        )
        .col_expr(
            agent_work_queue::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(finished_at),
        )
        .filter(agent_work_queue::Column::ExecutionId.eq(execution_id.to_owned()))
        .filter(agent_work_queue::Column::State.is_in(["claimed", "running"]))
        .exec(db)
        .await
        .context("failed to release claimed agent execution queue entry")?;

    let root_id = execution.work_graph_root_execution_id.clone();
    close_agent_root_scope_if_drained(db, root_id.as_str(), finished_at).await?;
    Ok(true)
}

/// Fence an explicitly cancelled root work graph in one durable transition.
/// Ordinary descendant cancellation must continue to use
/// `finalize_agent_execution`; this boundary accepts only the exact graph root
/// and never infers a root from a child.
pub async fn cancel_agent_work_graph(
    db: &DatabaseTransaction,
    root_execution_id: &str,
    terminal_reason: &str,
    finished_at: DateTimeWithTimeZone,
) -> Result<Vec<AgentWorkGraphCancellationTarget>> {
    let root = agent_execution::Entity::find_by_id(root_execution_id.to_owned())
        .one(db)
        .await
        .context("failed to load Agent work-graph root for cancellation")?
        .context("Agent work-graph root is missing")?;
    if root.id != root.work_graph_root_execution_id || root.parent_execution_id.is_some() {
        bail!("only the exact Agent work-graph root may cancel the graph");
    }
    let scope = agent_work_resource_scope::Entity::find_by_id(root_execution_id.to_owned())
        .one(db)
        .await
        .context("failed to load Agent work-graph resource scope for cancellation")?
        .context("Agent work-graph resource scope is missing")?;
    if !agent_work_resource_limits_are_bounded(
        scope.max_concurrency,
        scope.max_queue_depth,
        scope.max_depth,
        scope.max_fan_out,
        scope.max_total_nodes,
    ) {
        bail!("Agent work-graph resource scope exceeds its bounded policy");
    }
    let graph_row_limit = u64::try_from(scope.max_total_nodes)
        .unwrap_or_default()
        .saturating_add(1);
    let terminal_statuses = [
        "completed",
        "succeeded",
        "failed",
        "blocked",
        "cancelled",
        "timed_out",
    ];
    let executions = agent_execution::Entity::find()
        .filter(agent_execution::Column::WorkGraphRootExecutionId.eq(root_execution_id.to_owned()))
        .limit(graph_row_limit)
        .all(db)
        .await
        .context("failed to load Agent work graph for cancellation")?;
    if executions.len() > usize::try_from(scope.max_total_nodes).unwrap_or_default() {
        bail!("Agent work graph exceeds its bounded total-node policy");
    }
    if executions.is_empty() {
        bail!("Agent work graph has no root execution");
    }
    let active = executions
        .iter()
        .filter(|execution| !terminal_statuses.contains(&execution.status.as_str()))
        .collect::<Vec<_>>();
    let active_ids = active
        .iter()
        .map(|execution| execution.id.clone())
        .collect::<Vec<_>>();
    let response_turns = if active_ids.is_empty() {
        Vec::new()
    } else {
        let active_turn_ids = Query::select()
            .column(turn::Column::Id)
            .from(turn::Entity)
            .and_where(turn::Column::Status.eq("in_progress"))
            .to_owned();
        agent_turn_response_execution::Entity::find()
            .filter(agent_turn_response_execution::Column::ExecutionId.is_in(active_ids.clone()))
            .filter(agent_turn_response_execution::Column::TurnId.in_subquery(active_turn_ids))
            .limit(graph_row_limit)
            .all(db)
            .await
            .context("failed to load active Agent work-graph Turn bindings")?
    };
    if response_turns.len() > active_ids.len() {
        bail!("Agent work graph has too many active Turn bindings");
    }
    let mut response_turns_by_execution = BTreeMap::new();
    for response in response_turns {
        if response_turns_by_execution
            .insert(response.execution_id, response.turn_id)
            .is_some()
        {
            bail!("Agent execution has multiple active response Turns");
        }
    }
    let targets = active
        .iter()
        .map(|execution| AgentWorkGraphCancellationTarget {
            execution_id: execution.id.clone(),
            turn_id: response_turns_by_execution
                .get(execution.id.as_str())
                .cloned(),
            thread_id: execution.parent_thread_id.clone(),
            parent_task_id: execution.parent_task_id.clone(),
        })
        .collect::<Vec<_>>();

    if !active_ids.is_empty() {
        agent_execution::Entity::update_many()
            .col_expr(
                agent_execution::Column::Status,
                sea_orm::sea_query::Expr::value("cancelled"),
            )
            .col_expr(
                agent_execution::Column::FinishedAt,
                sea_orm::sea_query::Expr::value(Some(finished_at)),
            )
            .col_expr(
                agent_execution::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(finished_at),
            )
            .filter(agent_execution::Column::Id.is_in(active_ids.clone()))
            .filter(agent_execution::Column::Status.is_not_in(terminal_statuses))
            .exec(db)
            .await
            .context("failed to fence cancelled Agent work-graph executions")?;

        let resource_states = agent_execution_resource_state::Entity::find()
            .filter(agent_execution_resource_state::Column::ExecutionId.is_in(active_ids.clone()))
            .filter(agent_execution_resource_state::Column::Status.is_in([
                "queued",
                "running",
                "paused",
                "recovering",
            ]))
            .limit(graph_row_limit)
            .all(db)
            .await
            .context("failed to load Agent work-graph resource attempts")?;
        if resource_states.len() > active_ids.len() {
            bail!("Agent work graph has multiple active resource attempts");
        }
        let permit_ids = resource_states
            .iter()
            .filter_map(|state| state.permit_id.clone())
            .collect::<Vec<_>>();
        if !resource_states.is_empty() {
            agent_execution_resource_state::Entity::update_many()
                .col_expr(
                    agent_execution_resource_state::Column::Status,
                    sea_orm::sea_query::Expr::value("cancelled"),
                )
                .col_expr(
                    agent_execution_resource_state::Column::UpdatedAt,
                    sea_orm::sea_query::Expr::value(finished_at),
                )
                .filter(
                    agent_execution_resource_state::Column::Id
                        .is_in(resource_states.iter().map(|state| state.id.clone())),
                )
                .exec(db)
                .await
                .context("failed to cancel Agent work-graph resource attempts")?;
        }
        if !permit_ids.is_empty() {
            agent_running_permit::Entity::update_many()
                .col_expr(
                    agent_running_permit::Column::Status,
                    sea_orm::sea_query::Expr::value("released"),
                )
                .col_expr(
                    agent_running_permit::Column::ReleasedAt,
                    sea_orm::sea_query::Expr::value(Some(finished_at)),
                )
                .filter(agent_running_permit::Column::Id.is_in(permit_ids))
                .filter(agent_running_permit::Column::Status.eq("held"))
                .exec(db)
                .await
                .context("failed to release cancelled Agent work-graph permits")?;
        }
        agent_work_queue::Entity::update_many()
            .col_expr(
                agent_work_queue::Column::State,
                sea_orm::sea_query::Expr::value("cancelled"),
            )
            .col_expr(
                agent_work_queue::Column::ClaimToken,
                sea_orm::sea_query::Expr::value::<Option<String>>(None),
            )
            .col_expr(
                agent_work_queue::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(finished_at),
            )
            .filter(agent_work_queue::Column::ExecutionId.is_in(active_ids.clone()))
            .filter(agent_work_queue::Column::State.is_in([
                "queued",
                "recovering",
                "claimed",
                "running",
            ]))
            .exec(db)
            .await
            .context("failed to cancel Agent work-graph queue entries")?;

        task_run_execution::Entity::update_many()
            .col_expr(
                task_run_execution::Column::Status,
                sea_orm::sea_query::Expr::value("cancelled"),
            )
            .col_expr(
                task_run_execution::Column::WorkerId,
                sea_orm::sea_query::Expr::value::<Option<String>>(None),
            )
            .col_expr(
                task_run_execution::Column::LeaseUntil,
                sea_orm::sea_query::Expr::value::<Option<DateTimeWithTimeZone>>(None),
            )
            .col_expr(
                task_run_execution::Column::CompletedAt,
                sea_orm::sea_query::Expr::value(Some(finished_at)),
            )
            .col_expr(
                task_run_execution::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(finished_at),
            )
            .filter(task_run_execution::Column::Id.is_in(active_ids))
            .filter(task_run_execution::Column::Status.is_not_in([
                "succeeded",
                "failed",
                "blocked",
                "cancelled",
                "timed_out",
            ]))
            .exec(db)
            .await
            .context("failed to fence Task executions in cancelled Agent work graph")?;
    }

    task_occurrence_contract::Entity::update_many()
        .col_expr(
            task_occurrence_contract::Column::Status,
            sea_orm::sea_query::Expr::value("cancelled"),
        )
        .col_expr(
            task_occurrence_contract::Column::TerminalReason,
            sea_orm::sea_query::Expr::value(Some(terminal_reason.to_owned())),
        )
        .col_expr(
            task_occurrence_contract::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(finished_at),
        )
        .filter(
            task_occurrence_contract::Column::WorkGraphRootExecutionId
                .eq(root_execution_id.to_owned()),
        )
        .filter(task_occurrence_contract::Column::Status.is_in([
            "dormant",
            "queued",
            "recovering",
            "running",
            "waiting_review",
        ]))
        .exec(db)
        .await
        .context("failed to cancel Task occurrences in Agent work graph")?;

    agent_work_resource_scope::Entity::update_many()
        .col_expr(
            agent_work_resource_scope::Column::Status,
            sea_orm::sea_query::Expr::value("closed"),
        )
        .col_expr(
            agent_work_resource_scope::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(finished_at),
        )
        .filter(agent_work_resource_scope::Column::RootExecutionId.eq(root_execution_id.to_owned()))
        .filter(agent_work_resource_scope::Column::Status.is_in([
            "active",
            "queued",
            "recovering",
            "draining",
        ]))
        .exec(db)
        .await
        .context("failed to close cancelled Agent work-graph scope")?;
    Ok(targets)
}

/// Close only a graph whose root execution is already terminal and whose
/// execution/Task descendants are all terminal. Completing a child must never
/// implicitly complete a still-running parent root.
pub async fn close_agent_root_scope_if_drained<C: ConnectionTrait>(
    db: &C,
    root_id: &str,
    finished_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let terminal_statuses = [
        "completed",
        "succeeded",
        "failed",
        "blocked",
        "cancelled",
        "timed_out",
    ];
    let Some(root) = agent_execution::Entity::find_by_id(root_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent graph root for drain reconciliation")?
    else {
        return Ok(false);
    };
    if root.work_graph_root_execution_id != root.id
        || root.parent_execution_id.is_some()
        || !terminal_statuses.contains(&root.status.as_str())
    {
        return Ok(false);
    }
    let active_children = agent_execution::Entity::find()
        .filter(agent_execution::Column::WorkGraphRootExecutionId.eq(root_id.to_owned()))
        .filter(agent_execution::Column::Id.ne(root_id.to_owned()))
        .filter(agent_execution::Column::Status.is_not_in(terminal_statuses))
        .count(db)
        .await
        .context("failed to count active agent graph children")?;
    let active_task_occurrences = task_occurrence_contract::Entity::find()
        .filter(task_occurrence_contract::Column::WorkGraphRootExecutionId.eq(root_id.to_owned()))
        .filter(task_occurrence_contract::Column::Status.is_in([
            "dormant",
            "queued",
            "recovering",
            "running",
            "waiting_review",
        ]))
        .count(db)
        .await
        .context("failed to count active Task occurrences in agent graph")?;
    if active_children != 0 || active_task_occurrences != 0 {
        return Ok(false);
    }
    let resource_terminal_status = match root.status.as_str() {
        "completed" | "succeeded" => "completed",
        "cancelled" => "cancelled",
        "failed" | "blocked" | "timed_out" => "failed",
        _ => unreachable!("root terminal status was checked above"),
    };
    let closed = agent_work_resource_scope::Entity::update_many()
        .col_expr(
            agent_work_resource_scope::Column::Status,
            // The migration's terminal scope value is `closed`;
            // `completed` is an execution/resource-state value and violates
            // the durable scope check constraint.
            sea_orm::sea_query::Expr::value("closed"),
        )
        .col_expr(
            agent_work_resource_scope::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(finished_at),
        )
        .filter(agent_work_resource_scope::Column::RootExecutionId.eq(root_id.to_owned()))
        .filter(agent_work_resource_scope::Column::Status.is_in(["active", "queued", "recovering"]))
        .exec(db)
        .await
        .context("failed to finalize completed agent resource scope")?;
    agent_execution_resource_state::Entity::update_many()
        .col_expr(
            agent_execution_resource_state::Column::Status,
            sea_orm::sea_query::Expr::value(resource_terminal_status),
        )
        .col_expr(
            agent_execution_resource_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(finished_at),
        )
        .filter(agent_execution_resource_state::Column::ExecutionId.eq(root_id.to_owned()))
        .filter(
            agent_execution_resource_state::Column::Status.is_in(["queued", "running", "paused"]),
        )
        .exec(db)
        .await
        .context("failed to finalize completed agent root resource state")?;
    Ok(closed.rows_affected != 0)
}

/// Record liveness for exactly one execution attempt.  Heartbeats update only
/// the observation frontier; they never move progress or extend the hard
/// deadline.
pub async fn heartbeat_agent_execution<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    expected_attempt_generation: i64,
    heartbeat_at: DateTimeWithTimeZone,
    idle_deadline: Option<DateTimeWithTimeZone>,
) -> Result<bool> {
    agent_execution::Entity::find_by_id(execution_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent execution for heartbeat")?
        .with_context(|| format!("agent execution `{execution_id}` is missing"))?;
    if expected_attempt_generation < 1 {
        bail!("agent execution `{execution_id}` has an invalid attempt generation");
    }
    let mut update = agent_execution_resource_state::Entity::update_many()
        .col_expr(
            agent_execution_resource_state::Column::LastHeartbeatAt,
            sea_orm::sea_query::Expr::value(Some(heartbeat_at)),
        )
        .col_expr(
            agent_execution_resource_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(heartbeat_at),
        )
        .filter(agent_execution_resource_state::Column::ExecutionId.eq(execution_id.to_owned()))
        .filter(
            agent_execution_resource_state::Column::AttemptGeneration
                .eq(expected_attempt_generation),
        )
        .filter(agent_execution_resource_state::Column::Status.eq("running"));
    if let Some(idle_deadline) = idle_deadline {
        update = update.col_expr(
            agent_execution_resource_state::Column::IdleDeadline,
            sea_orm::sea_query::Expr::cust_with_values(
                "CASE WHEN hard_deadline IS NOT NULL AND hard_deadline <= ? THEN hard_deadline \
                 WHEN idle_deadline IS NULL OR idle_deadline < ? THEN ? ELSE idle_deadline END",
                [idle_deadline.clone(), idle_deadline.clone(), idle_deadline],
            ),
        );
    }
    let result = update
        .exec(db)
        .await
        .context("failed to heartbeat agent execution resource state")?;
    if result.rows_affected == 0 {
        return Ok(false);
    }
    agent_execution::Entity::update_many()
        .col_expr(
            agent_execution::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(heartbeat_at),
        )
        .filter(agent_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(agent_execution::Column::Status.is_in([
            "created",
            "queued",
            "running",
            "paused",
            "recovering",
        ]))
        .exec(db)
        .await
        .context("failed to touch agent execution liveness timestamp")?;
    Ok(true)
}

/// Advance the progress frontier for exactly one execution attempt.  The
/// caller supplies a bounded, non-secret summary; this is not a heartbeat and
/// therefore advances progress_sequence/last_progress_at as evidence of work.
pub async fn record_agent_execution_progress<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    expected_attempt_generation: i64,
    progress_frontier_json: &str,
    progress_at: DateTimeWithTimeZone,
    idle_deadline: Option<DateTimeWithTimeZone>,
) -> Result<bool> {
    if progress_frontier_json.len() > 16 * 1024 {
        bail!("agent execution progress frontier is too large");
    }
    agent_execution::Entity::find_by_id(execution_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent execution for progress")?
        .with_context(|| format!("agent execution `{execution_id}` is missing"))?;
    if expected_attempt_generation < 1 {
        bail!("agent execution `{execution_id}` has an invalid attempt generation");
    }
    let mut update = agent_execution_resource_state::Entity::update_many()
        .col_expr(
            agent_execution_resource_state::Column::ProgressSequence,
            sea_orm::sea_query::Expr::cust("progress_sequence + 1"),
        )
        .col_expr(
            agent_execution_resource_state::Column::ProgressFrontierJson,
            sea_orm::sea_query::Expr::value(progress_frontier_json.to_owned()),
        )
        .col_expr(
            agent_execution_resource_state::Column::LastProgressAt,
            sea_orm::sea_query::Expr::value(Some(progress_at)),
        )
        .col_expr(
            agent_execution_resource_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(progress_at),
        )
        .filter(agent_execution_resource_state::Column::ExecutionId.eq(execution_id.to_owned()))
        .filter(
            agent_execution_resource_state::Column::AttemptGeneration
                .eq(expected_attempt_generation),
        )
        .filter(agent_execution_resource_state::Column::ProgressSequence.lt(i64::MAX))
        .filter(agent_execution_resource_state::Column::Status.eq("running"));
    if let Some(idle_deadline) = idle_deadline {
        update = update.col_expr(
            agent_execution_resource_state::Column::IdleDeadline,
            sea_orm::sea_query::Expr::cust_with_values(
                "CASE WHEN hard_deadline IS NOT NULL AND hard_deadline <= ? THEN hard_deadline \
                 WHEN idle_deadline IS NULL OR idle_deadline < ? THEN ? ELSE idle_deadline END",
                [idle_deadline.clone(), idle_deadline.clone(), idle_deadline],
            ),
        );
    }
    let result = update
        .exec(db)
        .await
        .context("failed to record agent execution progress")?;
    if result.rows_affected == 0 {
        return Ok(false);
    }
    agent_execution::Entity::update_many()
        .col_expr(
            agent_execution::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(progress_at),
        )
        .filter(agent_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(agent_execution::Column::Status.is_in([
            "created",
            "queued",
            "running",
            "paused",
            "recovering",
        ]))
        .exec(db)
        .await
        .context("failed to touch agent execution progress timestamp")?;
    Ok(true)
}

async fn bind_running_state<C: ConnectionTrait>(
    db: &C,
    state_id: &str,
    execution_id: &str,
    permit_id: &str,
    now: DateTimeWithTimeZone,
    idle_timeout_secs: Option<i64>,
    hard_timeout_secs: Option<i64>,
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
            sea_orm::sea_query::Expr::cust_with_values(
                "CASE WHEN idle_deadline IS NULL THEN ? ELSE idle_deadline END",
                [Some(now.clone() + Duration::seconds(seconds))],
            ),
        );
    }
    if let Some(seconds) = hard_timeout_secs {
        state_update = state_update.col_expr(
            agent_execution_resource_state::Column::HardDeadline,
            sea_orm::sea_query::Expr::cust_with_values(
                "CASE WHEN hard_deadline IS NULL THEN ? ELSE hard_deadline END",
                [Some(now.clone() + Duration::seconds(seconds))],
            ),
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
    if execution_result.rows_affected != 1 {
        bail!(
            "agent execution `{execution_id}` was not in a bindable state while acquiring permit `{permit_id}`"
        );
    }
    Ok(())
}

pub fn canonical_agent_id(prefix: char, key: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(key.as_bytes());
    format!("{prefix}{}", &hex::encode(digest.finalize())[..20])
}

pub fn utc_now() -> DateTimeWithTimeZone {
    DateTime::<Utc>::from(Utc::now()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{Database, DatabaseBackend, TransactionTrait};

    #[test]
    fn nickname_rules_are_explicit_and_lowercase() {
        assert!(
            "agent-1".chars().all(|c| c.is_ascii_lowercase()
                || c.is_ascii_digit()
                || matches!(c, '-' | '_' | '.'))
        );
        assert!(!"Agent".chars().all(|c| c.is_ascii_lowercase()));
    }

    #[test]
    fn timeline_item_targets_use_database_row_ids_not_protocol_item_ids() {
        let targets = vec![
            ("turn-a".to_owned(), Some("protocol-item".to_owned())),
            ("turn-b".to_owned(), None),
        ];
        let item_rows = vec![(
            "database-row".to_owned(),
            "turn-a".to_owned(),
            "protocol-item".to_owned(),
        )];

        let keys = exact_timeline_target_keys(targets.as_slice(), item_rows.as_slice()).unwrap();

        assert_eq!(
            keys,
            vec![
                "turn_item:database-row".to_owned(),
                "turn_input:turn-b".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn root_turn_projects_exact_graph_queue_without_graph_wide_blocking() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        db.execute_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_keys = OFF".to_owned(),
        ))
        .await
        .unwrap();
        let now = utc_now();
        let root_id = "Egraphroot12345678901";
        let queued_id = "Egraphqueue1234567890";
        let blocked_id = "Egraphblock1234567890";
        for (id, parent_execution_id, status) in [
            (root_id, None, "running"),
            (queued_id, Some(root_id), "queued"),
            (blocked_id, Some(root_id), "blocked"),
        ] {
            agent_execution::ActiveModel {
                id: Set(id.to_owned()),
                workspace_id: Set("workspace-graph".to_owned()),
                agent_identity_id: Set("Aidentity123456789012".to_owned()),
                identity_source_revision: Set(1),
                identity_source_fingerprint: Set("source".to_owned()),
                parent_execution_id: Set(parent_execution_id.map(str::to_owned)),
                parent_task_id: Set(None),
                parent_thread_id: Set(Some("thread-graph".to_owned())),
                home_root_thread_id: Set("thread-graph".to_owned()),
                work_graph_root_execution_id: Set(root_id.to_owned()),
                requested_identity_selection_json: Set("{}".to_owned()),
                requested_profile_selection_json: Set("{}".to_owned()),
                resolved_profile_id: Set(None),
                resolved_profile_fingerprint: Set(None),
                presentation_snapshot_id: Set(None),
                authorization_context_fingerprint: Set("authorization".to_owned()),
                execution_generation: Set(1),
                status: Set(status.to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                finished_at: Set((status == "blocked").then_some(now)),
            }
            .insert(&db)
            .await
            .unwrap();
        }
        agent_work_resource_scope::ActiveModel {
            root_execution_id: Set(root_id.to_owned()),
            scope_generation: Set(1),
            max_concurrency: Set(1),
            max_queue_depth: Set(8),
            max_depth: Set(8),
            max_fan_out: Set(8),
            max_total_nodes: Set(64),
            aggregate_usage_json: Set("{}".to_owned()),
            queue_generation: Set(1),
            last_scheduled_sequence: Set(1),
            last_scheduled_at: Set(None),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&db)
        .await
        .unwrap();
        for (id, execution_id, attempt_generation, progress_sequence, status) in [
            ("state-root-old", root_id, 1, 2, "failed"),
            ("state-root", root_id, 2, 7, "running"),
            ("state-queued", queued_id, 1, 0, "queued"),
            ("state-blocked", blocked_id, 1, 3, "failed"),
        ] {
            agent_execution_resource_state::ActiveModel {
                id: Set(id.to_owned()),
                execution_id: Set(execution_id.to_owned()),
                attempt_generation: Set(attempt_generation),
                progress_sequence: Set(progress_sequence),
                progress_frontier_json: Set("{}".to_owned()),
                last_progress_at: Set(None),
                last_heartbeat_at: Set(None),
                idle_deadline: Set(None),
                hard_deadline: Set(None),
                local_usage_json: Set("{}".to_owned()),
                permit_id: Set((status == "running").then(|| "permit-root".to_owned())),
                branch_key: Set(execution_id.to_owned()),
                fair_order: Set(1),
                status: Set(status.to_owned()),
                fencing_generation: Set(1),
                fenced_at: Set(None),
                created_at: Set(now),
                updated_at: Set(now),
            }
            .insert(&db)
            .await
            .unwrap();
        }
        agent_turn_response_execution::ActiveModel {
            turn_id: Set("turn-graph".to_owned()),
            execution_id: Set(root_id.to_owned()),
            presentation_snapshot_id: Set("snapshot-graph".to_owned()),
            created_at: Set(now),
        }
        .insert(&db)
        .await
        .unwrap();

        let graph = load_agent_work_graph_projection_for_turn(&db, "turn-graph")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(graph.root_execution_id.as_str(), root_id);
        assert_eq!(graph.updated_at_unix_micros, now.timestamp_micros());
        assert_eq!(graph.running_count, 1);
        assert_eq!(graph.queued_count, 1);
        assert_eq!(graph.terminal_count, 1);
        assert!(graph.saturated);
        assert_eq!(graph.nodes.len(), 3);
        assert!(graph.nodes.iter().any(|node| {
            node.execution_id.as_str() == root_id
                && node.state == AgentWorkNodeState::Running
                && node.progress_revision == 7
        }));
        assert!(graph.nodes.iter().any(|node| {
            node.execution_id.as_str() == blocked_id && node.state == AgentWorkNodeState::Blocked
        }));
    }

    #[tokio::test]
    async fn durable_fair_queue_alternates_sibling_branches_across_transactions() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        db.execute_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_keys = OFF".to_owned(),
        ))
        .await
        .unwrap();
        let now = utc_now();
        let root_id = "Efairroot123456789012";
        agent_work_resource_scope::ActiveModel {
            root_execution_id: Set(root_id.to_owned()),
            scope_generation: Set(1),
            max_concurrency: Set(1),
            max_queue_depth: Set(8),
            max_depth: Set(8),
            max_fan_out: Set(8),
            max_total_nodes: Set(64),
            aggregate_usage_json: Set("{}".to_owned()),
            queue_generation: Set(4),
            last_scheduled_sequence: Set(0),
            last_scheduled_at: Set(None),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&db)
        .await
        .unwrap();
        for (sequence, id, execution_id, branch_key) in [
            (1, "queue-a-1", "execution-a-1", "branch-a"),
            (2, "queue-b-1", "execution-b-1", "branch-b"),
            (3, "queue-a-2", "execution-a-2", "branch-a"),
            (4, "queue-b-2", "execution-b-2", "branch-b"),
        ] {
            enqueue_agent_execution(
                &db,
                &AgentQueueEntryInput {
                    id: id.to_owned(),
                    root_execution_id: root_id.to_owned(),
                    execution_id: execution_id.to_owned(),
                    attempt_generation: 1,
                    branch_key: branch_key.to_owned(),
                    enqueue_sequence: sequence,
                    eligible_at: None,
                    now,
                },
            )
            .await
            .unwrap();
        }

        let mut selected = Vec::new();
        for _ in 0..4 {
            // A fresh transaction models the scheduler losing every in-memory
            // local between promotions. Only durable branch/global cursors
            // may determine the next sibling.
            let transaction = db.begin().await.unwrap();
            let candidate = load_fair_queued_candidate(&transaction, root_id, now)
                .await
                .unwrap()
                .unwrap();
            selected.push(candidate.id.clone());
            let claimed = agent_work_queue::Entity::update_many()
                .col_expr(
                    agent_work_queue::Column::State,
                    sea_orm::sea_query::Expr::value("claimed"),
                )
                .filter(agent_work_queue::Column::Id.eq(candidate.id))
                .filter(agent_work_queue::Column::State.eq("queued"))
                .exec(&transaction)
                .await
                .unwrap();
            assert_eq!(claimed.rows_affected, 1);
            mark_agent_branch_scheduled(&transaction, root_id, candidate.branch_key.as_str(), now)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
        }
        assert_eq!(
            selected,
            ["queue-a-1", "queue-b-1", "queue-a-2", "queue-b-2"]
        );
    }

    #[tokio::test]
    async fn root_graph_cancellation_fences_only_the_exact_graph_and_releases_resources() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        db.execute_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_keys = OFF".to_owned(),
        ))
        .await
        .unwrap();
        let now = utc_now();
        let root_id = "Ecancelroot1234567890";
        let child_id = "Ecancelchild123456789";
        let queued_id = "Ecancelqueue123456789";
        let unrelated_id = "Eunrelated12345678901";
        for (id, root, parent, status) in [
            (root_id, root_id, None, "running"),
            (child_id, root_id, Some(root_id), "running"),
            (queued_id, root_id, Some(root_id), "queued"),
            (unrelated_id, unrelated_id, None, "running"),
        ] {
            agent_execution::ActiveModel {
                id: Set(id.to_owned()),
                workspace_id: Set("workspace-cancel".to_owned()),
                agent_identity_id: Set("identity-cancel".to_owned()),
                identity_source_revision: Set(1),
                identity_source_fingerprint: Set("source".to_owned()),
                parent_execution_id: Set(parent.map(str::to_owned)),
                parent_task_id: Set(None),
                parent_thread_id: Set(Some("thread-cancel".to_owned())),
                home_root_thread_id: Set("thread-cancel".to_owned()),
                work_graph_root_execution_id: Set(root.to_owned()),
                requested_identity_selection_json: Set("{}".to_owned()),
                requested_profile_selection_json: Set("{}".to_owned()),
                resolved_profile_id: Set(None),
                resolved_profile_fingerprint: Set(None),
                presentation_snapshot_id: Set(None),
                authorization_context_fingerprint: Set("authorization".to_owned()),
                execution_generation: Set(1),
                status: Set(status.to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                finished_at: Set(None),
            }
            .insert(&db)
            .await
            .unwrap();
        }
        agent_work_resource_scope::ActiveModel {
            root_execution_id: Set(root_id.to_owned()),
            scope_generation: Set(1),
            max_concurrency: Set(2),
            max_queue_depth: Set(8),
            max_depth: Set(8),
            max_fan_out: Set(8),
            max_total_nodes: Set(64),
            aggregate_usage_json: Set("{}".to_owned()),
            queue_generation: Set(1),
            last_scheduled_sequence: Set(1),
            last_scheduled_at: Set(None),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&db)
        .await
        .unwrap();
        for (id, execution_id, permit_id, status) in [
            ("state-root", root_id, Some("permit-root"), "running"),
            ("state-child", child_id, Some("permit-child"), "running"),
            ("state-queued", queued_id, None, "queued"),
        ] {
            agent_execution_resource_state::ActiveModel {
                id: Set(id.to_owned()),
                execution_id: Set(execution_id.to_owned()),
                attempt_generation: Set(1),
                progress_sequence: Set(0),
                progress_frontier_json: Set("{}".to_owned()),
                last_progress_at: Set(None),
                last_heartbeat_at: Set(None),
                idle_deadline: Set(None),
                hard_deadline: Set(None),
                local_usage_json: Set("{}".to_owned()),
                permit_id: Set(permit_id.map(str::to_owned)),
                branch_key: Set(id.to_owned()),
                fair_order: Set(1),
                status: Set(status.to_owned()),
                fencing_generation: Set(1),
                fenced_at: Set(None),
                created_at: Set(now),
                updated_at: Set(now),
            }
            .insert(&db)
            .await
            .unwrap();
        }
        for (id, execution_id) in [("permit-root", root_id), ("permit-child", child_id)] {
            agent_running_permit::ActiveModel {
                id: Set(id.to_owned()),
                root_execution_id: Set(root_id.to_owned()),
                execution_id: Set(execution_id.to_owned()),
                attempt_generation: Set(1),
                lease_generation: Set(1),
                status: Set("held".to_owned()),
                acquired_at: Set(now),
                released_at: Set(None),
                fenced_at: Set(None),
            }
            .insert(&db)
            .await
            .unwrap();
        }
        agent_work_queue::ActiveModel {
            id: Set("queue-child".to_owned()),
            root_execution_id: Set(root_id.to_owned()),
            execution_id: Set(queued_id.to_owned()),
            attempt_generation: Set(1),
            branch_key: Set("queued".to_owned()),
            enqueue_sequence: Set(1),
            state: Set("queued".to_owned()),
            eligible_at: Set(None),
            claim_token: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&db)
        .await
        .unwrap();

        let transaction = db.begin().await.unwrap();
        let targets = cancel_agent_work_graph(&transaction, root_id, "root cancelled", now)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(targets.len(), 3);
        for id in [root_id, child_id, queued_id] {
            assert_eq!(
                agent_execution::Entity::find_by_id(id)
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "cancelled"
            );
        }
        assert_eq!(
            agent_execution::Entity::find_by_id(unrelated_id)
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .status,
            "running"
        );
        assert!(
            agent_running_permit::Entity::find()
                .all(&db)
                .await
                .unwrap()
                .iter()
                .all(|permit| permit.status == "released")
        );
        assert_eq!(
            agent_work_queue::Entity::find_by_id("queue-child")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .state,
            "cancelled"
        );
        assert_eq!(
            agent_work_resource_scope::Entity::find_by_id(root_id)
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .status,
            "closed"
        );

        let transaction = db.begin().await.unwrap();
        assert!(
            cancel_agent_work_graph(&transaction, child_id, "forged child cancellation", now)
                .await
                .is_err()
        );
        transaction.rollback().await.unwrap();
    }

    #[test]
    fn action_outbox_retry_is_bounded_and_terminal() {
        let now = utc_now();
        assert_eq!(
            agent_action_outbox_retry_at(1, now.clone()).unwrap(),
            Some(now.clone() + Duration::seconds(30))
        );
        assert_eq!(
            agent_action_outbox_retry_at(7, now.clone()).unwrap(),
            Some(now.clone() + Duration::minutes(30))
        );
        assert_eq!(
            agent_action_outbox_retry_at(AGENT_ACTION_OUTBOX_MAX_ATTEMPTS, now).unwrap(),
            None
        );
        assert!(agent_action_outbox_retry_at(0, utc_now()).is_err());
        assert!(
            agent_action_outbox_retry_at(AGENT_ACTION_OUTBOX_MAX_ATTEMPTS + 1, utc_now()).is_err()
        );
    }

    #[tokio::test]
    async fn queued_permit_wait_does_not_consume_outbox_retry_budget_and_is_woken() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        db.execute_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_keys = OFF".to_owned(),
        ))
        .await
        .unwrap();
        let now = utc_now();
        let execution_id = "Epermitwait1234567890";
        agent_execution::ActiveModel {
            id: Set(execution_id.to_owned()),
            workspace_id: Set("workspace-permit-wait".to_owned()),
            agent_identity_id: Set("identity-permit-wait".to_owned()),
            identity_source_revision: Set(1),
            identity_source_fingerprint: Set("source".to_owned()),
            parent_execution_id: Set(Some("Epermitroot1234567890".to_owned())),
            parent_task_id: Set(None),
            parent_thread_id: Set(Some("thread-permit-wait".to_owned())),
            home_root_thread_id: Set("thread-permit-wait".to_owned()),
            work_graph_root_execution_id: Set("Epermitroot1234567890".to_owned()),
            requested_identity_selection_json: Set("{}".to_owned()),
            requested_profile_selection_json: Set("{}".to_owned()),
            resolved_profile_id: Set(None),
            resolved_profile_fingerprint: Set(None),
            presentation_snapshot_id: Set(Some("snapshot-permit-wait".to_owned())),
            authorization_context_fingerprint: Set("authorization".to_owned()),
            execution_generation: Set(1),
            status: Set("running".to_owned()),
            created_at: Set(now.clone()),
            updated_at: Set(now.clone()),
            finished_at: Set(None),
        }
        .insert(&db)
        .await
        .unwrap();
        insert_compaction_test_tuple(
            &db,
            "permit-wait",
            "pending",
            0,
            now.clone(),
            None,
            "response".to_owned(),
            serde_json::json!({
                "kind": "start_agent",
                "spawned_execution_id": execution_id,
            })
            .to_string(),
        )
        .await;
        agent_turn_response_execution::Entity::insert(agent_turn_response_execution::ActiveModel {
            turn_id: Set("turn-permit-wait".to_owned()),
            execution_id: Set(execution_id.to_owned()),
            presentation_snapshot_id: Set("snapshot-permit-wait".to_owned()),
            created_at: Set(now.clone()),
        })
        .exec(&db)
        .await
        .unwrap();
        agent_action_timeline_target::Entity::insert(agent_action_timeline_target::ActiveModel {
            target_key: Set("turn_input:turn-permit-wait".to_owned()),
            action_id: Set("action-permit-wait".to_owned()),
            turn_id: Set("turn-permit-wait".to_owned()),
            turn_item_id: Set(None),
            target_kind: Set(ACTION_TIMELINE_TARGET_TURN_INPUT.to_owned()),
            created_at: Set(now.clone()),
        })
        .exec(&db)
        .await
        .unwrap();

        let claimed = claim_agent_action_outbox(&db, now.clone(), 1)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].attempts, 1);
        assert!(
            defer_agent_action_outbox_for_permit(
                &db,
                claimed[0].id.as_str(),
                claimed[0].attempts,
                now.clone(),
            )
            .await
            .unwrap()
        );
        let deferred = agent_action_outbox::Entity::find_by_id("outbox-permit-wait")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(deferred.status, "failed");
        assert_eq!(deferred.attempts, 0);
        assert_eq!(
            deferred.last_error.as_deref(),
            Some(AGENT_ACTION_OUTBOX_PERMIT_WAIT_CLASS)
        );
        assert!(
            deferred
                .next_attempt_at
                .as_ref()
                .is_some_and(|at| at > &now)
        );

        assert_eq!(
            wake_agent_action_outbox_for_execution(&db, execution_id, now.clone())
                .await
                .unwrap(),
            1
        );
        let woken = agent_action_outbox::Entity::find_by_id("outbox-permit-wait")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(woken.attempts, 0);
        assert_eq!(woken.next_attempt_at, Some(now));
    }

    #[tokio::test]
    async fn terminal_action_ledger_compaction_preserves_window_and_exact_replay_hashes() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        db.execute_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_keys = OFF".to_owned(),
        ))
        .await
        .unwrap();
        let now = utc_now();
        let old = now.clone() - Duration::days(AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS + 1);
        let recent = now.clone() - Duration::days(AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS - 1);
        let old_response = serde_json::json!({"result": "x".repeat(4096)}).to_string();
        let old_payload = serde_json::json!({
            "action_id": "action-old",
            "execution_id": "execution-old",
            "kind": "start_agent",
            "spawned_execution_id": "spawned-old",
            "dispatch": {"opaque": "y".repeat(8192)},
        })
        .to_string();
        insert_compaction_test_tuple(
            &db,
            "old",
            "delivered",
            1,
            old.clone(),
            Some(old.clone()),
            old_response.clone(),
            old_payload.clone(),
        )
        .await;
        insert_compaction_test_tuple(
            &db,
            "recent",
            "delivered",
            1,
            recent.clone(),
            Some(recent),
            "recent-response".to_owned(),
            serde_json::json!({
                "action_id": "action-recent",
                "execution_id": "execution-recent",
                "kind": "send_message",
            })
            .to_string(),
        )
        .await;
        insert_compaction_test_tuple(
            &db,
            "pending",
            "pending",
            0,
            old,
            None,
            "pending-response".to_owned(),
            serde_json::json!({
                "action_id": "action-pending",
                "execution_id": "execution-pending",
                "kind": "send_message",
            })
            .to_string(),
        )
        .await;

        let summary = compact_terminal_agent_action_ledger(&db, now, 100)
            .await
            .unwrap();
        assert_eq!(summary.candidate_rows, 1);
        assert_eq!(summary.compacted_rows, 1);
        assert!(summary.payload_bytes_released > 0);

        let action = agent_action::Entity::find_by_id("action-old")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let receipt = agent_action_receipt::Entity::find_by_id("receipt-old")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let outbox = agent_action_outbox::Entity::find_by_id("outbox-old")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(compacted_agent_action_value_matches(
            action.response_json.as_deref(),
            Some(old_response.as_str()),
            AGENT_ACTION_COMPACTION_FORMAT,
        ));
        assert!(compacted_agent_action_value_matches(
            receipt.response_json.as_deref(),
            Some(old_response.as_str()),
            AGENT_ACTION_RECEIPT_COMPACTION_FORMAT,
        ));
        assert!(compacted_agent_action_value_matches(
            Some(outbox.payload_json.as_str()),
            Some(old_payload.as_str()),
            AGENT_ACTION_OUTBOX_COMPACTION_FORMAT,
        ));
        let compacted_payload: serde_json::Value =
            serde_json::from_str(outbox.payload_json.as_str()).unwrap();
        assert_eq!(
            compacted_payload
                .get("spawned_execution_id")
                .and_then(serde_json::Value::as_str),
            Some("spawned-old")
        );
        assert_eq!(
            agent_action_outbox::Entity::find_by_id("outbox-recent")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .payload_json
                .contains("_pioneer_compacted"),
            false
        );
        assert_eq!(
            agent_action_outbox::Entity::find_by_id("outbox-pending")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .payload_json
                .contains("_pioneer_compacted"),
            false
        );
    }

    async fn insert_compaction_test_tuple(
        db: &DatabaseConnection,
        suffix: &str,
        outbox_status: &str,
        attempts: i64,
        created_at: DateTimeWithTimeZone,
        delivered_at: Option<DateTimeWithTimeZone>,
        response_json: String,
        payload_json: String,
    ) {
        let action_id = format!("action-{suffix}");
        let execution_id = format!("execution-{suffix}");
        let kind = serde_json::from_str::<serde_json::Value>(payload_json.as_str())
            .unwrap()
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_owned();
        agent_action::Entity::insert(agent_action::ActiveModel {
            id: Set(action_id.clone()),
            execution_id: Set(execution_id.clone()),
            action_kind: Set(kind.clone()),
            idempotency_key: Set(format!("idempotency-{suffix}")),
            request_fingerprint: Set("f".repeat(64)),
            status: Set("committed".to_owned()),
            created_at: Set(created_at.clone()),
            committed_at: Set(Some(created_at.clone())),
            response_json: Set(Some(response_json.clone())),
        })
        .exec(db)
        .await
        .unwrap();
        agent_action_receipt::Entity::insert(agent_action_receipt::ActiveModel {
            id: Set(format!("receipt-{suffix}")),
            action_id: Set(action_id.clone()),
            actor_kind: Set("agent_execution".to_owned()),
            actor_id: Set(Some(execution_id.clone())),
            decision: Set("allowed".to_owned()),
            policy_fingerprint: Set("a".repeat(64)),
            execution_grant_fingerprint: Set(Some("b".repeat(64))),
            execution_grant_policy_generation: Set(Some(1)),
            source_scope_id: Set(Some("source-scope".to_owned())),
            destination_scope_id: Set(None),
            action_kind: Set(Some(kind)),
            authorized_resource_action: Set(Some("send_message".to_owned())),
            subject_role_key: Set(Some("agent.collaborator".to_owned())),
            execution_generation: Set(Some(1)),
            source_policy_generation: Set(Some(1)),
            destination_policy_generation: Set(None),
            route_generation: Set(None),
            disclosure_class: Set(Some("source_capsule".to_owned())),
            decision_fingerprint: Set(Some("c".repeat(64))),
            committed_at: Set(created_at.clone()),
            response_json: Set(Some(response_json)),
            route_receipt_json: Set(None),
        })
        .exec(db)
        .await
        .unwrap();
        agent_action_outbox::Entity::insert(agent_action_outbox::ActiveModel {
            id: Set(format!("outbox-{suffix}")),
            action_id: Set(action_id),
            owner_execution_id: Set(execution_id),
            payload_json: Set(payload_json),
            status: Set(outbox_status.to_owned()),
            attempts: Set(attempts),
            next_attempt_at: Set(None),
            delivered_at: Set(delivered_at),
            last_error: Set(None),
            created_at: Set(created_at),
        })
        .exec(db)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn scheduler_cursor_is_monotonic_even_when_time_does_not_advance() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        let now = utc_now();
        let transaction = db.begin().await.unwrap();
        let first = next_agent_schedule_sequence(&transaction, now.clone())
            .await
            .unwrap();
        let second = next_agent_schedule_sequence(&transaction, now)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(first, 1);
        assert_eq!(second, 2);

        let persisted = agent_work_scheduler_state::Entity::find_by_id("global")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.schedule_generation, 2);
    }

    #[test]
    fn route_receipt_projects_only_safe_action_provenance() {
        let route = safe_route_provenance_from_receipt(
            r#"{"routeId":"D00000000000000000001","routeGeneration":4,"sourcePolicyGeneration":7,"destinationPolicyGeneration":9,"action":"send_message"}"#,
            "send_message",
        )
        .expect("valid exact route receipt");
        assert_eq!(
            route,
            pioneer_protocol::SafeRouteProvenance::delegated_action(
                pioneer_protocol::AgentRouteAction::SendMessage,
            )
        );
        let encoded = serde_json::to_string(&route).expect("safe projection");
        assert!(!encoded.contains("D00000000000000000001"));
        assert!(!encoded.contains("generation"));
    }

    #[test]
    fn route_receipt_rejects_action_rebinding_or_invalid_generations() {
        let rebound = safe_route_provenance_from_receipt(
            r#"{"routeId":"D00000000000000000001","routeGeneration":4,"sourcePolicyGeneration":7,"destinationPolicyGeneration":9,"action":"start_agent"}"#,
            "send_message",
        );
        assert!(rebound.is_err());
        let invalid_generation = safe_route_provenance_from_receipt(
            r#"{"routeId":"D00000000000000000001","routeGeneration":0,"sourcePolicyGeneration":7,"destinationPolicyGeneration":9,"action":"send_message"}"#,
            "send_message",
        );
        assert!(invalid_generation.is_err());
    }

    #[test]
    fn derived_ephemeral_profile_cannot_widen_parent_runtime_or_reasoning() {
        let parent_identity = AgentIdentityId::new("A12345678901234567890").unwrap();
        let ephemeral_identity = AgentIdentityId::new("A02345678901234567890").unwrap();
        let parent = AgentExecutionProfileProjection {
            id: AgentExecutionProfileId::new("P12345678901234567890").unwrap(),
            compatible_agent_identity_ids: vec![parent_identity],
            backend: pioneer_protocol::AgentExecutionProfileBackend::ApiProvider,
            provider_id: "provider".to_owned(),
            model_id: "model".to_owned(),
            provider_display_name: "Provider".to_owned(),
            model_display_name: "Model".to_owned(),
            allowed_reasoning: vec![pioneer_protocol::TurnReasoningSelection {
                effort: "medium".to_owned(),
            }],
            allowed_permission_profiles: vec![pioneer_protocol::TurnPermissionMode::Supervised],
            catalog_generation: 4,
            policy_generation: 7,
            fingerprint: "parent-profile-fingerprint".to_owned(),
        };
        let mut derived = parent.clone();
        derived
            .compatible_agent_identity_ids
            .push(ephemeral_identity.clone());
        derived.compatible_agent_identity_ids.sort();
        derived.fingerprint = hex::encode(Sha256::digest(
            format!(
                "agent-derived-profile\0{}\0{}",
                parent.fingerprint,
                ephemeral_identity.as_str()
            )
            .as_bytes(),
        ));
        assert!(derived_ephemeral_profile_is_no_wider(
            &derived,
            std::slice::from_ref(&parent),
            &ephemeral_identity,
        ));

        let mut widened = derived;
        widened
            .allowed_reasoning
            .push(pioneer_protocol::TurnReasoningSelection {
                effort: "high".to_owned(),
            });
        assert!(!derived_ephemeral_profile_is_no_wider(
            &widened,
            std::slice::from_ref(&parent),
            &ephemeral_identity,
        ));
    }
}
