//! Persistence primitives for agent domain.
//!
//! These functions deliberately keep identity, lineage, action receipts and
//! resource state in separate records.  Callers that need an all-or-nothing
//! mutation pass a `DatabaseTransaction`; no function infers an actor from a
//! provider, model, display label or the last message.

use crate::GatewayControlConnection;
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration, Utc};
pub use pioneer_crud_access::actor_nickname::{
    NICKNAME_ACTIVE, NICKNAME_OWNER_AGENT, NICKNAME_OWNER_PRINCIPAL, NICKNAME_OWNER_RESERVED,
    NICKNAME_TOMBSTONED, claim_actor_nickname,
};
use pioneer_crud_execution::agent_identity::agent_identity_source_kind;
pub use pioneer_crud_execution::agent_identity::{
    AgentExecutionAuthorProjection, AgentIdentityInput, AgentTurnResponseInput,
    NativeAgentConfigInput, PresentationSnapshotInput, SOURCE_CLI_RUNTIME_INSTANCE,
    SOURCE_EPHEMERAL, SOURCE_NATIVE_AGENT, agent_presentation_snapshot_from_rows,
    ensure_agent_identity, ensure_native_agent_config, insert_agent_turn_response,
    insert_presentation_snapshot, list_active_agent_identities_page, load_active_agent_identity,
    load_active_agent_identity_by_source, load_agent_identity, load_agent_identity_by_source,
    load_agent_presentation_snapshot, load_agent_turn_responses_for_turns,
    load_current_agent_authors_for_executions, load_current_agent_presentation_snapshot,
    load_native_agent_config, load_native_agent_config_by_system_key,
    revalidate_agent_presentation_for_execution,
};
pub use pioneer_crud_execution::execution_reads::load_agent_turn_response;

pub use pioneer_crud_execution::agent_queue::next_agent_schedule_sequence;
use pioneer_crud_execution::agent_queue::{
    ensure_agent_branch_schedule, load_fair_queued_candidate, mark_agent_branch_scheduled,
};
pub use pioneer_crud_execution::outbox_retention::{
    AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS, next_agent_ledger_retention_at,
};

use pioneer_crud_execution::outbox_claim::claim_agent_action_outbox_with_clock;
pub use pioneer_crud_execution::outbox_claim::{
    AGENT_ACTION_OUTBOX_MAX_ATTEMPTS, AGENT_START_RECONCILIATION_REQUIRED,
    AgentActionOutboxClaimBatch, AgentActionOutboxFailure,
};
use pioneer_entity::{
    agent_action, agent_action_outbox, agent_domain_commit, agent_execution, agent_execution_grant,
    agent_execution_resource_state, agent_identity, agent_presentation_snapshot,
    agent_running_permit, agent_turn_response_execution, agent_work_branch_schedule,
    agent_work_queue, agent_work_resource_scope, native_agent_config, task_delivery,
    task_occurrence_contract, task_run_execution, thread, thread_lineage, turn,
    turn_cli_runtime_binding,
};
use pioneer_protocol::{
    AgentExecutionId, AgentExecutionProfileId, AgentIdentityId, AgentIdentitySourceKind,
    PersistedActorRef, TaskActorContract, TaskOccurrenceContract, TaskOccurrenceStatus, Thread,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, Func, OnConflict, Query};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait,
    ExprTrait, FromQueryResult, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    TransactionSession, TransactionTrait, TryGetable,
};
use std::collections::BTreeMap;

// Independent of graph nodes: one Task occurrence may have many revision Turns.
const AGENT_GRAPH_RESPONSE_QUERY_BATCH: usize = 128;

use pioneer_crud_execution::agent_graph_projection::agent_work_resource_limits_are_bounded;

pub use pioneer_crud_execution::agent_action_compaction::AgentActionLedgerCompactionSummary;

pub use pioneer_crud_execution::agent_execution::{
    AgentExecutionInput, AgentResourceStateInput, acquire_agent_running_permit,
    insert_agent_resource_state, load_agent_execution,
};

pub use pioneer_crud_execution::agent_delegation_route::{
    AgentDelegationRouteInput, agent_delegation_route_projection,
    apply_prepared_agent_delegation_route_expiry_batch, insert_agent_delegation_route,
    list_agent_delegation_routes, list_agent_delegation_routes_for_source,
    load_agent_delegation_route, prepare_agent_delegation_route_expiry_batch,
    revoke_agent_delegation_route,
};

pub use pioneer_crud_execution::agent_actions::{
    AgentActionInput, insert_agent_action_idempotent, load_agent_action, load_agent_action_receipt,
};

#[derive(Debug, Clone)]
pub struct AgentThreadCreationCommitInput<'a> {
    pub thread: &'a Thread,
    pub execution_id: AgentExecutionId,
    pub route: Option<AgentDelegationRouteInput>,
    pub lineage: Option<pioneer_protocol::TaskThreadLineage>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

pub use pioneer_crud_execution::agent_graph_admission::{
    AgentExecutionGraphCommitInput, AgentExecutionGraphCommitResult,
};

pub use pioneer_crud_execution::agent_queue::AgentQueueEntryInput;

pub use pioneer_crud_execution::agent_graph_admission::PromotedAgentExecution;

pub use pioneer_crud_execution::agent_graph_projection::AgentWorkGraphProjectionTarget;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentWorkGraphCancellationTarget {
    pub execution_id: String,
    pub turn_id: Option<String>,
    pub thread_id: Option<String>,
    pub parent_task_id: Option<String>,
    /// Admission uncertainty, never a process completion acknowledgement.
    pub turn_pending: bool,
    pub has_cli_binding: bool,
}

#[derive(sea_orm::FromQueryResult)]
struct AgentGraphResponseBinding {
    execution_id: String,
    turn_id: String,
    bound_thread_id: Option<String>,
    bound_status: Option<String>,
    has_cli_binding: bool,
}

pub async fn insert_agent_execution<C: ConnectionTrait>(
    db: &C,
    input: &AgentExecutionInput,
) -> Result<agent_execution::Model> {
    let (execution, inserted) =
        pioneer_crud_execution::agent_execution::insert_agent_execution(db, input).await?;
    if inserted {
        super::task_occurrence_producers::agent_change(db, None, Some(&execution)).await?;
    }
    Ok(execution)
}

pub use pioneer_crud_execution::execution_reads::load_agent_execution_resource_state;

pub use pioneer_crud_execution::agent_graph_projection::{
    load_agent_work_graph_projection_for_turn, load_agent_work_graph_projection_target,
};

/// An empty agent-created thread has no Conversation turn from which to
/// restore composer reasoning. Its creator execution stores the immutable
/// applied selection; reading it neither creates identity nor grants authority.
pub(crate) async fn empty_agent_thread_reasoning<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<String>> {
    let Some(thread) = thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let Some(PersistedActorRef::AgentExecution(execution_id)) =
        crate::repositories::identity::actor_ref_from_db(
            thread.created_by_actor_kind.as_deref(),
            thread.created_by_actor_id.as_deref(),
        )?
    else {
        return Ok(None);
    };
    let Some(execution) = agent_execution::Entity::find_by_id(execution_id.to_string())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if execution.workspace_id != thread.workspace_id {
        bail!("thread creator execution workspace differs");
    }
    let selection: pioneer_protocol::AgentExecutionSelection =
        serde_json::from_str(&execution.requested_profile_selection_json)
            .context("thread creator execution selection is invalid")?;
    Ok(selection.reasoning.map(|reasoning| reasoning.effort))
}

/// Bounded list projection counterpart of empty_agent_thread_reasoning. The
/// caller already read the immutable thread creators in its list query.
pub(crate) async fn agent_thread_creator_reasoning<C: ConnectionTrait>(
    db: &C,
    execution_ids: &[&str],
) -> Result<BTreeMap<String, (String, Option<String>)>> {
    if execution_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let executions = agent_execution::Entity::find()
        .filter(agent_execution::Column::Id.is_in(execution_ids.iter().copied()))
        .all(db)
        .await?;
    // The query released its reader before parsing the saved selections.
    executions
        .into_iter()
        .map(|execution| {
            let selection: pioneer_protocol::AgentExecutionSelection =
                serde_json::from_str(&execution.requested_profile_selection_json)
                    .context("thread creator execution selection is invalid")?;
            Ok((
                execution.id,
                (
                    execution.workspace_id,
                    selection.reasoning.map(|reasoning| reasoning.effort),
                ),
            ))
        })
        .collect()
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

use pioneer_crud_execution::agent_grant::parse_agent_execution_launch_grant;
pub use pioneer_crud_execution::agent_grant::{
    AgentExecutionGrantInput, agent_execution_grant_fingerprint, insert_agent_execution_grant,
    load_agent_execution_grant, upgrade_agent_execution_grant_model_to_current,
};

pub async fn enqueue_agent_execution<C: GatewayControlConnection>(
    db: &C,
    input: &AgentQueueEntryInput,
) -> Result<agent_work_queue::Model> {
    let change = db.gateway_control_change_for(&[crate::GatewayControlWake::Resources]);
    let (queue, inserted) =
        pioneer_crud_execution::agent_queue::enqueue_agent_execution(db, input).await?;
    if let Some(change) = change {
        change.completed(inserted);
    }
    Ok(queue)
}

pub async fn promote_queued_agent_executions(
    db: &DatabaseTransaction,
    now: DateTimeWithTimeZone,
    limit: u64,
    idle_timeout_secs: i64,
    hard_timeout_secs: i64,
) -> Result<Vec<PromotedAgentExecution>> {
    pioneer_crud_execution::agent_graph_admission::promote_queued_agent_executions(
        db,
        now,
        limit,
        idle_timeout_secs,
        hard_timeout_secs,
        &TaskOccurrenceChanges,
    )
    .await
}

pub use pioneer_crud_execution::agent_action_timeline::{
    AgentActionTimelineProjection, bind_agent_action_timeline_target,
    load_agent_action_timeline_projections_for_targets,
};

pub use pioneer_crud_execution::agent_actions::load_agent_action_outbox;

pub use pioneer_crud_execution::agent_action_commit::{
    AgentCommitInput, AgentResourceCommitInput, TerminalTaskDeliveryCommitError,
};
pub(crate) use pioneer_crud_execution::agent_action_commit::{
    agent_action_commit_exists, agent_delivery_result_reference,
};

pub async fn commit_agent_action(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
) -> Result<agent_domain_commit::Model> {
    pioneer_crud_execution::agent_action_commit::commit_agent_action(
        db,
        input,
        commit_agent_action_resources,
    )
    .await
}

/// The delivery writer is intentionally typed because its Task notification
/// is committed in the same transaction. The mutation kind is checked before
/// the shared idempotent transaction runs.
pub async fn commit_agent_delivery_action(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
) -> Result<agent_domain_commit::Model> {
    if input.mutation_kind != "deliver_result" {
        bail!(
            "typed agent domain commit expected `deliver_result`, got `{}`",
            input.mutation_kind
        );
    }
    commit_agent_action(db, input).await
}

async fn commit_agent_action_resources(
    db: &DatabaseTransaction,
    resource: &AgentResourceCommitInput,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    pioneer_crud_execution::agent_graph_admission::commit_agent_action_resources(
        db,
        resource,
        now,
        &TaskOccurrenceChanges,
    )
    .await
}

pub use pioneer_crud_execution::agent_identity::commit_native_agent_config_mutation;

pub use pioneer_crud_execution::agent_action_compaction::{
    AgentLedgerCursor, compact_terminal_agent_action_ledger,
    compact_terminal_agent_action_ledger_page,
};

pub use pioneer_crud_execution::outbox_discovery::next_deadline as next_agent_action_outbox_deadline;

/// Claim at most 32 immediate and 32 timed inputs. Expired final leases share
/// this budget with delivery work. Nonzero limits must be 2..=64 so both ranges
/// receive a share. Dispatch only `rows`, whose commits are known to have succeeded.
pub async fn claim_agent_action_outbox<C: GatewayControlConnection + TransactionTrait>(
    db: &C,
    limit: u64,
) -> Result<AgentActionOutboxClaimBatch> {
    let change = db.gateway_control_change_for(&[crate::GatewayControlWake::Actions]);
    let result = claim_agent_action_outbox_with_clock(db, limit, &utc_now).await;
    if let Ok(batch) = &result {
        if batch.errors.is_empty() {
            if let Some(change) = change {
                change.completed(!batch.rows.is_empty());
            }
        }
    }
    result
}

pub async fn mark_agent_action_outbox_delivered<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    delivered_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let change = db.gateway_control_change_for(&[crate::GatewayControlWake::Actions]);
    let result = pioneer_crud_execution::outbox_claim::mark_agent_action_outbox_delivered(
        db,
        outbox_id,
        expected_attempts,
        delivered_at,
    )
    .await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed);
        }
    }
    result
}

pub async fn mark_agent_action_outbox_failed<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    failed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    mark_agent_action_outbox_failed_with_cause(
        db,
        outbox_id,
        expected_attempts,
        failed_at,
        AgentActionOutboxFailure::DeliveryFailed,
        false,
    )
    .await
}

pub async fn mark_agent_action_outbox_failed_with_cause<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    failed_at: DateTimeWithTimeZone,
    failure: AgentActionOutboxFailure,
    terminal: bool,
) -> Result<bool> {
    let change = db.gateway_control_change_for(&[crate::GatewayControlWake::Actions]);
    let result = pioneer_crud_execution::outbox_claim::mark_agent_action_outbox_failed_with_cause(
        db,
        outbox_id,
        expected_attempts,
        failed_at,
        failure,
        terminal,
    )
    .await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed);
        }
    }
    result
}

/// Release an outbox claim whose execution is still legitimately queued.
/// Waiting for root capacity is not a delivery attempt and therefore must not
/// consume the bounded failure budget. The scheduler explicitly wakes this
/// row when it grants the execution a permit; the short fallback deadline
/// also keeps recovery independent from an in-memory notification.
pub async fn defer_agent_action_outbox_for_permit<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    deferred_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let change = db.gateway_control_change_for(&[crate::GatewayControlWake::Actions]);
    let result = pioneer_crud_execution::outbox_claim::defer_agent_action_outbox_for_permit(
        db,
        outbox_id,
        expected_attempts,
        deferred_at,
    )
    .await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed);
        }
    }
    result
}

/// Release a StartAgent outbox claim while another native Turn still owns the
/// target thread runtime. This is ordinary serialization, not a delivery
/// failure, so it restores the attempt counter and retries promptly after the
/// parent Turn can finish.
pub async fn defer_agent_action_outbox_for_runtime<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    deferred_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let change = db.gateway_control_change_for(&[crate::GatewayControlWake::Actions]);
    let result = pioneer_crud_execution::outbox_claim::defer_agent_action_outbox_for_runtime(
        db,
        outbox_id,
        expected_attempts,
        deferred_at,
    )
    .await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed);
        }
    }
    result
}

/// Make a capacity-deferred StartAgent row immediately eligible after the
/// exact execution is promoted. The failure-class fence prevents scheduler
/// activity from disturbing an unrelated delivery retry.
pub async fn wake_agent_action_outbox_for_execution<C: GatewayControlConnection>(
    db: &C,
    execution_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let change = db.gateway_control_change_for(&[crate::GatewayControlWake::Actions]);
    let result = pioneer_crud_execution::outbox_claim::wake_agent_action_outbox_for_execution(
        db,
        execution_id,
        now,
    )
    .await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed > 0);
        }
    }
    result
}

pub use pioneer_crud_execution::agent_graph_resources::ensure_root_resource_scope;

pub(crate) async fn graph_scheduling_facts(
    db: &DatabaseTransaction,
    input: &AgentExecutionGraphCommitInput,
) -> Result<pioneer_crud_execution::agent_graph_resources::GraphSchedulingFacts> {
    pioneer_crud_execution::agent_graph_resources::graph_scheduling_facts(
        db,
        &input.child_resource_state.id,
        &input.root_execution_id,
        &input.child_execution.id,
        input.child_resource_state.attempt_generation,
    )
    .await
}

pub async fn commit_agent_execution_graph(
    db: &DatabaseTransaction,
    input: &AgentExecutionGraphCommitInput,
) -> Result<AgentExecutionGraphCommitResult> {
    let graph = pioneer_crud_execution::agent_graph_admission::commit_agent_execution_graph(
        db,
        input,
        &TaskOccurrenceChanges,
    )
    .await?;
    commit_task_graph_contracts(db, input, graph).await
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
pub async fn finalize_agent_execution(
    db: &DatabaseTransaction,
    execution_id: &str,
    terminal_status: &str,
    finished_at: DateTimeWithTimeZone,
) -> Result<bool> {
    use pioneer_crud_execution::agent_execution::{
        finalize_agent_execution_status, release_finalized_agent_execution,
    };
    let finalized =
        finalize_agent_execution_status(db, execution_id, terminal_status, finished_at).await?;
    super::task_occurrence_producers::agent_change(
        db,
        Some(&finalized.previous),
        Some(&finalized.current),
    )
    .await?;
    release_finalized_agent_execution(db, &finalized, finished_at).await?;
    Ok(true)
}

pub async fn reopen_agent_execution_for_retry(
    db: &DatabaseTransaction,
    execution_id: &str,
    expected_root_execution_id: &str,
    reopened_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let (previous, current, changed) =
        pioneer_crud_execution::agent_execution::reopen_agent_execution_for_retry(
            db,
            execution_id,
            expected_root_execution_id,
            reopened_at,
        )
        .await?;
    super::task_occurrence_producers::agent_change(db, Some(&previous), Some(&current)).await?;
    Ok(changed)
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
    cancel_agent_work_graph_targets(db, root_execution_id, terminal_reason, finished_at, false)
        .await
}

/// The await caller must also see already-fenced bindings on retry. This is
/// collected inside the same bounded native graph write, before its fence.
pub async fn cancel_agent_work_graph_and_collect_owners(
    db: &DatabaseTransaction,
    root_execution_id: &str,
    terminal_reason: &str,
    finished_at: DateTimeWithTimeZone,
) -> Result<Vec<AgentWorkGraphCancellationTarget>> {
    cancel_agent_work_graph_targets(db, root_execution_id, terminal_reason, finished_at, true).await
}

async fn cancel_agent_work_graph_targets(
    db: &DatabaseTransaction,
    root_execution_id: &str,
    terminal_reason: &str,
    finished_at: DateTimeWithTimeZone,
    collect_owners: bool,
) -> Result<Vec<AgentWorkGraphCancellationTarget>> {
    let root = agent_execution::Entity::find_by_id(root_execution_id.to_owned())
        .lock_exclusive()
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
        .lock_exclusive()
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
    let target_executions = if collect_owners {
        executions.iter().collect::<Vec<_>>()
    } else {
        // A prior graph fence may have committed before its direct response Turn
        // was interrupted. Rediscover that response on retry, while real Tasks
        // retain their aggregate cancellation/reconciliation protocol.
        executions
            .iter()
            .filter(|execution| {
                !terminal_statuses.contains(&execution.status.as_str())
                    || (execution.status == "cancelled" && execution.parent_task_id.is_none())
            })
            .collect::<Vec<_>>()
    };
    let target_ids = target_executions
        .iter()
        .map(|execution| execution.id.clone())
        .collect::<Vec<_>>();
    let targets = if collect_owners {
        let mut bindings = BTreeMap::<String, Vec<(String, String, bool, bool)>>::new();
        for batch in target_ids.chunks(AGENT_GRAPH_RESPONSE_QUERY_BATCH) {
            let responses = agent_turn_response_execution::Entity::find()
                .filter(agent_turn_response_execution::Column::ExecutionId.is_in(batch.to_vec()))
                .select_only()
                .column(agent_turn_response_execution::Column::ExecutionId)
                .column(agent_turn_response_execution::Column::TurnId)
                .column_as(turn::Column::ThreadId, "bound_thread_id")
                .column_as(turn::Column::Status, "bound_status")
                .expr_as(
                    sea_orm::sea_query::Expr::col((turn::Entity, turn::Column::Id)).in_subquery(
                        Query::select()
                            .column(turn_cli_runtime_binding::Column::TurnId)
                            .from(turn_cli_runtime_binding::Entity)
                            .to_owned(),
                    ),
                    "has_cli_binding",
                )
                .left_join(turn::Entity)
                .into_model::<AgentGraphResponseBinding>()
                .all(db)
                .await
                .context("failed to load Agent graph response bindings")?;
            for response in responses {
                let thread = response
                    .bound_thread_id
                    .context("Agent graph response Turn is missing")?;
                let status = response
                    .bound_status
                    .context("Agent graph response Turn status is missing")?;
                bindings.entry(response.execution_id).or_default().push((
                    response.turn_id,
                    thread,
                    status == "in_progress",
                    response.has_cli_binding,
                ));
            }
        }
        // One-to-many bindings, with an explicit unbound node for queued work.
        // Thread identity comes from the response Turn, not its execution's parent.
        let mut targets = Vec::new();
        for execution in &target_executions {
            if let Some(responses) = bindings.remove(&execution.id) {
                for (turn_id, thread_id, turn_pending, has_cli_binding) in responses {
                    targets.push(AgentWorkGraphCancellationTarget {
                        execution_id: execution.id.clone(),
                        turn_id: Some(turn_id),
                        thread_id: Some(thread_id),
                        parent_task_id: execution.parent_task_id.clone(),
                        turn_pending,
                        has_cli_binding,
                    });
                }
            } else {
                targets.push(AgentWorkGraphCancellationTarget {
                    execution_id: execution.id.clone(),
                    turn_id: None,
                    thread_id: execution.parent_thread_id.clone(),
                    parent_task_id: execution.parent_task_id.clone(),
                    turn_pending: false,
                    has_cli_binding: false,
                });
            }
        }
        targets
    } else {
        // Preserve direct-response retries and the standalone binding validation.
        let response_turns = if target_ids.is_empty() {
            Vec::new()
        } else {
            let active_turn_ids = Query::select()
                .column(turn::Column::Id)
                .from(turn::Entity)
                .and_where(turn::Column::Status.eq("in_progress"))
                .to_owned();
            agent_turn_response_execution::Entity::find()
                .filter(agent_turn_response_execution::Column::ExecutionId.is_in(target_ids))
                .filter(agent_turn_response_execution::Column::TurnId.in_subquery(active_turn_ids))
                .limit(graph_row_limit)
                .all(db)
                .await
                .context("failed to load Agent work-graph Turn bindings")?
        };
        if response_turns.len() > target_executions.len() {
            bail!("Agent work graph has too many active Turn bindings");
        }
        let mut responses = BTreeMap::new();
        for response in response_turns {
            if responses
                .insert(response.execution_id, response.turn_id)
                .is_some()
            {
                bail!("Agent execution has multiple active response Turns");
            }
        }
        target_executions
            .iter()
            .filter(|execution| {
                !terminal_statuses.contains(&execution.status.as_str())
                    || responses.contains_key(&execution.id)
            })
            .map(|execution| AgentWorkGraphCancellationTarget {
                execution_id: execution.id.clone(),
                turn_id: responses.get(&execution.id).cloned(),
                thread_id: execution.parent_thread_id.clone(),
                parent_task_id: execution.parent_task_id.clone(),
                turn_pending: true,
                has_cli_binding: false,
            })
            .collect()
    };

    let occurrence_old_executions = task_run_execution::Entity::find()
        .filter(task_run_execution::Column::Id.is_in(active_ids.clone()))
        .limit(graph_row_limit)
        .lock_exclusive()
        .all(db)
        .await?;
    let occurrence_old_contracts = task_occurrence_contract::Entity::find()
        .filter(
            task_occurrence_contract::Column::WorkGraphRootExecutionId
                .eq(root_execution_id.to_owned()),
        )
        .limit(graph_row_limit)
        .lock_exclusive()
        .all(db)
        .await?;
    if occurrence_old_contracts.len() >= usize::try_from(graph_row_limit).unwrap_or(usize::MAX) {
        bail!("Task occurrence graph exceeds bounded cancellation policy");
    }
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
    for old in &executions {
        let new = agent_execution::Entity::find_by_id(&old.id)
            .lock_exclusive()
            .one(db)
            .await?;
        super::task_occurrence_producers::agent_change(db, Some(old), new.as_ref()).await?;
    }
    for old in &occurrence_old_executions {
        let new = task_run_execution::Entity::find_by_id(old.id.clone())
            .lock_exclusive()
            .one(db)
            .await?;
        super::task_occurrence_producers::execution_change(db, Some(old), new.as_ref()).await?;
    }
    for old in &occurrence_old_contracts {
        let new = task_occurrence_contract::Entity::find_by_id(old.occurrence_id.clone())
            .lock_exclusive()
            .one(db)
            .await?;
        super::task_occurrence_producers::contract_change(db, Some(old), new.as_ref()).await?;
    }
    Ok(targets)
}

pub use pioneer_crud_execution::agent_execution::{
    close_agent_root_scope_if_drained, heartbeat_agent_execution, record_agent_execution_progress,
};

struct TaskOccurrenceChanges;

impl pioneer_crud_execution::agent_graph_admission::ExecutionChangeSink for TaskOccurrenceChanges {
    async fn agent_changed<'a, C: ConnectionTrait>(
        &'a self,
        db: &'a C,
        before: Option<&'a agent_execution::Model>,
        after: Option<&'a agent_execution::Model>,
    ) -> Result<()> {
        super::task_occurrence_producers::agent_change(db, before, after).await
    }
}

pub use pioneer_crud_execution::canonical_id::canonical_agent_id;

pub fn utc_now() -> DateTimeWithTimeZone {
    DateTime::<Utc>::from(Utc::now()).into()
}

#[cfg(test)]
#[path = "agent_domain_launch_continuity_tests.rs"]
mod launch_continuity_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::TransactionTrait;

    #[pioneer_postgres_test::postgres_test]
    async fn root_graph_cancellation_fences_only_the_exact_graph_and_releases_resources() {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection().clone();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES('workspace-cancel','Cancel');
             INSERT INTO agent_identity(id,workspace_id,source_kind,source_id,source_fingerprint)
             VALUES('I00000000000000000001','workspace-cancel','ephemeral','cancel','source');
             INSERT INTO agent_presentation_snapshot
                (id,agent_identity_id,source_revision,source_fingerprint,display_name,nickname)
             VALUES('S00000000000000000001','I00000000000000000001',1,'source','Cancel','cancel');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES('thread-cancel','workspace-cancel','','agent','m','p','active'),
                   ('thread-revisions','workspace-cancel','','agent','m','p','active');",
        )
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
                agent_identity_id: Set("I00000000000000000001".to_owned()),
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

        // A Task occurrence may retain several completed revisions plus a
        // currently admitted revision: response cardinality exceeds node count.
        for (id, status) in [
            ("initial", "completed"),
            ("revision-1", "completed"),
            ("revision-2", "completed"),
            ("revision-active", "in_progress"),
        ] {
            turn::ActiveModel {
                id: Set(id.to_owned()),
                thread_id: Set("thread-revisions".to_owned()),
                status: Set(status.to_owned()),
                prompt_manifest_json: Set("{}".to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                turn_kind: Set("agent".to_owned()),
                origin: Set("task".to_owned()),
                mentions_json: Set("[]".to_owned()),
                message_revision: Set(1),
                work_owner: Set("turn".to_owned()),
                ..Default::default()
            }
            .insert(&db)
            .await
            .unwrap();
            agent_turn_response_execution::ActiveModel {
                turn_id: Set(id.to_owned()),
                execution_id: Set(child_id.to_owned()),
                presentation_snapshot_id: Set("S00000000000000000001".to_owned()),
                created_at: Set(now),
            }
            .insert(&db)
            .await
            .unwrap();
        }
        let transaction = db.begin().await.unwrap();
        let targets = cancel_agent_work_graph(&transaction, root_id, "root cancelled", now)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(targets.len(), 3);
        let active_child = targets
            .iter()
            .find(|target| target.execution_id == child_id)
            .unwrap();
        assert_eq!(active_child.turn_id.as_deref(), Some("revision-active"));
        // Standalone still uses its original parent-thread targeting semantics.
        assert_eq!(active_child.thread_id.as_deref(), Some("thread-cancel"));
        // A committed execution fence must still rediscover its live direct
        // response until the caller has interrupted that Turn.
        let transaction = db.begin().await.unwrap();
        let retry = cancel_agent_work_graph(&transaction, root_id, "repeat", now)
            .await
            .unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].execution_id, child_id);
        assert_eq!(retry[0].turn_id.as_deref(), Some("revision-active"));
        transaction.rollback().await.unwrap();
        // Completion must retain the exact graph after that same durable fence.
        let transaction = db.begin().await.unwrap();
        let owners = cancel_agent_work_graph_and_collect_owners(&transaction, root_id, "wait", now)
            .await
            .unwrap();
        assert_eq!(owners.len(), 6);
        let revisions = owners
            .iter()
            .filter(|target| target.execution_id == child_id)
            .collect::<Vec<_>>();
        assert_eq!(revisions.len(), 4);
        assert!(
            revisions
                .iter()
                .all(|target| target.thread_id.as_deref() == Some("thread-revisions"))
        );
        assert_eq!(
            revisions
                .iter()
                .filter(|target| target.turn_pending)
                .count(),
            1
        );
        assert!(
            owners
                .iter()
                .all(|target| target.execution_id != unrelated_id)
        );
        assert!(
            owners
                .iter()
                .find(|target| target.execution_id == queued_id)
                .unwrap()
                .turn_id
                .is_none()
        );
        transaction.rollback().await.unwrap();
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
}

#[cfg(test)]
#[path = "grant_json_compat_tests.rs"]
mod grant_json_compat_tests;
