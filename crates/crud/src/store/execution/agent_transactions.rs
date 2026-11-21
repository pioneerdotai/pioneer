//! Atomic agent graph, thread creation and action commits.
//!
//! These functions deliberately keep identity, lineage, action receipts and
//! resource state in separate records.  Callers that need an all-or-nothing
//! mutation pass a `DatabaseTransaction`; no function infers an actor from a
//! provider, model, display label or the last message.

use anyhow::{Context, Result, bail};
use pioneer_crud_execution::agent_action_commit::{AgentCommitInput, AgentResourceCommitInput};
use pioneer_crud_execution::agent_delegation_route::insert_agent_delegation_route;
use pioneer_crud_execution::agent_graph_admission;
use pioneer_crud_execution::agent_graph_admission::{
    AgentExecutionGraphCommitInput, AgentExecutionGraphCommitResult, PromotedAgentExecution,
};
use pioneer_crud_execution::agent_graph_cancellation::AgentWorkGraphCancellationTarget;
use pioneer_crud_execution::agent_thread_creation::{
    AgentThreadCreationCommitInput, validate_agent_thread_creation,
};
use pioneer_crud_tasks::agent_execution_binding::{
    TaskOccurrenceChanges, commit_task_graph_contracts,
};
use pioneer_entity::agent_domain_commit;
use pioneer_protocol::PersistedActorRef;
use sea_orm::DatabaseTransaction;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

/// Atomically materialize a server-resolved thread and its execution-scoped
/// continuation route. Callers pass a DatabaseTransaction when this is part
/// of an AgentAction commit; no user-selected participant/ACL data is accepted
/// here.
pub async fn commit_agent_thread_creation(
    db: &DatabaseTransaction,
    input: &AgentThreadCreationCommitInput<'_>,
) -> Result<()> {
    let access_class = validate_agent_thread_creation(db, input).await?;
    let creator = PersistedActorRef::AgentExecution(input.execution_id.clone());
    crate::store::conversations::thread_writes::upsert_agent_thread_with_creator(
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
        crate::store::conversations::thread_writes::upsert_lineage(db, lineage)
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

pub async fn promote_queued_agent_executions(
    db: &DatabaseTransaction,
    now: DateTimeWithTimeZone,
    limit: u64,
    idle_timeout_secs: i64,
    hard_timeout_secs: i64,
) -> Result<Vec<PromotedAgentExecution>> {
    agent_graph_admission::promote_queued_agent_executions(
        db,
        now,
        limit,
        idle_timeout_secs,
        hard_timeout_secs,
        &TaskOccurrenceChanges,
    )
    .await
}

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
    agent_graph_admission::commit_agent_action_resources(db, resource, now, &TaskOccurrenceChanges)
        .await
}

/// Claim at most 32 immediate and 32 timed inputs. Expired final leases share
/// this budget with delivery work. Nonzero limits must be 2..=64 so both ranges
/// receive a share. Dispatch only `rows`, whose commits are known to have succeeded.

pub async fn commit_agent_execution_graph(
    db: &DatabaseTransaction,
    input: &AgentExecutionGraphCommitInput,
) -> Result<AgentExecutionGraphCommitResult> {
    let graph =
        agent_graph_admission::commit_agent_execution_graph(db, input, &TaskOccurrenceChanges)
            .await?;
    commit_task_graph_contracts(db, input, graph).await
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
    let graph = pioneer_crud_execution::agent_graph_cancellation::lock_graph_for_cancellation(
        db,
        root_execution_id,
        collect_owners,
    )
    .await?;
    let tasks = pioneer_crud_tasks::agent_graph_cancellation::PreparedTaskGraphCancellation::lock(
        db,
        root_execution_id,
        &graph.active_ids,
        graph.graph_row_limit,
    )
    .await?;
    graph.fence_executions(db, finished_at).await?;
    tasks
        .fence_tasks(
            db,
            root_execution_id,
            &graph.active_ids,
            terminal_reason,
            finished_at,
        )
        .await?;
    let targets = graph
        .close_and_publish(db, root_execution_id, finished_at, &TaskOccurrenceChanges)
        .await?;
    tasks.publish_changes(db).await?;
    Ok(targets)
}

#[cfg(test)]
#[path = "agent_launch_continuity_tests.rs"]
mod launch_continuity_tests;

#[cfg(test)]
#[path = "agent_graph_cancellation_tests.rs"]
mod tests;
