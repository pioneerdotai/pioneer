//! Task occurrence publication when a durable Agent execution changes.
use anyhow::Result;
use pioneer_crud_execution::{
    agent_execution::AgentExecutionInput,
    agent_graph_admission::{AgentExecutionGraphCommitInput, AgentExecutionGraphCommitResult},
};
use pioneer_entity::agent_execution;
use pioneer_protocol::TaskOccurrenceStatus;
use sea_orm::{ConnectionTrait, DatabaseTransaction, entity::prelude::DateTimeWithTimeZone};

pub async fn insert_agent_execution<C: ConnectionTrait>(
    db: &C,
    input: &AgentExecutionInput,
) -> Result<agent_execution::Model> {
    let (execution, inserted) =
        pioneer_crud_execution::agent_execution::insert_agent_execution(db, input).await?;
    if inserted {
        crate::task_occurrence_producers::agent_change(db, None, Some(&execution)).await?;
    }
    Ok(execution)
}

pub async fn commit_task_graph_contracts(
    db: &DatabaseTransaction,
    input: &AgentExecutionGraphCommitInput,
    result: AgentExecutionGraphCommitResult,
) -> Result<AgentExecutionGraphCommitResult> {
    if let Some(actor) = input.task_actor_contract.as_ref() {
        crate::task_actor_contract::upsert_task_actor_contract(db, actor, input.contract_now)
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
        crate::task_actor_contract::upsert_task_occurrence_contract(
            db,
            &occurrence,
            input.contract_now,
        )
        .await?;
    }
    Ok(result)
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
    crate::task_occurrence_producers::agent_change(db, Some(&previous), Some(&current)).await?;
    Ok(changed)
}

pub struct TaskOccurrenceChanges;

impl pioneer_crud_execution::agent_graph_admission::ExecutionChangeSink for TaskOccurrenceChanges {
    async fn agent_changed<'a, C: ConnectionTrait>(
        &'a self,
        db: &'a C,
        before: Option<&'a agent_execution::Model>,
        after: Option<&'a agent_execution::Model>,
    ) -> Result<()> {
        crate::task_occurrence_producers::agent_change(db, before, after).await
    }
}
