//! Row locks, execution fencing and scope closure for a cancelled Agent graph.
use crate::agent_graph_admission::ExecutionChangeSink;
use anyhow::{Context, Result, bail};
use pioneer_entity::{
    agent_execution, agent_execution_resource_state, agent_running_permit,
    agent_turn_response_execution, agent_work_queue, agent_work_resource_scope, turn,
    turn_cli_runtime_binding,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Query;
use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect};
use std::collections::BTreeMap;

const TERMINAL_STATUSES: [&str; 6] = [
    "completed",
    "succeeded",
    "failed",
    "blocked",
    "cancelled",
    "timed_out",
];

// Independent of graph nodes: one Task occurrence may have many revision Turns.
const AGENT_GRAPH_RESPONSE_QUERY_BATCH: usize = 128;

use crate::agent_graph_projection::agent_work_resource_limits_are_bounded;

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

pub struct PreparedGraphCancellation {
    pub graph_row_limit: u64,
    pub active_ids: Vec<String>,
    executions: Vec<agent_execution::Model>,
    targets: Vec<AgentWorkGraphCancellationTarget>,
}

pub async fn lock_graph_for_cancellation(
    db: &DatabaseTransaction,
    root_execution_id: &str,
    collect_owners: bool,
) -> Result<PreparedGraphCancellation> {
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
        .filter(|execution| !TERMINAL_STATUSES.contains(&execution.status.as_str()))
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
                !TERMINAL_STATUSES.contains(&execution.status.as_str())
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
                !TERMINAL_STATUSES.contains(&execution.status.as_str())
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

    Ok(PreparedGraphCancellation {
        graph_row_limit,
        active_ids,
        executions,
        targets,
    })
}

impl PreparedGraphCancellation {
    pub async fn fence_executions(
        &self,
        db: &DatabaseTransaction,
        finished_at: DateTimeWithTimeZone,
    ) -> Result<()> {
        let active_ids = &self.active_ids;
        let graph_row_limit = self.graph_row_limit;
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
                .filter(agent_execution::Column::Id.is_in(active_ids.to_vec()))
                .filter(agent_execution::Column::Status.is_not_in(TERMINAL_STATUSES))
                .exec(db)
                .await
                .context("failed to fence cancelled Agent work-graph executions")?;

            let resource_states = agent_execution_resource_state::Entity::find()
                .filter(
                    agent_execution_resource_state::Column::ExecutionId.is_in(active_ids.to_vec()),
                )
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
                .filter(agent_work_queue::Column::ExecutionId.is_in(active_ids.to_vec()))
                .filter(agent_work_queue::Column::State.is_in([
                    "queued",
                    "recovering",
                    "claimed",
                    "running",
                ]))
                .exec(db)
                .await
                .context("failed to cancel Agent work-graph queue entries")?;
        }
        Ok(())
    }

    pub async fn close_and_publish(
        self,
        db: &DatabaseTransaction,
        root_execution_id: &str,
        finished_at: DateTimeWithTimeZone,
        changes: &impl ExecutionChangeSink,
    ) -> Result<Vec<AgentWorkGraphCancellationTarget>> {
        agent_work_resource_scope::Entity::update_many()
            .col_expr(
                agent_work_resource_scope::Column::Status,
                sea_orm::sea_query::Expr::value("closed"),
            )
            .col_expr(
                agent_work_resource_scope::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(finished_at),
            )
            .filter(
                agent_work_resource_scope::Column::RootExecutionId.eq(root_execution_id.to_owned()),
            )
            .filter(agent_work_resource_scope::Column::Status.is_in([
                "active",
                "queued",
                "recovering",
                "draining",
            ]))
            .exec(db)
            .await
            .context("failed to close cancelled Agent work-graph scope")?;
        for old in &self.executions {
            let new = agent_execution::Entity::find_by_id(&old.id)
                .lock_exclusive()
                .one(db)
                .await?;
            changes.agent_changed(db, Some(old), new.as_ref()).await?;
        }
        Ok(self.targets)
    }
}
