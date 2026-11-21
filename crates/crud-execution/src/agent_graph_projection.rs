//! Bounded projection of a root agent's durable work graph.

use crate::execution_reads::load_agent_turn_response;
use anyhow::{Context, Result, anyhow, bail};
use pioneer_entity::{
    agent_execution, agent_execution_resource_state, agent_turn_response_execution,
    agent_work_resource_scope, thread, turn,
};
use pioneer_protocol::{
    AgentExecutionId, AgentWorkGraphProjection, AgentWorkNodeProjection, AgentWorkNodeState,
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};
use std::collections::BTreeMap;

const AGENT_PROJECTION_BATCH_LIMIT: usize = 200;
const AGENT_WORK_GRAPH_MAX_CONCURRENCY: i64 = 4_096;
const AGENT_WORK_GRAPH_MAX_QUEUE_DEPTH: i64 = 2_048;
const AGENT_WORK_GRAPH_MAX_DEPTH: i64 = 64;
const AGENT_WORK_GRAPH_MAX_FAN_OUT: i64 = 128;
const AGENT_WORK_GRAPH_MAX_TOTAL_NODES: i64 = 4_096;

pub fn agent_work_resource_limits_are_bounded(
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
pub struct AgentWorkGraphProjectionTarget {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
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

#[cfg(test)]
mod tests;

/// An empty agent-created thread has no Conversation turn from which to
/// restore composer reasoning. Its creator execution stores the immutable
/// applied selection; reading it neither creates identity nor grants authority.
pub async fn empty_agent_thread_reasoning<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<String>> {
    let Some(thread) = thread::Entity::find_by_id(thread_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let Some(pioneer_protocol::PersistedActorRef::AgentExecution(execution_id)) =
        pioneer_crud_access::identity::actor_ref_from_db(
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
pub async fn agent_thread_creator_reasoning<C: ConnectionTrait>(
    db: &C,
    execution_ids: &[&str],
) -> Result<std::collections::BTreeMap<String, (String, Option<String>)>> {
    if execution_ids.is_empty() {
        return Ok(std::collections::BTreeMap::new());
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
