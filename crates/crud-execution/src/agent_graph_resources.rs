//! Bounded execution resources and persisted work graph lineage.
use anyhow::{Context, Result, bail};
use pioneer_entity::{
    agent_execution, agent_execution_resource_state, agent_work_queue, agent_work_resource_scope,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, PaginatorTrait, QueryFilter,
    QuerySelect, Set,
};

use crate::agent_graph_projection::agent_work_resource_limits_are_bounded;

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
    Ok(scope)
}

pub type GraphSchedulingFacts = (
    Option<(
        String,
        Option<String>,
        Option<DateTimeWithTimeZone>,
        Option<DateTimeWithTimeZone>,
    )>,
    Option<(String, Option<DateTimeWithTimeZone>, Option<String>)>,
);

/// Read before and after graph admission under its caller's writer transaction.
pub async fn graph_scheduling_facts(
    db: &DatabaseTransaction,
    resource_state_id: &str,
    root_execution_id: &str,
    child_execution_id: &str,
    attempt_generation: i64,
) -> Result<GraphSchedulingFacts> {
    let state = agent_execution_resource_state::Entity::find_by_id(resource_state_id)
        .select_only()
        .columns([
            agent_execution_resource_state::Column::Status,
            agent_execution_resource_state::Column::PermitId,
            agent_execution_resource_state::Column::IdleDeadline,
            agent_execution_resource_state::Column::HardDeadline,
        ])
        .into_tuple::<(
            String,
            Option<String>,
            Option<DateTimeWithTimeZone>,
            Option<DateTimeWithTimeZone>,
        )>()
        .one(db)
        .await?;
    let queue = agent_work_queue::Entity::find()
        .filter(agent_work_queue::Column::RootExecutionId.eq(root_execution_id))
        .filter(agent_work_queue::Column::ExecutionId.eq(child_execution_id))
        .filter(agent_work_queue::Column::AttemptGeneration.eq(attempt_generation))
        .select_only()
        .columns([
            agent_work_queue::Column::State,
            agent_work_queue::Column::EligibleAt,
            agent_work_queue::Column::ClaimToken,
        ])
        .into_tuple::<(String, Option<DateTimeWithTimeZone>, Option<String>)>()
        .one(db)
        .await?;
    Ok((state, queue))
}

pub async fn validate_agent_graph_bounds(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{EXECUTION_ID, seed_agent_execution};
    use sea_orm::TransactionTrait;

    #[pioneer_postgres_test::postgres_test]
    async fn resource_scope_rollback_and_retry_keep_the_first_committed_limits() -> Result<()> {
        let fixture = pioneer_postgres_test::scoped_database().await?;
        let db = fixture.connection();
        seed_agent_execution(db).await;
        let now = chrono::Utc::now().fixed_offset();

        let tx = db.begin().await?;
        ensure_root_resource_scope(&tx, EXECUTION_ID, 1, 8, 8, 8, 64, now).await?;
        tx.rollback().await?;
        assert!(
            agent_work_resource_scope::Entity::find_by_id(EXECUTION_ID)
                .one(db)
                .await?
                .is_none()
        );

        let tx = db.begin().await?;
        let created = ensure_root_resource_scope(&tx, EXECUTION_ID, 2, 8, 8, 8, 64, now).await?;
        let root = agent_execution::Entity::find_by_id(EXECUTION_ID)
            .one(&tx)
            .await?
            .context("root execution missing")?;
        validate_agent_graph_bounds(&tx, &root, &created).await?;
        tx.commit().await?;
        let retried = ensure_root_resource_scope(db, EXECUTION_ID, 1, 9, 8, 8, 64, now).await?;
        assert_eq!(retried.scope_generation, created.scope_generation);
        assert_eq!(retried.max_concurrency, 2);
        assert_eq!(retried.max_queue_depth, 8);
        assert!(
            ensure_root_resource_scope(db, EXECUTION_ID, 0, 8, 8, 8, 64, now)
                .await
                .is_err()
        );
        Ok(())
    }
}
