//! Agent execution identity, resource state, and per-attempt permits.
use anyhow::{Context, Result, bail};
use pioneer_entity::{
    agent_execution, agent_execution_resource_state, agent_running_permit, agent_work_queue,
    agent_work_resource_scope, task_occurrence_contract,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, PaginatorTrait, QueryFilter,
    QuerySelect, Set,
};

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
pub struct AgentResourceStateInput {
    pub id: String,
    pub execution_id: String,
    pub attempt_generation: i64,
    pub branch_key: String,
    pub fair_order: i64,
    pub now: DateTimeWithTimeZone,
}

pub async fn insert_agent_execution<C: ConnectionTrait>(
    db: &C,
    input: &AgentExecutionInput,
) -> Result<(agent_execution::Model, bool)> {
    if input.execution_generation < 1 {
        bail!("execution generation must be positive");
    }
    let inserted = agent_execution::Entity::insert(agent_execution::ActiveModel {
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
        .lock_exclusive()
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
    Ok((execution, inserted == 1))
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
/// Terminal state and affected attempt ownership. The caller coordinates any
/// other domain's consequences before releasing resources in the transaction.
pub struct FinalizedAgentExecution {
    pub previous: agent_execution::Model,
    pub current: agent_execution::Model,
    resource_terminal_status: &'static str,
}

pub async fn finalize_agent_execution_status(
    db: &DatabaseTransaction,
    execution_id: &str,
    terminal_status: &str,
    finished_at: DateTimeWithTimeZone,
) -> Result<FinalizedAgentExecution> {
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
        .lock_exclusive()
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
    let current = agent_execution::Entity::find_by_id(execution_id)
        .one(db)
        .await?
        .context("agent execution disappeared during finalization")?;
    Ok(FinalizedAgentExecution {
        previous: execution,
        current,
        resource_terminal_status,
    })
}

pub async fn release_finalized_agent_execution(
    db: &DatabaseTransaction,
    finalized: &FinalizedAgentExecution,
    finished_at: DateTimeWithTimeZone,
) -> Result<()> {
    let execution = &finalized.current;
    let execution_id = &execution.id;
    let resource_terminal_status = finalized.resource_terminal_status;
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
    Ok(())
}

/// Reopen the immutable AgentExecution for an explicit retry while leaving
/// the terminal resource attempt as history. The caller advances the Task
/// occurrence retry generation in the same transaction, so subsequent graph
/// materialization creates a fresh resource state and permit/queue row.
pub async fn reopen_agent_execution_for_retry(
    db: &DatabaseTransaction,
    execution_id: &str,
    expected_root_execution_id: &str,
    reopened_at: DateTimeWithTimeZone,
) -> Result<(agent_execution::Model, agent_execution::Model, bool)> {
    let execution = agent_execution::Entity::find_by_id(execution_id.to_owned())
        .lock_exclusive()
        .one(db)
        .await
        .context("failed to load AgentExecution for retry")?
        .with_context(|| format!("AgentExecution `{execution_id}` is missing"))?;
    if execution.work_graph_root_execution_id != expected_root_execution_id {
        bail!("AgentExecution retry crosses its immutable work-graph root");
    }
    if !matches!(
        execution.status.as_str(),
        "blocked" | "failed" | "timed_out"
    ) {
        bail!(
            "AgentExecution `{execution_id}` is `{}`; only a failed resumable execution can retry",
            execution.status
        );
    }

    let scope =
        agent_work_resource_scope::Entity::find_by_id(expected_root_execution_id.to_owned())
            .one(db)
            .await
            .context("failed to load AgentExecution root scope for retry")?
            .context("AgentExecution retry has no durable root resource scope")?;
    if !matches!(scope.status.as_str(), "active" | "closed") {
        bail!("AgentExecution retry root scope is not resumable");
    }
    if scope.status == "closed" {
        let reopened_scope = agent_work_resource_scope::Entity::update_many()
            .col_expr(
                agent_work_resource_scope::Column::Status,
                sea_orm::sea_query::Expr::value("active"),
            )
            .col_expr(
                agent_work_resource_scope::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(reopened_at),
            )
            .filter(
                agent_work_resource_scope::Column::RootExecutionId
                    .eq(expected_root_execution_id.to_owned()),
            )
            .filter(agent_work_resource_scope::Column::Status.eq("closed"))
            .exec(db)
            .await
            .context("failed to reopen AgentExecution root scope for retry")?;
        if reopened_scope.rows_affected != 1 {
            bail!("AgentExecution root scope changed while retry was committing");
        }
    }

    let reopened = agent_execution::Entity::update_many()
        .col_expr(
            agent_execution::Column::Status,
            sea_orm::sea_query::Expr::value("recovering"),
        )
        .col_expr(
            agent_execution::Column::FinishedAt,
            sea_orm::sea_query::Expr::value(None::<DateTimeWithTimeZone>),
        )
        .col_expr(
            agent_execution::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(reopened_at),
        )
        .filter(agent_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(agent_execution::Column::Status.is_in(["blocked", "failed", "timed_out"]))
        .exec(db)
        .await
        .context("failed to reopen AgentExecution for retry")?;
    let current = agent_execution::Entity::find_by_id(execution_id)
        .one(db)
        .await?
        .context("agent execution disappeared during retry")?;
    Ok((execution, current, reopened.rows_affected == 1))
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
pub async fn heartbeat_agent_execution(
    db: &DatabaseTransaction,
    execution_id: &str,
    expected_attempt_generation: i64,
    heartbeat_at: DateTimeWithTimeZone,
    idle_deadline: Option<DateTimeWithTimeZone>,
) -> Result<bool> {
    agent_execution::Entity::find_by_id(execution_id.to_owned())
        .select_only()
        .column(agent_execution::Column::Id)
        .lock_exclusive()
        .into_tuple::<String>()
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
            crate::agent_queue::extend_idle_deadline(idle_deadline),
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
pub async fn record_agent_execution_progress(
    db: &DatabaseTransaction,
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
        .select_only()
        .column(agent_execution::Column::Id)
        .lock_exclusive()
        .into_tuple::<String>()
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
            Expr::col(agent_execution_resource_state::Column::ProgressSequence).add(1_i64),
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
            crate::agent_queue::extend_idle_deadline(idle_deadline),
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

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_postgres_test::with_database;
    use sea_orm::TransactionTrait;

    #[tokio::test]
    async fn execution_identity_and_permit_replay_use_the_same_committed_rows() -> Result<()> {
        with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared(
                "INSERT INTO workspace(id,name) VALUES ('execution-ws','Execution');
                 INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
                 VALUES ('execution-thread','execution-ws','','agent','model','provider','active');
                 INSERT INTO agent_identity(id,workspace_id,source_kind,source_id,source_fingerprint)
                 VALUES ('I00000000000000000001','execution-ws','ephemeral','identity','source');",
            ).await?;
            let id = "E00000000000000000001";
            let now = chrono::Utc::now().fixed_offset();
            let input = AgentExecutionInput {
                id: id.to_owned(),
                workspace_id: "execution-ws".to_owned(),
                agent_identity_id: "I00000000000000000001".to_owned(),
                identity_source_revision: 1,
                identity_source_fingerprint: "source".to_owned(),
                parent_execution_id: None,
                parent_task_id: None,
                parent_thread_id: None,
                home_root_thread_id: "execution-thread".to_owned(),
                work_graph_root_execution_id: id.to_owned(),
                requested_identity_selection_json: "{}".to_owned(),
                requested_profile_selection_json: "{}".to_owned(),
                resolved_profile_id: None,
                resolved_profile_fingerprint: None,
                presentation_snapshot_id: None,
                authorization_context_fingerprint: "auth".to_owned(),
                execution_generation: 1,
                status: "queued".to_owned(),
                now,
            };
            let resource = AgentResourceStateInput {
                id: "resource-id".to_owned(),
                execution_id: id.to_owned(),
                attempt_generation: 1,
                branch_key: "branch".to_owned(),
                fair_order: 1,
                now,
            };
            let observer = fixture.connect().await?;
            for commit in [false, true] {
                let tx = db.begin().await?;
                let (created, inserted) = insert_agent_execution(&tx, &input).await?;
                assert!(inserted);
                assert_eq!(created.id, id);
                let (replayed, inserted) = insert_agent_execution(&tx, &input).await?;
                assert!(!inserted);
                assert_eq!(created, replayed);
                let state = insert_agent_resource_state(&tx, &resource).await?;
                assert_eq!(state, insert_agent_resource_state(&tx, &resource).await?);
                let permit = acquire_agent_running_permit(&tx, "permit-id", id, id, 1, now).await?;
                assert_eq!(permit, acquire_agent_running_permit(&tx, "permit-id", id, id, 1, now).await?);
                assert!(load_agent_execution(&observer, id).await?.is_none());
                if commit {
                    tx.commit().await?;
                    assert_eq!(load_agent_execution(&observer, id).await?, Some(created));
                } else {
                    tx.rollback().await?;
                    assert!(load_agent_execution(&observer, id).await?.is_none());
                }
            }

            let tx = db.begin().await?;
            let mut conflict = input;
            conflict.authorization_context_fingerprint = "different".to_owned();
            assert!(insert_agent_execution(&tx, &conflict).await.unwrap_err().to_string().contains("different immutable execution facts"));
            tx.rollback().await?;
            observer.close().await?;
            Ok(())
        }).await
    }
}
