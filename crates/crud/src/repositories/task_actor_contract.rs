//! Persistence for the exact actor/occurrence/delivery facts of agent domain.

use anyhow::{Context, Result, anyhow, bail};
use pioneer_entity::{task_actor_contract, task_delivery_authority, task_occurrence_contract};
use pioneer_protocol::{
    PersistedActorRef, TaskActorContract, TaskOccurrenceContract, TaskOccurrenceStatus,
    TaskResultReviewerRef,
};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    Set, Statement,
};

use crate::util::{optional_typed_json_to_db, typed_json_to_db, unix_to_datetime};

pub async fn upsert_task_actor_contract<C: ConnectionTrait>(
    db: &C,
    contract: &TaskActorContract,
    now: i64,
) -> Result<()> {
    contract
        .validate()
        .map_err(|error| anyhow!("invalid task actor contract: {error:?}"))?;
    task_actor_contract::Entity::insert(task_actor_contract::ActiveModel {
        task_id: Set(contract.task_id.clone()),
        workspace_id: Set(contract.workspace_id.clone()),
        creator_json: Set(typed_json_to_db(&contract.creator)?),
        creator_snapshot_json: Set(optional_typed_json_to_db(
            &contract.creator_presentation_snapshot,
        )?),
        reviewer_json: Set(typed_json_to_db(&contract.reviewer)?),
        execution_destination_thread_id: Set(contract.execution_destination_thread_id.clone()),
        execution_route_id: Set(contract.execution_route_id.clone()),
        execution_route_receipt_json: Set(contract.execution_route_receipt_json.clone()),
        execution_route_expires_at_millis: Set(contract.execution_route_expires_at_millis),
        delivery_json: Set(typed_json_to_db(&contract.delivery)?),
        launch_selection_json: Set(optional_typed_json_to_db(&contract.launch)?),
        requested_identity_json: Set(contract.requested_identity_json.clone()),
        resolved_identity_id: Set(contract.resolved_identity_id.clone()),
        resolved_profile_id: Set(contract.resolved_profile_id.clone()),
        source_config_fingerprint: Set(contract.source_config_fingerprint.clone()),
        derived_child_launch_grant_json: Set(contract.derived_child_launch_grant_json.clone()),
        creator_work_graph_root_execution_id: Set(contract
            .creator_work_graph_root_execution_id
            .clone()),
        work_graph_root_execution_id: Set(contract.work_graph_root_execution_id.clone()),
        root_resource_scope_id: Set(contract.root_resource_scope_id.clone()),
        accounting_attribution_json: Set(optional_typed_json_to_db(
            &contract.accounting_attribution,
        )?),
        controller_principal_id: Set(contract.controller_principal_id.clone()),
        revision: Set(i64::try_from(contract.revision).context("task actor revision overflow")?),
        created_at: Set(unix_to_datetime(now)),
        updated_at: Set(unix_to_datetime(now)),
    })
    .on_conflict(
        OnConflict::column(task_actor_contract::Column::TaskId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to insert immutable task actor contract")?;
    let persisted = find_task_actor_contract(db, contract.task_id.as_str())
        .await?
        .context("immutable task actor contract disappeared after insert")?;
    if &persisted != contract {
        bail!(
            "task actor contract `{}` conflicts with its immutable persisted facts",
            contract.task_id
        );
    }
    Ok(())
}

pub async fn find_task_actor_contract<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Option<TaskActorContract>> {
    let Some(row) = task_actor_contract::Entity::find_by_id(task_id.to_owned())
        .one(db)
        .await
        .context("failed to load task actor contract")?
    else {
        return Ok(None);
    };
    Ok(Some(task_actor_contract_from_model(row)?))
}

pub async fn upsert_task_occurrence_contract<C: ConnectionTrait>(
    db: &C,
    contract: &TaskOccurrenceContract,
    now: i64,
) -> Result<()> {
    contract
        .validate()
        .map_err(|error| anyhow!("invalid task occurrence contract: {error:?}"))?;
    let requested_at = unix_to_datetime(now);
    let persisted_row =
        task_occurrence_contract::Entity::find_by_id(contract.occurrence_id.clone())
            .one(db)
            .await
            .context("failed to load task occurrence contract before upsert")?;
    let updated_at = if let Some(row) = persisted_row.as_ref() {
        let persisted = task_occurrence_contract_from_model(row.clone())?;
        validate_occurrence_update(&persisted, contract)?;
        std::cmp::max(row.updated_at, requested_at)
    } else {
        requested_at
    };
    let result = task_occurrence_contract::Entity::insert(task_occurrence_contract::ActiveModel {
        occurrence_id: Set(contract.occurrence_id.clone()),
        task_id: Set(contract.task_id.clone()),
        run_id: Set(contract.run_id.clone()),
        trigger_id: Set(contract.trigger_id.clone()),
        occurrence_key: Set(contract.occurrence_key.clone()),
        execution_generation: Set(i64::try_from(contract.execution_generation)
            .context("task occurrence generation overflow")?),
        agent_execution_id: Set(contract.agent_execution_id.clone()),
        work_graph_root_execution_id: Set(contract.work_graph_root_execution_id.clone()),
        root_resource_scope_id: Set(contract.root_resource_scope_id.clone()),
        status: Set(serde_json::to_string(&contract.status)?
            .trim_matches('"')
            .to_owned()),
        queue_position: Set(contract
            .queue_position
            .map(|value| i64::try_from(value))
            .transpose()?),
        retry_attempt: Set(i64::from(contract.retry_attempt)),
        action_idempotency_key: Set(contract.action_idempotency_key.clone()),
        route_id: Set(contract.route_id.clone()),
        result_return_route_id: Set(contract.result_return_route_id.clone()),
        delivery_plan_json: Set(optional_typed_json_to_db(&contract.delivery_plan)?),
        terminal_reason: Set(contract.terminal_reason.clone()),
        created_at: Set(requested_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::column(task_occurrence_contract::Column::OccurrenceId)
            .update_columns([
                task_occurrence_contract::Column::TaskId,
                task_occurrence_contract::Column::RunId,
                task_occurrence_contract::Column::TriggerId,
                task_occurrence_contract::Column::OccurrenceKey,
                task_occurrence_contract::Column::ExecutionGeneration,
                task_occurrence_contract::Column::AgentExecutionId,
                task_occurrence_contract::Column::WorkGraphRootExecutionId,
                task_occurrence_contract::Column::RootResourceScopeId,
                task_occurrence_contract::Column::Status,
                task_occurrence_contract::Column::QueuePosition,
                task_occurrence_contract::Column::RetryAttempt,
                task_occurrence_contract::Column::ActionIdempotencyKey,
                task_occurrence_contract::Column::RouteId,
                task_occurrence_contract::Column::ResultReturnRouteId,
                task_occurrence_contract::Column::DeliveryPlanJson,
                task_occurrence_contract::Column::TerminalReason,
            ])
            .value(
                task_occurrence_contract::Column::UpdatedAt,
                Expr::cust("MAX(task_occurrence_contract.updated_at, excluded.updated_at)"),
            )
            .action_and_where(Expr::cust(
                "excluded.retry_attempt > task_occurrence_contract.retry_attempt \
                 OR (excluded.retry_attempt = task_occurrence_contract.retry_attempt \
                     AND (task_occurrence_contract.status NOT IN ('delivered', 'failed', 'cancelled') \
                         OR excluded.status = task_occurrence_contract.status))",
            ))
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to upsert task occurrence contract")?;
    if result == 0 {
        bail!(
            "task occurrence contract `{}` lost its status/retry fence",
            contract.occurrence_id
        );
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalTaskOccurrenceMismatch {
    pub task_id: String,
    pub run_id: String,
    pub execution_id: String,
    pub run_status: String,
    pub execution_status: String,
    pub occurrence_status: String,
    pub expected_occurrence_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalTaskOccurrenceMismatchPage {
    pub mismatches: Vec<TerminalTaskOccurrenceMismatch>,
    pub scanned_occurrences: usize,
    pub next_cursor: Option<String>,
}

const TERMINAL_OCCURRENCE_SCAN_HARD_LIMIT: u64 = 512;

/// Scans a bounded primary-key page, then checks only those occurrence ids for
/// an exact terminal authority chain. The cursor advances across poison or
/// ambiguous rows, so either can be skipped without starving later records.
pub async fn scan_terminal_task_occurrence_mismatches<C: ConnectionTrait>(
    db: &C,
    after_occurrence_id: Option<&str>,
    limit: u64,
) -> Result<TerminalTaskOccurrenceMismatchPage> {
    if limit == 0 {
        return Ok(TerminalTaskOccurrenceMismatchPage {
            mismatches: Vec::new(),
            scanned_occurrences: 0,
            next_cursor: None,
        });
    }
    let scan_limit = limit.min(TERMINAL_OCCURRENCE_SCAN_HARD_LIMIT);
    let mut occurrence_query = task_occurrence_contract::Entity::find()
        .select_only()
        .column(task_occurrence_contract::Column::OccurrenceId)
        .order_by_asc(task_occurrence_contract::Column::OccurrenceId)
        .limit(scan_limit.saturating_add(1));
    if let Some(after_occurrence_id) = after_occurrence_id {
        occurrence_query = occurrence_query.filter(
            task_occurrence_contract::Column::OccurrenceId.gt(after_occurrence_id.to_owned()),
        );
    }
    let mut occurrence_ids = occurrence_query
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to scan bounded Task occurrence page")?;
    let page_size = usize::try_from(scan_limit).expect("hard scan limit fits usize");
    let next_cursor = if occurrence_ids.len() > page_size {
        occurrence_ids.truncate(page_size);
        occurrence_ids.last().cloned()
    } else {
        None
    };
    let scanned_occurrences = occurrence_ids.len();
    let mismatches = list_terminal_task_occurrence_mismatches_for_ids(db, &occurrence_ids).await?;
    Ok(TerminalTaskOccurrenceMismatchPage {
        mismatches,
        scanned_occurrences,
        next_cursor,
    })
}

/// Completes an explicit diagnostic scan through bounded primary-key pages.
/// Periodic maintenance must call `scan_terminal_task_occurrence_mismatches`
/// once per quantum and retain its cursor instead of using this full scan.
pub async fn list_terminal_task_occurrence_mismatches<C: ConnectionTrait>(
    db: &C,
    limit: u64,
) -> Result<Vec<TerminalTaskOccurrenceMismatch>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut after_occurrence_id = None;
    let mut mismatches = Vec::new();
    loop {
        let page = scan_terminal_task_occurrence_mismatches(
            db,
            after_occurrence_id.as_deref(),
            TERMINAL_OCCURRENCE_SCAN_HARD_LIMIT,
        )
        .await?;
        mismatches.extend(page.mismatches);
        if u64::try_from(mismatches.len()).unwrap_or(u64::MAX) >= limit {
            mismatches.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
            break;
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        after_occurrence_id = Some(next_cursor);
    }
    Ok(mismatches)
}

async fn list_terminal_task_occurrence_mismatches_for_ids<C: ConnectionTrait>(
    db: &C,
    occurrence_ids: &[String],
) -> Result<Vec<TerminalTaskOccurrenceMismatch>> {
    if occurrence_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = vec!["?"; occurrence_ids.len()].join(", ");
    let sql = format!(
        r#"SELECT occurrence.task_id AS task_id,
                  occurrence.run_id AS run_id,
                  run_execution.id AS execution_id,
                  run.status AS run_status,
                  run_execution.status AS execution_status,
                  occurrence.status AS occurrence_status,
                  CASE run.status
                      WHEN 'succeeded' THEN 'delivered'
                      WHEN 'cancelled' THEN 'cancelled'
                      ELSE 'failed'
                  END AS expected_occurrence_status
           FROM task_occurrence_contract occurrence
           INNER JOIN task_run run ON run.id = occurrence.run_id
           INNER JOIN task task_row ON task_row.id = run.task_id
           INNER JOIN task_run_execution run_execution
                   ON run_execution.task_run_id = run.id
           LEFT JOIN agent_execution agent ON agent.id = run_execution.id
           WHERE occurrence.occurrence_id IN ({placeholders})
             AND occurrence.task_id = run.task_id
             AND task_row.executor_kind = run.executor_kind
             AND run_execution.task_id = run.task_id
             AND run_execution.executor_kind = run.executor_kind
             AND run.completed_at IS NOT NULL
             AND run_execution.completed_at IS NOT NULL
             AND ((run.status = 'succeeded' AND run_execution.status = 'succeeded')
               OR (run.status = 'failed' AND run_execution.status = 'failed')
               OR (run.status = 'blocked' AND run_execution.status = 'blocked')
               OR (run.status = 'timed_out' AND run_execution.status = 'timed_out')
               OR (run.status = 'cancelled' AND run_execution.status = 'cancelled'))
             AND ((run_execution.executor_kind = 'agent'
                   AND occurrence.agent_execution_id = run_execution.id
                   AND agent.id = run_execution.id
                   AND agent.workspace_id = task_row.workspace_id
                   AND agent.parent_task_id = run.task_id
                   AND agent.execution_generation = occurrence.execution_generation
                   AND agent.status = run_execution.status
                   AND agent.finished_at IS NOT NULL
                   AND occurrence.work_graph_root_execution_id = agent.work_graph_root_execution_id
                   AND occurrence.root_resource_scope_id = agent.work_graph_root_execution_id)
               OR (run_execution.executor_kind = 'system'
                   AND occurrence.agent_execution_id IS NULL
                   AND occurrence.work_graph_root_execution_id IS NULL
                   AND occurrence.root_resource_scope_id IS NULL))
             AND occurrence.status <> CASE run.status
                     WHEN 'succeeded' THEN 'delivered'
                     WHEN 'cancelled' THEN 'cancelled'
                     ELSE 'failed'
                 END
           ORDER BY occurrence.occurrence_id ASC"#,
    );
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            occurrence_ids
                .iter()
                .cloned()
                .map(sea_orm::Value::from)
                .collect::<Vec<_>>(),
        ))
        .await
        .context("failed to list terminal Task occurrence mismatches")?;
    rows.into_iter()
        .map(|row| {
            Ok(TerminalTaskOccurrenceMismatch {
                task_id: row.try_get("", "task_id")?,
                run_id: row.try_get("", "run_id")?,
                execution_id: row.try_get("", "execution_id")?,
                run_status: row.try_get("", "run_status")?,
                execution_status: row.try_get("", "execution_status")?,
                occurrence_status: row.try_get("", "occurrence_status")?,
                expected_occurrence_status: row.try_get("", "expected_occurrence_status")?,
            })
        })
        .collect()
}

/// Explicit repair path for an occurrence whose exact terminal authorities
/// were revalidated by the caller in the same transaction. The optimistic
/// fence prevents a concurrent retry or actor rebinding from being repaired.
pub(crate) async fn repair_terminal_task_occurrence_status<C: ConnectionTrait>(
    db: &C,
    current: &task_occurrence_contract::Model,
    expected_status: TaskOccurrenceStatus,
    now: i64,
) -> Result<bool> {
    if !is_terminal_task_occurrence_status(&expected_status) {
        bail!("terminal Task occurrence repair requires a terminal status");
    }
    let mut update = task_occurrence_contract::Entity::update_many()
        .filter(task_occurrence_contract::Column::OccurrenceId.eq(current.occurrence_id.clone()))
        .filter(task_occurrence_contract::Column::TaskId.eq(current.task_id.clone()))
        .filter(task_occurrence_contract::Column::RunId.eq(current.run_id.clone()))
        .filter(
            task_occurrence_contract::Column::ExecutionGeneration.eq(current.execution_generation),
        )
        .filter(task_occurrence_contract::Column::RetryAttempt.eq(current.retry_attempt))
        .filter(task_occurrence_contract::Column::Status.eq(current.status.clone()))
        .filter(
            task_occurrence_contract::Column::ActionIdempotencyKey
                .eq(current.action_idempotency_key.clone()),
        )
        .filter(task_occurrence_contract::Column::UpdatedAt.eq(current.updated_at))
        .col_expr(
            task_occurrence_contract::Column::Status,
            Expr::value(task_occurrence_status_to_db(&expected_status)),
        )
        .col_expr(
            task_occurrence_contract::Column::UpdatedAt,
            Expr::value(unix_to_datetime(now)),
        );
    update = match current.agent_execution_id.as_ref() {
        Some(execution_id) => update
            .filter(task_occurrence_contract::Column::AgentExecutionId.eq(execution_id.clone())),
        None => update.filter(task_occurrence_contract::Column::AgentExecutionId.is_null()),
    };
    update = match current.work_graph_root_execution_id.as_ref() {
        Some(root_execution_id) => update.filter(
            task_occurrence_contract::Column::WorkGraphRootExecutionId
                .eq(root_execution_id.clone()),
        ),
        None => update.filter(task_occurrence_contract::Column::WorkGraphRootExecutionId.is_null()),
    };
    update = match current.root_resource_scope_id.as_ref() {
        Some(scope_id) => update
            .filter(task_occurrence_contract::Column::RootResourceScopeId.eq(scope_id.clone())),
        None => update.filter(task_occurrence_contract::Column::RootResourceScopeId.is_null()),
    };
    let result = update
        .exec(db)
        .await
        .context("failed to repair terminal Task occurrence status")?;
    Ok(result.rows_affected == 1)
}

#[derive(Debug, Clone)]
pub struct PreparedTaskOccurrenceRetry {
    occurrence_id: String,
    task_id: String,
    run_id: String,
    action_idempotency_key: String,
    expected_status: String,
    expected_retry_attempt: i64,
    status: String,
    retry_attempt: i64,
    queue_position: Option<i64>,
    terminal_reason: Option<String>,
    updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

pub fn prepare_task_occurrence_retry(
    current: &TaskOccurrenceContract,
    next: &TaskOccurrenceContract,
    now: i64,
) -> Result<PreparedTaskOccurrenceRetry> {
    current
        .validate()
        .map_err(|error| anyhow!("invalid current task occurrence contract: {error:?}"))?;
    next.validate()
        .map_err(|error| anyhow!("invalid resumed task occurrence contract: {error:?}"))?;
    validate_occurrence_update(current, next)?;
    if current.run_id != next.run_id {
        bail!("a resumed task occurrence cannot change its run");
    }

    Ok(PreparedTaskOccurrenceRetry {
        occurrence_id: next.occurrence_id.clone(),
        task_id: next.task_id.clone(),
        run_id: next.run_id.clone(),
        action_idempotency_key: next.action_idempotency_key.clone(),
        expected_status: serde_json::to_string(&current.status)?
            .trim_matches('"')
            .to_owned(),
        expected_retry_attempt: i64::from(current.retry_attempt),
        status: serde_json::to_string(&next.status)?
            .trim_matches('"')
            .to_owned(),
        retry_attempt: i64::from(next.retry_attempt),
        queue_position: next
            .queue_position
            .map(i64::try_from)
            .transpose()
            .context("task occurrence queue position overflow")?,
        terminal_reason: next.terminal_reason.clone(),
        updated_at: unix_to_datetime(now),
    })
}

/// Applies a prevalidated occurrence retry using an optimistic fence. All
/// protocol validation and enum serialization happen before the writer is
/// acquired.
pub async fn apply_prepared_task_occurrence_retry<C: ConnectionTrait>(
    db: &C,
    prepared: &PreparedTaskOccurrenceRetry,
) -> Result<bool> {
    let result = task_occurrence_contract::Entity::update_many()
        .filter(task_occurrence_contract::Column::OccurrenceId.eq(prepared.occurrence_id.clone()))
        .filter(task_occurrence_contract::Column::TaskId.eq(prepared.task_id.clone()))
        .filter(task_occurrence_contract::Column::RunId.eq(prepared.run_id.clone()))
        .filter(
            task_occurrence_contract::Column::ActionIdempotencyKey
                .eq(prepared.action_idempotency_key.clone()),
        )
        .filter(task_occurrence_contract::Column::Status.eq(prepared.expected_status.clone()))
        .filter(task_occurrence_contract::Column::RetryAttempt.eq(prepared.expected_retry_attempt))
        .col_expr(
            task_occurrence_contract::Column::Status,
            sea_orm::sea_query::Expr::value(prepared.status.clone()),
        )
        .col_expr(
            task_occurrence_contract::Column::RetryAttempt,
            sea_orm::sea_query::Expr::value(prepared.retry_attempt),
        )
        .col_expr(
            task_occurrence_contract::Column::QueuePosition,
            sea_orm::sea_query::Expr::value(prepared.queue_position),
        )
        .col_expr(
            task_occurrence_contract::Column::TerminalReason,
            sea_orm::sea_query::Expr::value(prepared.terminal_reason.clone()),
        )
        .col_expr(
            task_occurrence_contract::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(prepared.updated_at),
        )
        .exec(db)
        .await
        .context("failed to apply prepared task occurrence retry")?;
    Ok(result.rows_affected == 1)
}

fn validate_occurrence_update(
    persisted: &TaskOccurrenceContract,
    candidate: &TaskOccurrenceContract,
) -> Result<()> {
    if persisted.occurrence_id != candidate.occurrence_id
        || persisted.task_id != candidate.task_id
        || persisted.trigger_id != candidate.trigger_id
        || persisted.occurrence_key != candidate.occurrence_key
        || persisted.execution_generation != candidate.execution_generation
        || persisted.action_idempotency_key != candidate.action_idempotency_key
        || persisted.route_id != candidate.route_id
        || persisted.result_return_route_id != candidate.result_return_route_id
        || persisted.delivery_plan != candidate.delivery_plan
    {
        bail!(
            "task occurrence contract `{}` attempts to rewrite immutable actor/action facts",
            candidate.occurrence_id
        );
    }
    if candidate.retry_attempt < persisted.retry_attempt {
        bail!(
            "task occurrence contract `{}` attempts to regress its retry attempt",
            candidate.occurrence_id
        );
    }
    if candidate.retry_attempt == persisted.retry_attempt
        && is_terminal_task_occurrence_status(&persisted.status)
        && candidate.status != persisted.status
    {
        bail!(
            "task occurrence contract `{}` attempts to leave terminal status without a newer retry",
            candidate.occurrence_id
        );
    }
    if persisted.run_id != candidate.run_id && candidate.retry_attempt <= persisted.retry_attempt {
        bail!(
            "task occurrence contract `{}` can change run only for a newer retry",
            candidate.occurrence_id
        );
    }
    for (field, persisted_value, candidate_value) in [
        (
            "work graph root",
            persisted.work_graph_root_execution_id.as_deref(),
            candidate.work_graph_root_execution_id.as_deref(),
        ),
        (
            "root resource scope",
            persisted.root_resource_scope_id.as_deref(),
            candidate.root_resource_scope_id.as_deref(),
        ),
    ] {
        if persisted_value.is_some() && persisted_value != candidate_value {
            bail!(
                "task occurrence contract `{}` attempts to rewrite its {field}",
                candidate.occurrence_id
            );
        }
    }
    Ok(())
}

pub(crate) const fn is_terminal_task_occurrence_status(status: &TaskOccurrenceStatus) -> bool {
    matches!(
        status,
        TaskOccurrenceStatus::Delivered
            | TaskOccurrenceStatus::Failed
            | TaskOccurrenceStatus::Cancelled
    )
}

pub(crate) const fn task_occurrence_status_to_db(status: &TaskOccurrenceStatus) -> &'static str {
    match status {
        TaskOccurrenceStatus::Dormant => "dormant",
        TaskOccurrenceStatus::Queued => "queued",
        TaskOccurrenceStatus::Recovering => "recovering",
        TaskOccurrenceStatus::Running => "running",
        TaskOccurrenceStatus::WaitingReview => "waiting_review",
        TaskOccurrenceStatus::Delivered => "delivered",
        TaskOccurrenceStatus::Failed => "failed",
        TaskOccurrenceStatus::Cancelled => "cancelled",
    }
}

pub async fn find_task_occurrence_contract<C: ConnectionTrait>(
    db: &C,
    occurrence_id: &str,
) -> Result<Option<TaskOccurrenceContract>> {
    let Some(row) = task_occurrence_contract::Entity::find_by_id(occurrence_id.to_owned())
        .one(db)
        .await
        .context("failed to load task occurrence contract")?
    else {
        return Ok(None);
    };
    Ok(Some(task_occurrence_contract_from_model(row)?))
}

pub async fn find_task_occurrence_by_run_id<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Option<TaskOccurrenceContract>> {
    let Some(row) = task_occurrence_contract::Entity::find()
        .filter(task_occurrence_contract::Column::RunId.eq(run_id.to_owned()))
        .one(db)
        .await
        .context("failed to load task occurrence contract by run")?
    else {
        return Ok(None);
    };
    Ok(Some(task_occurrence_contract_from_model(row)?))
}

pub async fn list_task_occurrences<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    limit: u64,
) -> Result<Vec<TaskOccurrenceContract>> {
    if limit == 0 || limit > 200 {
        bail!("Task occurrence batch exceeds its bounded limit");
    }
    let rows = task_occurrence_contract::Entity::find()
        .filter(task_occurrence_contract::Column::TaskId.eq(task_id.to_owned()))
        .order_by_desc(task_occurrence_contract::Column::CreatedAt)
        .order_by_desc(task_occurrence_contract::Column::OccurrenceId)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list task occurrence contracts")?;
    rows.into_iter()
        .map(task_occurrence_contract_from_model)
        .collect()
}

pub async fn next_task_occurrence_execution_generation<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<u64> {
    let current = task_occurrence_contract::Entity::find()
        .filter(task_occurrence_contract::Column::TaskId.eq(task_id.to_owned()))
        .order_by_desc(task_occurrence_contract::Column::ExecutionGeneration)
        .limit(1)
        .one(db)
        .await
        .context("failed to load latest Task occurrence generation")?
        .map(|row| {
            u64::try_from(row.execution_generation)
                .context("persisted Task occurrence generation is invalid")
        })
        .transpose()?
        .unwrap_or(0);
    current
        .checked_add(1)
        .context("Task occurrence execution generation overflow")
}

pub async fn task_occurrence_matches_execution_or_graph<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    execution_id: &str,
    work_graph_root_execution_id: &str,
) -> Result<bool> {
    task_occurrence_contract::Entity::find()
        .filter(task_occurrence_contract::Column::TaskId.eq(task_id.to_owned()))
        .filter(
            Condition::any()
                .add(task_occurrence_contract::Column::AgentExecutionId.eq(execution_id.to_owned()))
                .add(
                    task_occurrence_contract::Column::WorkGraphRootExecutionId
                        .eq(work_graph_root_execution_id.to_owned()),
                ),
        )
        .limit(1)
        .one(db)
        .await
        .context("failed to resolve Task occurrence execution ownership")
        .map(|row| row.is_some())
}

pub(crate) async fn upsert_task_delivery_authority<C: ConnectionTrait>(
    db: &C,
    delivery_id: &str,
    task_id: &str,
    run_id: &str,
    author_json: &str,
    reviewer_json: Option<&str>,
    destination_route_id: Option<&str>,
    route_receipt_json: Option<&str>,
    disclosure_generation: u64,
    idempotency_key: &str,
    status: &str,
    now: i64,
) -> Result<()> {
    let prepared = prepare_task_delivery_authority(
        delivery_id,
        task_id,
        run_id,
        author_json,
        reviewer_json,
        destination_route_id,
        route_receipt_json,
        disclosure_generation,
        idempotency_key,
        status,
        now,
    )?;
    upsert_prepared_task_delivery_authority(db, prepared).await
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedTaskDeliveryAuthority {
    delivery_id: String,
    task_id: String,
    run_id: String,
    author_json: String,
    reviewer_json: Option<String>,
    destination_route_id: Option<String>,
    route_receipt_json: Option<String>,
    disclosure_generation: i64,
    idempotency_key: String,
    status: String,
    now: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_task_delivery_authority(
    delivery_id: &str,
    task_id: &str,
    run_id: &str,
    author_json: &str,
    reviewer_json: Option<&str>,
    destination_route_id: Option<&str>,
    route_receipt_json: Option<&str>,
    disclosure_generation: u64,
    idempotency_key: &str,
    status: &str,
    now: i64,
) -> Result<PreparedTaskDeliveryAuthority> {
    if delivery_id.trim().is_empty() || task_id.trim().is_empty() || run_id.trim().is_empty() {
        bail!("task delivery authority ids must not be empty");
    }
    if author_json.trim().is_empty()
        || idempotency_key.trim().is_empty()
        || disclosure_generation == 0
        || !matches!(
            status,
            "pending" | "delivering" | "delivered" | "failed" | "cancelled"
        )
    {
        bail!("task delivery authority author and idempotency key are required");
    }
    serde_json::from_str::<PersistedActorRef>(author_json)
        .context("task delivery authority author is invalid")?;
    reviewer_json
        .map(serde_json::from_str::<TaskResultReviewerRef>)
        .transpose()
        .context("task delivery authority reviewer is invalid")?;
    if destination_route_id.is_some() != route_receipt_json.is_some() {
        bail!("task delivery authority has incomplete exact route facts");
    }
    Ok(PreparedTaskDeliveryAuthority {
        delivery_id: delivery_id.to_owned(),
        task_id: task_id.to_owned(),
        run_id: run_id.to_owned(),
        author_json: author_json.to_owned(),
        reviewer_json: reviewer_json.map(str::to_owned),
        destination_route_id: destination_route_id.map(str::to_owned),
        route_receipt_json: route_receipt_json.map(str::to_owned),
        disclosure_generation: i64::try_from(disclosure_generation)
            .context("delivery disclosure generation overflow")?,
        idempotency_key: idempotency_key.to_owned(),
        status: status.to_owned(),
        now: unix_to_datetime(now),
    })
}

pub(crate) async fn upsert_prepared_task_delivery_authority<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTaskDeliveryAuthority,
) -> Result<()> {
    task_delivery_authority::Entity::insert(task_delivery_authority::ActiveModel {
        delivery_id: Set(prepared.delivery_id.clone()),
        task_id: Set(prepared.task_id.clone()),
        run_id: Set(prepared.run_id.clone()),
        author_json: Set(prepared.author_json.clone()),
        reviewer_json: Set(prepared.reviewer_json.clone()),
        destination_route_id: Set(prepared.destination_route_id.clone()),
        route_receipt_json: Set(prepared.route_receipt_json.clone()),
        disclosure_generation: Set(prepared.disclosure_generation),
        idempotency_key: Set(prepared.idempotency_key.clone()),
        status: Set(prepared.status.clone()),
        created_at: Set(prepared.now),
        updated_at: Set(prepared.now),
    })
    .on_conflict(
        OnConflict::column(task_delivery_authority::Column::DeliveryId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to insert task delivery authority")?;
    let persisted = task_delivery_authority::Entity::find_by_id(prepared.delivery_id.clone())
        .one(db)
        .await
        .context("failed to reload task delivery authority")?
        .context("task delivery authority disappeared after insert")?;
    if persisted.task_id != prepared.task_id
        || persisted.run_id != prepared.run_id
        || persisted.author_json != prepared.author_json
        || persisted.reviewer_json != prepared.reviewer_json
        || persisted.destination_route_id != prepared.destination_route_id
        || persisted.route_receipt_json != prepared.route_receipt_json
        || persisted.disclosure_generation != prepared.disclosure_generation
        || persisted.idempotency_key != prepared.idempotency_key
    {
        bail!("task delivery authority attempts to rewrite immutable actor/route facts");
    }
    let status = prepared.status.as_str();
    let transition_allowed = match persisted.status.as_str() {
        "pending" => matches!(status, "pending" | "delivering" | "cancelled"),
        "delivering" => matches!(
            status,
            "delivering" | "pending" | "delivered" | "failed" | "cancelled"
        ),
        "delivered" => status == "delivered",
        "failed" => status == "failed",
        "cancelled" => status == "cancelled",
        _ => false,
    };
    if !transition_allowed {
        bail!(
            "task delivery authority cannot transition from `{}` to `{status}`",
            persisted.status
        );
    }
    if persisted.status != status {
        let update = task_delivery_authority::Entity::update_many()
            .col_expr(
                task_delivery_authority::Column::Status,
                sea_orm::sea_query::Expr::value(prepared.status.clone()),
            )
            .col_expr(
                task_delivery_authority::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(prepared.now),
            )
            .filter(task_delivery_authority::Column::DeliveryId.eq(prepared.delivery_id.clone()))
            .filter(task_delivery_authority::Column::Status.eq(persisted.status.clone()))
            .exec(db)
            .await
            .context("failed to advance task delivery authority status")?;
        if update.rows_affected != 1 {
            let current = task_delivery_authority::Entity::find_by_id(prepared.delivery_id.clone())
                .one(db)
                .await
                .context("failed to recheck concurrent task delivery authority transition")?;
            if current.as_ref().map(|row| row.status.as_str()) != Some(status) {
                bail!("task delivery authority changed concurrently");
            }
        }
    }
    Ok(())
}

pub(super) fn task_actor_contract_from_model(
    row: task_actor_contract::Model,
) -> Result<TaskActorContract> {
    let (_, contract) = upgrade_task_actor_contract_model_and_parse(row)?;
    Ok(contract)
}

/// Upcasts a stored Task actor contract in memory and validates the complete
/// row against the current domain contract. Background maintenance may persist
/// the returned model; ordinary CRUD reads use the same path without writing.
pub fn upgrade_task_actor_contract_model_to_current(
    row: task_actor_contract::Model,
) -> Result<task_actor_contract::Model> {
    let (row, _) = upgrade_task_actor_contract_model_and_parse(row)?;
    Ok(row)
}

fn upgrade_task_actor_contract_model_and_parse(
    mut row: task_actor_contract::Model,
) -> Result<(task_actor_contract::Model, TaskActorContract)> {
    let derived_child_launch_grant_json = row
        .derived_child_launch_grant_json
        .as_deref()
        .map(pioneer_protocol::migrate_task_derived_child_launch_grant_json_to_current)
        .transpose()
        .with_context(|| {
            format!(
                "failed to upcast child launch grant for Task `{}`",
                row.task_id
            )
        })?;
    row.derived_child_launch_grant_json = derived_child_launch_grant_json;
    let contract = task_actor_contract_from_current_model(row.clone())?;
    Ok((row, contract))
}

fn task_actor_contract_from_current_model(
    row: task_actor_contract::Model,
) -> Result<TaskActorContract> {
    let contract = TaskActorContract {
        task_id: row.task_id,
        workspace_id: row.workspace_id,
        creator: serde_json::from_str(&row.creator_json)?,
        creator_presentation_snapshot: row
            .creator_snapshot_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        reviewer: serde_json::from_str(&row.reviewer_json)?,
        execution_destination_thread_id: row.execution_destination_thread_id,
        execution_route_id: row.execution_route_id,
        execution_route_receipt_json: row.execution_route_receipt_json,
        execution_route_expires_at_millis: row.execution_route_expires_at_millis,
        delivery: serde_json::from_str(&row.delivery_json)?,
        launch: row
            .launch_selection_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        requested_identity_json: row.requested_identity_json,
        resolved_identity_id: row.resolved_identity_id,
        resolved_profile_id: row.resolved_profile_id,
        source_config_fingerprint: row.source_config_fingerprint,
        derived_child_launch_grant_json: row.derived_child_launch_grant_json,
        creator_work_graph_root_execution_id: row.creator_work_graph_root_execution_id,
        work_graph_root_execution_id: row.work_graph_root_execution_id,
        root_resource_scope_id: row.root_resource_scope_id,
        accounting_attribution: row
            .accounting_attribution_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        controller_principal_id: row.controller_principal_id,
        revision: u64::try_from(row.revision).context("task actor revision is negative")?,
    };
    contract
        .validate()
        .map_err(|error| anyhow!("persisted task actor contract is invalid: {error:?}"))?;
    Ok(contract)
}

pub(crate) fn task_occurrence_contract_from_model(
    row: task_occurrence_contract::Model,
) -> Result<TaskOccurrenceContract> {
    let status = match row.status.as_str() {
        "dormant" => TaskOccurrenceStatus::Dormant,
        "queued" => TaskOccurrenceStatus::Queued,
        "recovering" => TaskOccurrenceStatus::Recovering,
        "running" => TaskOccurrenceStatus::Running,
        "waiting_review" => TaskOccurrenceStatus::WaitingReview,
        "delivered" => TaskOccurrenceStatus::Delivered,
        "failed" => TaskOccurrenceStatus::Failed,
        "cancelled" => TaskOccurrenceStatus::Cancelled,
        other => bail!("unknown task occurrence status `{other}`"),
    };
    let contract = TaskOccurrenceContract {
        occurrence_id: row.occurrence_id,
        task_id: row.task_id,
        run_id: row.run_id,
        trigger_id: row.trigger_id,
        occurrence_key: row.occurrence_key,
        execution_generation: u64::try_from(row.execution_generation)
            .context("task occurrence generation is negative")?,
        agent_execution_id: row.agent_execution_id,
        work_graph_root_execution_id: row.work_graph_root_execution_id,
        root_resource_scope_id: row.root_resource_scope_id,
        status,
        queue_position: row.queue_position.map(u64::try_from).transpose()?,
        retry_attempt: u32::try_from(row.retry_attempt)
            .context("task occurrence retry attempt is invalid")?,
        action_idempotency_key: row.action_idempotency_key,
        route_id: row.route_id,
        result_return_route_id: row.result_return_route_id,
        delivery_plan: row
            .delivery_plan_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        terminal_reason: row.terminal_reason,
    };
    contract
        .validate()
        .map_err(|error| anyhow!("persisted task occurrence contract is invalid: {error:?}"))?;
    Ok(contract)
}
