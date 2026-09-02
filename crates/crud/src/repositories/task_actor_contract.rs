//! Persistence for the exact actor/occurrence/delivery facts of agent domain.

use anyhow::{Context, Result, anyhow, bail};
use pioneer_entity::{task_actor_contract, task_delivery_authority, task_occurrence_contract};
use pioneer_protocol::{
    PersistedActorRef, TaskActorContract, TaskOccurrenceContract, TaskOccurrenceStatus,
    TaskResultReviewerRef,
};
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
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
    if let Some(persisted) =
        find_task_occurrence_contract(db, contract.occurrence_id.as_str()).await?
    {
        validate_occurrence_update(&persisted, contract)?;
    }
    task_occurrence_contract::Entity::insert(task_occurrence_contract::ActiveModel {
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
        created_at: Set(unix_to_datetime(now)),
        updated_at: Set(unix_to_datetime(now)),
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
                task_occurrence_contract::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert task occurrence contract")?;
    Ok(())
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

pub(super) fn task_occurrence_contract_from_model(
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
