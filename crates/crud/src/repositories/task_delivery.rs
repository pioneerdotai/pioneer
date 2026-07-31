#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::{task_delivery, task_delivery_attempt};
use pioneer_protocol::{TaskDeliveriesParams, TaskDelivery, TaskDeliveryAttempt};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::ExprTrait;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    task_delivery_attempt_status_to_db, task_delivery_mode_to_db, task_delivery_status_to_db,
};
use crate::repositories::task::{TaskRootAccessFilter, accessible_task_ids};
use crate::util::{optional_typed_json_to_db, unix_to_datetime};

const DEFAULT_DELIVERY_LIMIT: u64 = 100;

pub async fn upsert_delivery<C: ConnectionTrait>(db: &C, delivery: &TaskDelivery) -> Result<()> {
    task_delivery::Entity::insert(active_model_from_delivery(delivery)?)
        .on_conflict(
            OnConflict::column(task_delivery::Column::Id)
                .update_columns([
                    task_delivery::Column::WorkspaceId,
                    task_delivery::Column::TaskId,
                    task_delivery::Column::RunId,
                    task_delivery::Column::DeliveryKey,
                    task_delivery::Column::Mode,
                    task_delivery::Column::TargetThreadId,
                    task_delivery::Column::TargetUserId,
                    task_delivery::Column::WebhookUrl,
                    task_delivery::Column::WebhookUrlFingerprint,
                    task_delivery::Column::Status,
                    task_delivery::Column::NextAttemptAt,
                    task_delivery::Column::AttemptCount,
                    task_delivery::Column::MaxAttempts,
                    task_delivery::Column::ResultSnapshotJson,
                    task_delivery::Column::ErrorSnapshotJson,
                    task_delivery::Column::DeliveredTurnId,
                    task_delivery::Column::DeliveredNotificationId,
                    task_delivery::Column::DeliveredAt,
                    task_delivery::Column::LastError,
                    task_delivery::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task delivery")?;
    Ok(())
}

pub async fn upsert_attempt<C: ConnectionTrait>(
    db: &C,
    attempt: &TaskDeliveryAttempt,
) -> Result<()> {
    task_delivery_attempt::Entity::insert(active_model_from_attempt(attempt))
        .on_conflict(
            OnConflict::column(task_delivery_attempt::Column::Id)
                .update_columns([
                    task_delivery_attempt::Column::DeliveryId,
                    task_delivery_attempt::Column::AttemptNumber,
                    task_delivery_attempt::Column::Status,
                    task_delivery_attempt::Column::StartedAt,
                    task_delivery_attempt::Column::CompletedAt,
                    task_delivery_attempt::Column::HttpStatus,
                    task_delivery_attempt::Column::Error,
                    task_delivery_attempt::Column::ResponseFingerprint,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task delivery attempt")?;
    Ok(())
}

pub async fn find_delivery_by_id<C: ConnectionTrait>(
    db: &C,
    delivery_id: &str,
) -> Result<Option<task_delivery::Model>> {
    task_delivery::Entity::find_by_id(delivery_id.to_owned())
        .one(db)
        .await
        .context("failed to query task delivery by id")
}

pub async fn list_due_deliveries<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<task_delivery::Model>> {
    task_delivery::Entity::find()
        .filter(task_delivery::Column::Status.eq("pending"))
        .filter(
            task_delivery::Column::NextAttemptAt
                .is_null()
                .or(task_delivery::Column::NextAttemptAt.lte(now)),
        )
        .order_by_asc(task_delivery::Column::NextAttemptAt)
        .order_by_asc(task_delivery::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list due task deliveries")
}

pub async fn list_stuck_deliveries<C: ConnectionTrait>(
    db: &C,
    before: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<task_delivery::Model>> {
    task_delivery::Entity::find()
        .filter(task_delivery::Column::Status.eq("delivering"))
        .filter(task_delivery::Column::UpdatedAt.lte(before))
        .order_by_asc(task_delivery::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list stuck task deliveries")
}

pub async fn list_deliveries<C: ConnectionTrait>(
    db: &C,
    params: &TaskDeliveriesParams,
) -> Result<Vec<task_delivery::Model>> {
    list_deliveries_scoped(db, params, None).await
}

pub async fn list_deliveries_scoped<C: ConnectionTrait>(
    db: &C,
    params: &TaskDeliveriesParams,
    access: Option<&TaskRootAccessFilter>,
) -> Result<Vec<task_delivery::Model>> {
    let mut query = task_delivery::Entity::find()
        .filter(task_delivery::Column::WorkspaceId.eq(params.workspace_id.clone()))
        .order_by_desc(task_delivery::Column::UpdatedAt)
        .limit(
            params
                .limit
                .map(u64::from)
                .unwrap_or(DEFAULT_DELIVERY_LIMIT),
        );

    if let Some(access) = access {
        query =
            query.filter(task_delivery::Column::TaskId.in_subquery(accessible_task_ids(access)));
    }
    if let Some(task_id) = params.task_id.as_deref() {
        query = query.filter(task_delivery::Column::TaskId.eq(task_id.to_owned()));
    }
    if let Some(run_id) = params.run_id.as_deref() {
        query = query.filter(task_delivery::Column::RunId.eq(run_id.to_owned()));
    }
    if !params.statuses.is_empty() {
        query = query.filter(
            task_delivery::Column::Status.is_in(
                params
                    .statuses
                    .iter()
                    .copied()
                    .map(task_delivery_status_to_db)
                    .map(str::to_owned)
                    .collect::<Vec<_>>(),
            ),
        );
    }

    query
        .all(db)
        .await
        .context("failed to list task deliveries")
}

pub async fn list_deliveries_for_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Vec<task_delivery::Model>> {
    task_delivery::Entity::find()
        .filter(task_delivery::Column::TaskId.eq(task_id.to_owned()))
        .order_by_asc(task_delivery::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list task deliveries by task")
}

pub async fn list_delivered_thread_deliveries_for_tasks<C: ConnectionTrait>(
    db: &C,
    task_ids: &[String],
    target_thread_id: &str,
) -> Result<Vec<task_delivery::Model>> {
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }
    task_delivery::Entity::find()
        .filter(task_delivery::Column::TaskId.is_in(task_ids.to_vec()))
        .filter(task_delivery::Column::TargetThreadId.eq(target_thread_id.to_owned()))
        .filter(task_delivery::Column::Status.eq("delivered"))
        .filter(task_delivery::Column::DeliveredTurnId.is_not_null())
        .order_by_asc(task_delivery::Column::DeliveredAt)
        .order_by_asc(task_delivery::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list delivered task conversation links")
}

pub async fn list_thread_deliveries_by_delivered_turns<C: ConnectionTrait>(
    db: &C,
    target_thread_id: &str,
    delivered_turn_ids: &[String],
) -> Result<Vec<task_delivery::Model>> {
    if delivered_turn_ids.is_empty() {
        return Ok(Vec::new());
    }
    task_delivery::Entity::find()
        .filter(task_delivery::Column::TargetThreadId.eq(target_thread_id.to_owned()))
        .filter(task_delivery::Column::DeliveredTurnId.is_in(delivered_turn_ids.to_vec()))
        .all(db)
        .await
        .context("failed to list task deliveries by delivered turns")
}

pub async fn list_attempts_for_deliveries<C: ConnectionTrait>(
    db: &C,
    delivery_ids: &[String],
) -> Result<Vec<task_delivery_attempt::Model>> {
    if delivery_ids.is_empty() {
        return Ok(Vec::new());
    }
    task_delivery_attempt::Entity::find()
        .filter(task_delivery_attempt::Column::DeliveryId.is_in(delivery_ids.to_vec()))
        .order_by_asc(task_delivery_attempt::Column::StartedAt)
        .all(db)
        .await
        .context("failed to list task delivery attempts")
}

fn active_model_from_delivery(delivery: &TaskDelivery) -> Result<task_delivery::ActiveModel> {
    Ok(task_delivery::ActiveModel {
        id: Set(delivery.id.clone()),
        workspace_id: Set(delivery.workspace_id.clone()),
        task_id: Set(delivery.task_id.clone()),
        run_id: Set(delivery.run_id.clone()),
        delivery_key: Set(delivery.delivery_key.clone()),
        mode: Set(task_delivery_mode_to_db(delivery.mode).to_owned()),
        target_thread_id: Set(delivery.target_thread_id.clone()),
        target_user_id: Set(delivery.target_user_id.clone()),
        webhook_url: Set(delivery.webhook_url.clone()),
        webhook_url_fingerprint: Set(delivery.webhook_url_fingerprint.clone()),
        status: Set(task_delivery_status_to_db(delivery.status).to_owned()),
        next_attempt_at: Set(delivery.next_attempt_at.map(unix_to_datetime)),
        attempt_count: Set(i64::from(delivery.attempt_count)),
        max_attempts: Set(i64::from(delivery.max_attempts)),
        result_snapshot_json: Set(optional_typed_json_to_db(&delivery.result_snapshot)?),
        error_snapshot_json: Set(optional_typed_json_to_db(&delivery.error_snapshot)?),
        delivered_turn_id: Set(delivery.delivered_turn_id.clone()),
        delivered_notification_id: Set(delivery.delivered_notification_id.clone()),
        delivered_at: Set(delivery.delivered_at.map(unix_to_datetime)),
        last_error: Set(delivery.last_error.clone()),
        created_at: Set(unix_to_datetime(delivery.created_at)),
        updated_at: Set(unix_to_datetime(delivery.updated_at)),
    })
}

fn active_model_from_attempt(attempt: &TaskDeliveryAttempt) -> task_delivery_attempt::ActiveModel {
    task_delivery_attempt::ActiveModel {
        id: Set(attempt.id.clone()),
        delivery_id: Set(attempt.delivery_id.clone()),
        attempt_number: Set(i64::from(attempt.attempt_number)),
        status: Set(task_delivery_attempt_status_to_db(attempt.status).to_owned()),
        started_at: Set(unix_to_datetime(attempt.started_at)),
        completed_at: Set(attempt.completed_at.map(unix_to_datetime)),
        http_status: Set(attempt.http_status.map(i64::from)),
        error: Set(attempt.error.clone()),
        response_fingerprint: Set(attempt.response_fingerprint.clone()),
    }
}
