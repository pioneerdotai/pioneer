use anyhow::{Context, Result, bail};
use pioneer_entity::{task_delivery, task_delivery_attempt};
use pioneer_protocol::{
    TaskDeliveriesParams, TaskDelivery, TaskDeliveryAttempt, TaskDeliveryAttemptStatus,
    TaskDeliveryMode, TaskDeliveryStatus,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::ExprTrait;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    task_delivery_attempt_status_to_db, task_delivery_mode_to_db, task_delivery_status_to_db,
    task_delivery_thread_target_to_db,
};
use crate::repositories::task::{TaskRootAccessFilter, accessible_task_ids};
use crate::util::{optional_typed_json_to_db, unix_to_datetime};

const DEFAULT_DELIVERY_LIMIT: u64 = 100;
const MAX_DELIVERY_LIST_LIMIT: u64 = 1_000;
const MAX_DELIVERY_BATCH_IDS: usize = 512;
const MAX_DELIVERY_ATTEMPTS: u32 = 16;

// Preserve the notification helper's latest-100 window without loading snapshots
// or the (unbounded) attempt history of those deliveries.
pub async fn pending_thread_targets<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    task_id: &str,
    run_id: &str,
) -> Result<Vec<(String, Option<String>)>> {
    Ok(task_delivery::Entity::find()
        .select_only()
        .columns([
            task_delivery::Column::Mode,
            task_delivery::Column::TargetThreadId,
        ])
        .filter(task_delivery::Column::WorkspaceId.eq(workspace_id))
        .filter(task_delivery::Column::TaskId.eq(task_id))
        .filter(task_delivery::Column::RunId.eq(run_id))
        .filter(sea_orm::sea_query::Expr::cust(
            "status IN ('pending','delivering')",
        ))
        .order_by_desc(task_delivery::Column::UpdatedAt)
        .limit(100)
        .into_tuple::<(String, Option<String>)>()
        .all(db)
        .await?)
}

#[derive(Clone, Debug)]
pub struct PreparedTaskDeliveryProjection {
    delivery: TaskDelivery,
    active_model: task_delivery::ActiveModel,
    result_snapshot_json: Option<String>,
    error_snapshot_json: Option<String>,
    persisted: Option<task_delivery::Model>,
}

impl PreparedTaskDeliveryProjection {
    pub(crate) fn delivery_key(&self) -> &str {
        &self.delivery.delivery_key
    }
}

#[derive(Clone, Debug)]
pub struct PreparedTaskDeliveryAttemptProjection {
    attempt: TaskDeliveryAttempt,
    active_model: task_delivery_attempt::ActiveModel,
    persisted: Option<task_delivery_attempt::Model>,
    insert_new: bool,
}

pub fn prepare_delivery_projection(
    delivery: &TaskDelivery,
) -> Result<PreparedTaskDeliveryProjection> {
    let result_snapshot_json = optional_typed_json_to_db(&delivery.result_snapshot)?;
    let error_snapshot_json = optional_typed_json_to_db(&delivery.error_snapshot)?;
    Ok(PreparedTaskDeliveryProjection {
        delivery: delivery.clone(),
        active_model: active_model_from_delivery_json(
            delivery,
            result_snapshot_json.clone(),
            error_snapshot_json.clone(),
        ),
        result_snapshot_json,
        error_snapshot_json,
        persisted: None,
    })
}

pub fn prepare_attempt_projection(
    attempt: &TaskDeliveryAttempt,
) -> PreparedTaskDeliveryAttemptProjection {
    PreparedTaskDeliveryAttemptProjection {
        attempt: attempt.clone(),
        active_model: active_model_from_attempt(attempt),
        persisted: None,
        insert_new: false,
    }
}

pub async fn upsert_prepared_delivery<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTaskDeliveryProjection,
) -> Result<()> {
    let delivery = &prepared.delivery;
    let persisted = if let Some(persisted) = prepared.persisted {
        persisted
    } else {
        task_delivery::Entity::insert(prepared.active_model)
            .on_conflict(
                OnConflict::column(task_delivery::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(db)
            .await
            .context("failed to insert task delivery")?;
        find_delivery_by_id(db, &delivery.id)
            .await?
            .context("task delivery disappeared after insert")?
    };
    validate_delivery_update(
        &persisted,
        delivery,
        prepared.result_snapshot_json.as_deref(),
        prepared.error_snapshot_json.as_deref(),
    )?;
    let desired_status = task_delivery_status_to_db(delivery.status);
    let desired_attempt_count = i64::from(delivery.attempt_count);
    if persisted.status != desired_status
        || persisted
            .next_attempt_at
            .as_ref()
            .map(|value| value.timestamp())
            != delivery.next_attempt_at
        || persisted.attempt_count != desired_attempt_count
        || persisted.delivered_turn_id != delivery.delivered_turn_id
        || persisted.delivered_notification_id != delivery.delivered_notification_id
        || persisted
            .delivered_at
            .as_ref()
            .map(|value| value.timestamp())
            != delivery.delivered_at
        || persisted.last_error != delivery.last_error
        || persisted.updated_at.timestamp() != delivery.updated_at
    {
        let update = task_delivery::Entity::update_many()
            .col_expr(
                task_delivery::Column::Status,
                sea_orm::sea_query::Expr::value(desired_status.to_owned()),
            )
            .col_expr(
                task_delivery::Column::NextAttemptAt,
                sea_orm::sea_query::Expr::value(delivery.next_attempt_at.map(unix_to_datetime)),
            )
            .col_expr(
                task_delivery::Column::AttemptCount,
                sea_orm::sea_query::Expr::value(desired_attempt_count),
            )
            .col_expr(
                task_delivery::Column::DeliveredTurnId,
                sea_orm::sea_query::Expr::value(delivery.delivered_turn_id.clone()),
            )
            .col_expr(
                task_delivery::Column::DeliveredNotificationId,
                sea_orm::sea_query::Expr::value(delivery.delivered_notification_id.clone()),
            )
            .col_expr(
                task_delivery::Column::DeliveredAt,
                sea_orm::sea_query::Expr::value(delivery.delivered_at.map(unix_to_datetime)),
            )
            .col_expr(
                task_delivery::Column::LastError,
                sea_orm::sea_query::Expr::value(delivery.last_error.clone()),
            )
            .col_expr(
                task_delivery::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(unix_to_datetime(delivery.updated_at)),
            )
            .filter(task_delivery::Column::Id.eq(delivery.id.clone()))
            .filter(task_delivery::Column::Status.eq(persisted.status.clone()))
            .filter(task_delivery::Column::AttemptCount.eq(persisted.attempt_count))
            .exec(db)
            .await
            .context("failed to advance task delivery")?;
        if update.rows_affected != 1 {
            bail!("task delivery changed concurrently");
        }
    }
    Ok(())
}

pub async fn upsert_prepared_attempt<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTaskDeliveryAttemptProjection,
) -> Result<()> {
    let attempt = &prepared.attempt;
    if prepared.insert_new {
        validate_attempt_fields(attempt)?;
        if attempt.status != TaskDeliveryAttemptStatus::Started {
            bail!("new delivery attempt is not started");
        }
        // The lifecycle preflight proved absence of the next exact number.
        // Plain INSERT preserves id/number collision errors, so no post-insert
        // SELECT or no-op UPSERT is necessary on this path.
        task_delivery_attempt::Entity::insert(prepared.active_model)
            .exec_without_returning(db)
            .await
            .context("failed to insert owned new task delivery attempt")?;
        return Ok(());
    }

    let persisted = if let Some(persisted) = prepared.persisted {
        persisted
    } else {
        task_delivery_attempt::Entity::insert(prepared.active_model)
            .on_conflict(
                OnConflict::column(task_delivery_attempt::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(db)
            .await
            .context("failed to insert task delivery attempt")?;
        task_delivery_attempt::Entity::find_by_id(attempt.id.clone())
            .one(db)
            .await
            .context("failed to reload task delivery attempt")?
            .context("task delivery attempt disappeared after insert")?
    };
    validate_attempt_update(&persisted, attempt)?;
    let desired_status = task_delivery_attempt_status_to_db(attempt.status);
    if persisted.status != desired_status
        || persisted
            .completed_at
            .as_ref()
            .map(|value| value.timestamp())
            != attempt.completed_at
        || persisted.http_status != attempt.http_status.map(i64::from)
        || persisted.error != attempt.error
        || persisted.response_fingerprint != attempt.response_fingerprint
    {
        let update = task_delivery_attempt::Entity::update_many()
            .col_expr(
                task_delivery_attempt::Column::Status,
                sea_orm::sea_query::Expr::value(desired_status.to_owned()),
            )
            .col_expr(
                task_delivery_attempt::Column::CompletedAt,
                sea_orm::sea_query::Expr::value(attempt.completed_at.map(unix_to_datetime)),
            )
            .col_expr(
                task_delivery_attempt::Column::HttpStatus,
                sea_orm::sea_query::Expr::value(attempt.http_status.map(i64::from)),
            )
            .col_expr(
                task_delivery_attempt::Column::Error,
                sea_orm::sea_query::Expr::value(attempt.error.clone()),
            )
            .col_expr(
                task_delivery_attempt::Column::ResponseFingerprint,
                sea_orm::sea_query::Expr::value(attempt.response_fingerprint.clone()),
            )
            .filter(task_delivery_attempt::Column::Id.eq(attempt.id.clone()))
            .filter(task_delivery_attempt::Column::Status.eq(persisted.status.clone()))
            .exec(db)
            .await
            .context("failed to advance task delivery attempt")?;
        if update.rows_affected != 1 {
            bail!("task delivery attempt changed concurrently");
        }
    }
    Ok(())
}

// These cached rows are attached only after a point read in the same writer
// transaction. General event ingestion continues to load and validate its own rows.
impl PreparedTaskDeliveryProjection {
    pub(crate) fn with_persisted(mut self, row: task_delivery::Model) -> Self {
        self.persisted = Some(row);
        self
    }

    pub(crate) fn validate_start_fields(&self) -> Result<()> {
        // A terminal delivery at its budget still gets a writer preflight.
        // Its prepared next number is never inserted: only an eligible Pending
        // row with remaining budget can grant that number inside the writer.
        if self.delivery.max_attempts == 0
            || self.delivery.max_attempts > MAX_DELIVERY_ATTEMPTS
            || self.delivery.attempt_count > self.delivery.max_attempts.saturating_add(1)
        {
            bail!("delivery start has an invalid attempt budget");
        }
        validate_delivery_receipt_fields(&self.delivery)
    }

    pub(crate) fn validate_fields(&self) -> Result<()> {
        validate_delivery_fields(&self.delivery)
    }

    pub(crate) fn validate_identity(&self, row: &task_delivery::Model) -> Result<()> {
        validate_delivery_identity(
            row,
            &self.delivery,
            self.result_snapshot_json.as_deref(),
            self.error_snapshot_json.as_deref(),
        )
    }
}

impl PreparedTaskDeliveryAttemptProjection {
    pub(crate) fn with_new_attempt(mut self) -> Self {
        self.insert_new = true;
        self
    }

    pub(crate) fn validate_fields(&self) -> Result<()> {
        validate_attempt_fields(&self.attempt)
    }
    pub(crate) fn with_persisted(mut self, row: task_delivery_attempt::Model) -> Self {
        self.persisted = Some(row);
        self
    }
}

pub(crate) fn validate_durable_delivery(row: &task_delivery::Model) -> Result<()> {
    if row.attempt_count < 0
        || row.max_attempts <= 0
        || row.max_attempts > i64::from(MAX_DELIVERY_ATTEMPTS)
        || row.attempt_count > row.max_attempts
    {
        bail!("durable delivery has an invalid attempt budget");
    }
    let has_receipt = row.delivered_turn_id.is_some()
        || row.delivered_notification_id.is_some()
        || row.delivered_at.is_some();
    let valid = match row.status.as_str() {
        "pending" => {
            row.next_attempt_at.is_some() && !has_receipt && row.attempt_count < row.max_attempts
        }
        "delivering" => row.attempt_count > 0 && row.next_attempt_at.is_none() && !has_receipt,
        "cancelled" => row.next_attempt_at.is_none() && !has_receipt,
        "failed" => {
            row.attempt_count > 0
                && row.next_attempt_at.is_none()
                && !has_receipt
                && row.last_error.is_some()
        }
        "delivered" => {
            row.attempt_count > 0
                && row.next_attempt_at.is_none()
                && row.delivered_at.is_some()
                && row.last_error.is_none()
                && match row.mode.as_str() {
                    "thread" => {
                        row.delivered_turn_id.is_some() && row.delivered_notification_id.is_none()
                    }
                    "user_notification" => {
                        row.delivered_turn_id.is_none() && row.delivered_notification_id.is_some()
                    }
                    "webhook" => {
                        row.delivered_turn_id.is_none() && row.delivered_notification_id.is_none()
                    }
                    _ => false,
                }
        }
        _ => false,
    };
    if !valid {
        bail!("durable delivery has inconsistent lifecycle fields");
    }
    Ok(())
}

pub(crate) fn validate_durable_attempt(row: &task_delivery_attempt::Model) -> Result<()> {
    if row.attempt_number <= 0
        || row
            .http_status
            .is_some_and(|status| !(0..=i64::from(u16::MAX)).contains(&status))
        || !match row.status.as_str() {
            "started" => row.completed_at.is_none() && row.error.is_none(),
            "delivered" => row.completed_at.is_some() && row.error.is_none(),
            "failed" => row.completed_at.is_some() && row.error.is_some(),
            _ => false,
        }
    {
        bail!("durable delivery attempt has inconsistent lifecycle fields");
    }
    Ok(())
}

fn validate_delivery_identity(
    persisted: &task_delivery::Model,
    delivery: &TaskDelivery,
    result_snapshot_json: Option<&str>,
    error_snapshot_json: Option<&str>,
) -> Result<()> {
    // Queue replay ignores the new preparation's identity and clock. Ordinary
    // lifecycle updates still address the original row and creation time.
    if persisted.id != delivery.id || persisted.created_at.timestamp() != delivery.created_at {
        bail!("task delivery attempts to rewrite immutable destination/result facts");
    }
    validate_delivery_logical_facts(
        persisted,
        delivery,
        result_snapshot_json,
        error_snapshot_json,
    )
}

fn validate_delivery_logical_facts(
    persisted: &task_delivery::Model,
    delivery: &TaskDelivery,
    result_snapshot_json: Option<&str>,
    error_snapshot_json: Option<&str>,
) -> Result<()> {
    let immutable_matches = persisted.workspace_id == delivery.workspace_id
        && persisted.task_id == delivery.task_id
        && persisted.run_id == delivery.run_id
        && persisted.delivery_key == delivery.delivery_key
        && persisted.mode == task_delivery_mode_to_db(delivery.mode)
        && persisted.thread_target.as_deref()
            == delivery
                .thread_target
                .map(task_delivery_thread_target_to_db)
        && persisted.target_thread_id == delivery.target_thread_id
        && persisted.target_user_id == delivery.target_user_id
        && persisted.webhook_url == delivery.webhook_url
        && persisted.webhook_url_fingerprint == delivery.webhook_url_fingerprint
        && persisted.max_attempts == i64::from(delivery.max_attempts)
        && persisted.result_snapshot_json.as_deref() == result_snapshot_json
        && persisted.error_snapshot_json.as_deref() == error_snapshot_json;
    if !immutable_matches {
        bail!("task delivery attempts to rewrite immutable destination/result facts");
    }
    Ok(())
}

pub(crate) fn validate_attempt_identity(
    persisted: &task_delivery_attempt::Model,
    attempt: &TaskDeliveryAttempt,
) -> Result<()> {
    if persisted.id != attempt.id
        || persisted.delivery_id != attempt.delivery_id
        || persisted.attempt_number != i64::from(attempt.attempt_number)
        || persisted.started_at.timestamp() != attempt.started_at
        || attempt.attempt_number == 0
    {
        bail!("task delivery attempt rewrites immutable ownership facts");
    }
    Ok(())
}

pub(crate) async fn find_attempt_by_id<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<task_delivery_attempt::Model>> {
    task_delivery_attempt::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query task delivery attempt by id")
}

pub(crate) async fn find_attempt_by_number<C: ConnectionTrait>(
    db: &C,
    delivery_id: &str,
    number: u32,
) -> Result<Option<task_delivery_attempt::Model>> {
    task_delivery_attempt::Entity::find()
        .filter(task_delivery_attempt::Column::DeliveryId.eq(delivery_id.to_owned()))
        .filter(task_delivery_attempt::Column::AttemptNumber.eq(i64::from(number)))
        .one(db)
        .await
        .context("failed to query exact task delivery attempt")
}

fn validate_delivery_update(
    persisted: &task_delivery::Model,
    delivery: &TaskDelivery,
    result_snapshot_json: Option<&str>,
    error_snapshot_json: Option<&str>,
) -> Result<()> {
    validate_delivery_identity(
        persisted,
        delivery,
        result_snapshot_json,
        error_snapshot_json,
    )?;
    if delivery.max_attempts == 0
        || delivery.max_attempts > MAX_DELIVERY_ATTEMPTS
        || delivery.attempt_count > delivery.max_attempts
    {
        bail!("task delivery has an invalid attempt bound");
    }
    let desired_status = task_delivery_status_to_db(delivery.status);
    let transition_allowed = match persisted.status.as_str() {
        "pending" => matches!(desired_status, "pending" | "delivering" | "cancelled"),
        "delivering" => matches!(
            desired_status,
            "delivering" | "pending" | "delivered" | "failed" | "cancelled"
        ),
        "delivered" => desired_status == "delivered",
        "failed" => desired_status == "failed",
        "cancelled" => desired_status == "cancelled",
        _ => false,
    };
    if !transition_allowed || i64::from(delivery.attempt_count) < persisted.attempt_count {
        bail!(
            "task delivery cannot transition from `{}` to `{desired_status}`",
            persisted.status
        );
    }
    if desired_status == "delivering"
        && i64::from(delivery.attempt_count)
            != persisted.attempt_count + if persisted.status == "pending" { 1 } else { 0 }
    {
        bail!("task delivery start does not own the next exact attempt");
    }
    if matches!(
        persisted.status.as_str(),
        "delivered" | "failed" | "cancelled"
    ) || (persisted.status == "delivering" && desired_status == "delivering")
    {
        let mutable_matches = persisted.status == desired_status
            && persisted
                .next_attempt_at
                .as_ref()
                .map(|value| value.timestamp())
                == delivery.next_attempt_at
            && persisted.attempt_count == i64::from(delivery.attempt_count)
            && persisted.delivered_turn_id == delivery.delivered_turn_id
            && persisted.delivered_notification_id == delivery.delivered_notification_id
            && persisted
                .delivered_at
                .as_ref()
                .map(|value| value.timestamp())
                == delivery.delivered_at
            && persisted.last_error == delivery.last_error
            && persisted.updated_at.timestamp() == delivery.updated_at;
        if !mutable_matches {
            bail!("task delivery attempts to rewrite a committed attempt/terminal state");
        }
    }
    validate_delivery_fields(delivery)
}

fn validate_delivery_fields(delivery: &TaskDelivery) -> Result<()> {
    if delivery.max_attempts == 0
        || delivery.max_attempts > MAX_DELIVERY_ATTEMPTS
        || delivery.attempt_count > delivery.max_attempts
    {
        bail!("task delivery has an invalid attempt bound");
    }
    validate_delivery_receipt_fields(delivery)
}

fn validate_delivery_receipt_fields(delivery: &TaskDelivery) -> Result<()> {
    match delivery.status {
        TaskDeliveryStatus::Pending => {
            if delivery.next_attempt_at.is_none()
                || delivery.delivered_turn_id.is_some()
                || delivery.delivered_notification_id.is_some()
                || delivery.delivered_at.is_some()
            {
                bail!("pending task delivery has invalid receipt fields");
            }
        }
        TaskDeliveryStatus::Delivering => {
            if delivery.attempt_count == 0
                || delivery.next_attempt_at.is_some()
                || delivery.delivered_turn_id.is_some()
                || delivery.delivered_notification_id.is_some()
                || delivery.delivered_at.is_some()
            {
                bail!("delivering task delivery has invalid receipt fields");
            }
        }
        TaskDeliveryStatus::Delivered => {
            let exact_receipt = match delivery.mode {
                TaskDeliveryMode::Thread => {
                    delivery.delivered_turn_id.is_some()
                        && delivery.delivered_notification_id.is_none()
                }
                TaskDeliveryMode::UserNotification => {
                    delivery.delivered_turn_id.is_none()
                        && delivery.delivered_notification_id.is_some()
                }
                TaskDeliveryMode::Webhook => {
                    delivery.delivered_turn_id.is_none()
                        && delivery.delivered_notification_id.is_none()
                }
                TaskDeliveryMode::None => false,
            };
            if !exact_receipt
                || delivery.delivered_at.is_none()
                || delivery.next_attempt_at.is_some()
                || delivery.last_error.is_some()
            {
                bail!("delivered task delivery has invalid exact receipt fields");
            }
        }
        TaskDeliveryStatus::Failed => {
            if delivery.last_error.is_none()
                || delivery.next_attempt_at.is_some()
                || delivery.delivered_turn_id.is_some()
                || delivery.delivered_notification_id.is_some()
                || delivery.delivered_at.is_some()
            {
                bail!("failed task delivery has invalid terminal fields");
            }
        }
        TaskDeliveryStatus::Cancelled => {
            if delivery.next_attempt_at.is_some()
                || delivery.delivered_turn_id.is_some()
                || delivery.delivered_notification_id.is_some()
                || delivery.delivered_at.is_some()
            {
                bail!("cancelled task delivery has invalid terminal fields");
            }
        }
    }
    Ok(())
}

fn validate_attempt_update(
    persisted: &task_delivery_attempt::Model,
    attempt: &TaskDeliveryAttempt,
) -> Result<()> {
    if persisted.delivery_id != attempt.delivery_id
        || persisted.attempt_number != i64::from(attempt.attempt_number)
        || persisted.started_at.timestamp() != attempt.started_at
        || attempt.attempt_number == 0
    {
        bail!("task delivery attempt rewrites immutable ownership facts");
    }
    let desired_status = task_delivery_attempt_status_to_db(attempt.status);
    let transition_allowed = match persisted.status.as_str() {
        "started" => matches!(desired_status, "started" | "delivered" | "failed"),
        "delivered" => desired_status == "delivered",
        "failed" => desired_status == "failed",
        _ => false,
    };
    if !transition_allowed {
        bail!("task delivery attempt has an invalid status transition");
    }
    if matches!(persisted.status.as_str(), "delivered" | "failed") {
        let exact = persisted.status == desired_status
            && persisted
                .completed_at
                .as_ref()
                .map(|value| value.timestamp())
                == attempt.completed_at
            && persisted.http_status == attempt.http_status.map(i64::from)
            && persisted.error == attempt.error
            && persisted.response_fingerprint == attempt.response_fingerprint;
        if !exact {
            bail!("task delivery attempt rewrites terminal receipt facts");
        }
    }
    validate_attempt_fields(attempt)
}

fn validate_attempt_fields(attempt: &TaskDeliveryAttempt) -> Result<()> {
    if attempt.attempt_number == 0 {
        bail!("task delivery attempt number is zero");
    }
    match attempt.status {
        TaskDeliveryAttemptStatus::Started => {
            if attempt.completed_at.is_some() || attempt.error.is_some() {
                bail!("started task delivery attempt has terminal fields");
            }
        }
        TaskDeliveryAttemptStatus::Delivered => {
            if attempt.completed_at.is_none() || attempt.error.is_some() {
                bail!("delivered task delivery attempt has invalid receipt fields");
            }
        }
        TaskDeliveryAttemptStatus::Failed => {
            if attempt.completed_at.is_none() || attempt.error.is_none() {
                bail!("failed task delivery attempt has invalid receipt fields");
            }
        }
    }
    Ok(())
}

pub async fn find_delivery_by_key<C: ConnectionTrait>(
    db: &C,
    delivery_key: &str,
) -> Result<Option<task_delivery::Model>> {
    task_delivery::Entity::find()
        .filter(task_delivery::Column::DeliveryKey.eq(delivery_key.to_owned()))
        .one(db)
        .await
        .context("failed to query exact task delivery key")
}

/// Queue replay compares logical facts, never the new random id or wall clock.
/// It does not call the ordinary update validator or mutate attempt/receipt state.
pub(crate) fn resolve_queue_replay(
    persisted: task_delivery::Model,
    prepared: PreparedTaskDeliveryProjection,
) -> Result<TaskDelivery> {
    validate_delivery_logical_facts(
        &persisted,
        &prepared.delivery,
        prepared.result_snapshot_json.as_deref(),
        prepared.error_snapshot_json.as_deref(),
    )?;
    // Reuse every durable identity, time, attempt and receipt without decoding
    // result/error JSON under the writer. Their bytes were compared above.
    let mut delivery = prepared.delivery;
    delivery.id = persisted.id;
    delivery.status = crate::convention::task_delivery_status_from_db(&persisted.status)
        .context("invalid persisted delivery status")?;
    delivery.next_attempt_at = persisted.next_attempt_at.map(|t| t.timestamp());
    delivery.attempt_count = u32::try_from(persisted.attempt_count)?;
    delivery.delivered_turn_id = persisted.delivered_turn_id;
    delivery.delivered_notification_id = persisted.delivered_notification_id;
    delivery.delivered_at = persisted.delivered_at.map(|t| t.timestamp());
    delivery.last_error = persisted.last_error;
    delivery.created_at = persisted.created_at.timestamp();
    delivery.updated_at = persisted.updated_at.timestamp();
    Ok(delivery)
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
    if limit == 0 || limit > MAX_DELIVERY_LIST_LIMIT {
        bail!("due Task delivery batch exceeds its bounded limit");
    }
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
    if limit == 0 || limit > MAX_DELIVERY_LIST_LIMIT {
        bail!("stuck Task delivery batch exceeds its bounded limit");
    }
    task_delivery::Entity::find()
        .filter(task_delivery::Column::Status.eq("delivering"))
        .filter(task_delivery::Column::UpdatedAt.lte(before))
        .order_by_asc(task_delivery::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list stuck task deliveries")
}

pub async fn list_deliveries_scoped<C: ConnectionTrait>(
    db: &C,
    params: &TaskDeliveriesParams,
    access: Option<&TaskRootAccessFilter>,
) -> Result<Vec<task_delivery::Model>> {
    let limit = params
        .limit
        .map(u64::from)
        .unwrap_or(DEFAULT_DELIVERY_LIMIT);
    if limit == 0 || limit > MAX_DELIVERY_LIST_LIMIT {
        bail!("Task delivery list exceeds its bounded limit");
    }
    let mut query = task_delivery::Entity::find()
        .filter(task_delivery::Column::WorkspaceId.eq(params.workspace_id.clone()))
        .order_by_desc(task_delivery::Column::UpdatedAt)
        .limit(limit);

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
        .order_by_desc(task_delivery::Column::CreatedAt)
        .limit(1)
        .all(db)
        .await
        .context("failed to list task deliveries by task")
}

pub async fn list_delivered_thread_deliveries_for_tasks<C: ConnectionTrait>(
    db: &C,
    task_ids: &[String],
    target_thread_id: &str,
    delivered_turn_ids: &[String],
) -> Result<Vec<task_delivery::Model>> {
    if task_ids.is_empty() || delivered_turn_ids.is_empty() {
        return Ok(Vec::new());
    }
    if task_ids.len() > MAX_DELIVERY_BATCH_IDS || delivered_turn_ids.len() > MAX_DELIVERY_BATCH_IDS
    {
        bail!("Task conversation delivery batch exceeds its bounded limit");
    }
    let rows = task_delivery::Entity::find()
        .filter(task_delivery::Column::TaskId.is_in(task_ids.to_vec()))
        .filter(task_delivery::Column::TargetThreadId.eq(target_thread_id.to_owned()))
        .filter(task_delivery::Column::Status.eq("delivered"))
        .filter(task_delivery::Column::DeliveredTurnId.is_in(delivered_turn_ids.to_vec()))
        .order_by_asc(task_delivery::Column::DeliveredAt)
        .order_by_asc(task_delivery::Column::CreatedAt)
        .limit((delivered_turn_ids.len() as u64).saturating_add(1))
        .all(db)
        .await
        .context("failed to list delivered task conversation links")?;
    if rows.len() > delivered_turn_ids.len() {
        bail!("Task conversation Turn has multiple delivery owners");
    }
    Ok(rows)
}

pub async fn list_thread_deliveries_by_delivered_turns<C: ConnectionTrait>(
    db: &C,
    target_thread_id: &str,
    delivered_turn_ids: &[String],
) -> Result<Vec<task_delivery::Model>> {
    if delivered_turn_ids.is_empty() {
        return Ok(Vec::new());
    }
    if delivered_turn_ids.len() > MAX_DELIVERY_BATCH_IDS {
        bail!("Task delivered-Turn batch exceeds its bounded limit");
    }
    let rows = task_delivery::Entity::find()
        .filter(task_delivery::Column::TargetThreadId.eq(target_thread_id.to_owned()))
        .filter(task_delivery::Column::DeliveredTurnId.is_in(delivered_turn_ids.to_vec()))
        .limit((delivered_turn_ids.len() as u64).saturating_add(1))
        .all(db)
        .await
        .context("failed to list task deliveries by delivered turns")?;
    if rows.len() > delivered_turn_ids.len() {
        bail!("Task delivered Turn has multiple delivery owners");
    }
    Ok(rows)
}

pub async fn list_attempts_for_deliveries<C: ConnectionTrait>(
    db: &C,
    delivery_ids: &[String],
) -> Result<Vec<task_delivery_attempt::Model>> {
    if delivery_ids.is_empty() {
        return Ok(Vec::new());
    }
    if delivery_ids.len() > MAX_DELIVERY_LIST_LIMIT as usize {
        bail!("Task delivery-attempt batch exceeds its bounded limit");
    }
    let maximum_rows = delivery_ids
        .len()
        .saturating_mul(MAX_DELIVERY_ATTEMPTS as usize);
    let rows = task_delivery_attempt::Entity::find()
        .filter(task_delivery_attempt::Column::DeliveryId.is_in(delivery_ids.to_vec()))
        .order_by_asc(task_delivery_attempt::Column::StartedAt)
        .limit((maximum_rows as u64).saturating_add(1))
        .all(db)
        .await
        .context("failed to list task delivery attempts")?;
    if rows.len() > maximum_rows {
        bail!("Task delivery attempts exceed their bounded retry policy");
    }
    Ok(rows)
}

fn active_model_from_delivery_json(
    delivery: &TaskDelivery,
    result_snapshot_json: Option<String>,
    error_snapshot_json: Option<String>,
) -> task_delivery::ActiveModel {
    task_delivery::ActiveModel {
        id: Set(delivery.id.clone()),
        workspace_id: Set(delivery.workspace_id.clone()),
        task_id: Set(delivery.task_id.clone()),
        run_id: Set(delivery.run_id.clone()),
        delivery_key: Set(delivery.delivery_key.clone()),
        mode: Set(task_delivery_mode_to_db(delivery.mode).to_owned()),
        thread_target: Set(delivery
            .thread_target
            .map(task_delivery_thread_target_to_db)
            .map(str::to_owned)),
        target_thread_id: Set(delivery.target_thread_id.clone()),
        target_user_id: Set(delivery.target_user_id.clone()),
        webhook_url: Set(delivery.webhook_url.clone()),
        webhook_url_fingerprint: Set(delivery.webhook_url_fingerprint.clone()),
        status: Set(task_delivery_status_to_db(delivery.status).to_owned()),
        next_attempt_at: Set(delivery.next_attempt_at.map(unix_to_datetime)),
        attempt_count: Set(i64::from(delivery.attempt_count)),
        max_attempts: Set(i64::from(delivery.max_attempts)),
        result_snapshot_json: Set(result_snapshot_json),
        error_snapshot_json: Set(error_snapshot_json),
        delivered_turn_id: Set(delivery.delivered_turn_id.clone()),
        delivered_notification_id: Set(delivery.delivered_notification_id.clone()),
        delivered_at: Set(delivery.delivered_at.map(unix_to_datetime)),
        last_error: Set(delivery.last_error.clone()),
        created_at: Set(unix_to_datetime(delivery.created_at)),
        updated_at: Set(unix_to_datetime(delivery.updated_at)),
    }
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
