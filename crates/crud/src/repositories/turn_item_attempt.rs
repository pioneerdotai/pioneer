use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_item, turn_item_attempt};
use pioneer_protocol::{
    TurnItem, TurnItemAttemptStatus, TurnItemExecutionClass, TurnItemTimeoutReason, TurnItemType,
    TurnStatus, generate_id,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, ExprTrait, Order, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    ATTEMPT_STATUS_TIMED_OUT, TURN_ITEM_STATUS_IN_PROGRESS, TURN_ITEM_STATUS_TIMED_OUT,
    turn_item_attempt_status_to_db, turn_item_execution_class_from_db,
    turn_item_execution_class_to_db, turn_item_timeout_reason_from_db,
    turn_item_timeout_reason_to_db, turn_item_type_from_db, turn_item_type_to_db,
    turn_status_to_db,
};
use crate::turn_item_terminal::{TurnItemTerminalState, terminalize_turn_item_payload};

const DB_ID_LEN: usize = 21;

#[derive(Debug, Clone)]
pub struct AttemptDeadlines {
    pub lease_expires_at: Option<DateTimeWithTimeZone>,
    pub idle_deadline_at: Option<DateTimeWithTimeZone>,
    pub hard_deadline_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone)]
pub struct RunningAttemptSnapshot {
    pub id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub execution_class: Option<TurnItemExecutionClass>,
    pub attempt_number: i64,
    pub started_at: DateTimeWithTimeZone,
    pub started_event_sequence: Option<i64>,
    pub last_heartbeat_at: Option<DateTimeWithTimeZone>,
    pub lease_expires_at: Option<DateTimeWithTimeZone>,
    pub idle_deadline_at: Option<DateTimeWithTimeZone>,
    pub hard_deadline_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone)]
pub struct TimedOutAttemptSnapshot {
    pub id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub execution_class: TurnItemExecutionClass,
    pub attempt_number: i64,
    pub timeout_reason: TurnItemTimeoutReason,
    pub started_at: DateTimeWithTimeZone,
    pub started_event_sequence: Option<i64>,
    pub last_heartbeat_at: Option<DateTimeWithTimeZone>,
    pub lease_expires_at: Option<DateTimeWithTimeZone>,
    pub idle_deadline_at: Option<DateTimeWithTimeZone>,
    pub hard_deadline_at: Option<DateTimeWithTimeZone>,
}

pub async fn create_running_attempt<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
    item_type: TurnItemType,
    execution_class: TurnItemExecutionClass,
    payload_json: String,
    deadlines: AttemptDeadlines,
    started_at: DateTimeWithTimeZone,
    started_event_sequence: Option<i64>,
) -> Result<turn_item_attempt::Model> {
    let next_attempt_number = next_attempt_number(db, turn_id, item_id).await?;
    let id = generate_id(DB_ID_LEN);

    let status = turn_item_attempt_status_to_db(TurnItemAttemptStatus::Running).to_owned();
    let item_type_db = turn_item_type_to_db(item_type).to_owned();

    turn_item_attempt::Entity::insert(turn_item_attempt::ActiveModel {
        id: Set(id.clone()),
        turn_id: Set(turn_id.to_owned()),
        item_id: Set(item_id.to_owned()),
        item_type: Set(item_type_db),
        execution_class: Set(Some(
            turn_item_execution_class_to_db(execution_class).to_owned(),
        )),
        attempt_number: Set(next_attempt_number),
        status: Set(status.clone()),
        timeout_reason: Set(None),
        failure_reason: Set(None),
        recovery_action: Set(None),
        idempotency_key: Set(None),
        trace_id: Set(None),
        payload: Set(payload_json),
        started_at: Set(started_at),
        started_event_sequence: Set(started_event_sequence),
        last_heartbeat_at: Set(Some(started_at)),
        lease_expires_at: Set(deadlines.lease_expires_at),
        idle_deadline_at: Set(deadlines.idle_deadline_at),
        hard_deadline_at: Set(deadlines.hard_deadline_at),
        recovery_suppressed_reason: Set(None),
        recovery_suppressed_at: Set(None),
        recovery_suppression_context_json: Set(None),
        updated_at: Set(started_at),
    })
    .exec(db)
    .await
    .context("failed to insert turn_item_attempts running row")?;

    turn_item::Entity::update_many()
        .col_expr(
            turn_item::Column::ActiveAttemptNumber,
            Expr::value(next_attempt_number),
        )
        .col_expr(turn_item::Column::ActiveAttemptStatus, Expr::value(status))
        .col_expr(turn_item::Column::ActiveAttemptId, Expr::value(id.clone()))
        .col_expr(
            turn_item::Column::LastHeartbeatAt,
            Expr::value(Some(started_at)),
        )
        .col_expr(
            turn_item::Column::LeaseExpiresAt,
            Expr::value(deadlines.lease_expires_at),
        )
        .col_expr(turn_item::Column::UpdatedAt, Expr::value(started_at))
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
        .exec(db)
        .await
        .context("failed to update turn_item active attempt metadata")?;

    turn_item_attempt::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload inserted running attempt")?
        .context("inserted running attempt row is missing")
}

pub async fn heartbeat_running_attempt<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
    heartbeat_at: DateTimeWithTimeZone,
    next_lease_expires_at: Option<DateTimeWithTimeZone>,
    next_idle_deadline_at: Option<DateTimeWithTimeZone>,
    next_hard_deadline_at: Option<DateTimeWithTimeZone>,
) -> Result<bool> {
    let Some(running) = latest_running_attempt(db, turn_id, item_id).await? else {
        return Ok(false);
    };

    let mut update = turn_item_attempt::Entity::update_many()
        .col_expr(
            turn_item_attempt::Column::LastHeartbeatAt,
            Expr::value(Some(heartbeat_at)),
        )
        .col_expr(
            turn_item_attempt::Column::LeaseExpiresAt,
            Expr::value(next_lease_expires_at),
        )
        .col_expr(
            turn_item_attempt::Column::IdleDeadlineAt,
            Expr::value(next_idle_deadline_at),
        );
    if let Some(hard_deadline_at) = next_hard_deadline_at {
        update = update.col_expr(
            turn_item_attempt::Column::HardDeadlineAt,
            Expr::value(Some(hard_deadline_at)),
        );
    }
    let affected = update
        .col_expr(
            turn_item_attempt::Column::UpdatedAt,
            Expr::value(heartbeat_at),
        )
        .filter(turn_item_attempt::Column::Id.eq(running.id.clone()))
        .filter(
            turn_item_attempt::Column::Status.eq(turn_item_attempt_status_to_db(
                TurnItemAttemptStatus::Running,
            )),
        )
        .exec(db)
        .await
        .context("failed to heartbeat running attempt")?
        .rows_affected
        > 0;

    if !affected {
        return Ok(false);
    }

    turn_item::Entity::update_many()
        .col_expr(
            turn_item::Column::LastHeartbeatAt,
            Expr::value(Some(heartbeat_at)),
        )
        .col_expr(
            turn_item::Column::LeaseExpiresAt,
            Expr::value(next_lease_expires_at),
        )
        .col_expr(turn_item::Column::UpdatedAt, Expr::value(heartbeat_at))
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
        .exec(db)
        .await
        .context("failed to heartbeat turn_item row")?;

    Ok(true)
}

pub async fn configure_running_attempt_deadlines<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
    heartbeat_at: DateTimeWithTimeZone,
    lease_expires_at: Option<DateTimeWithTimeZone>,
    idle_deadline_at: Option<DateTimeWithTimeZone>,
    hard_deadline_at: Option<DateTimeWithTimeZone>,
) -> Result<bool> {
    let Some(running) = latest_running_attempt(db, turn_id, item_id).await? else {
        return Ok(false);
    };

    let affected = turn_item_attempt::Entity::update_many()
        .col_expr(
            turn_item_attempt::Column::LastHeartbeatAt,
            Expr::value(Some(heartbeat_at)),
        )
        .col_expr(
            turn_item_attempt::Column::LeaseExpiresAt,
            Expr::value(lease_expires_at),
        )
        .col_expr(
            turn_item_attempt::Column::IdleDeadlineAt,
            Expr::value(idle_deadline_at),
        )
        .col_expr(
            turn_item_attempt::Column::HardDeadlineAt,
            Expr::value(hard_deadline_at),
        )
        .col_expr(
            turn_item_attempt::Column::UpdatedAt,
            Expr::value(heartbeat_at),
        )
        .filter(turn_item_attempt::Column::Id.eq(running.id.clone()))
        .filter(
            turn_item_attempt::Column::Status.eq(turn_item_attempt_status_to_db(
                TurnItemAttemptStatus::Running,
            )),
        )
        .exec(db)
        .await
        .context("failed to configure running attempt deadlines")?
        .rows_affected
        > 0;

    if !affected {
        return Ok(false);
    }

    turn_item::Entity::update_many()
        .col_expr(
            turn_item::Column::LastHeartbeatAt,
            Expr::value(Some(heartbeat_at)),
        )
        .col_expr(
            turn_item::Column::LeaseExpiresAt,
            Expr::value(lease_expires_at),
        )
        .col_expr(turn_item::Column::UpdatedAt, Expr::value(heartbeat_at))
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
        .exec(db)
        .await
        .context("failed to update turn_item deadlines")?;

    Ok(true)
}

pub async fn finish_running_attempt<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
    status: TurnItemAttemptStatus,
    failure_reason: Option<String>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(running) = latest_running_attempt(db, turn_id, item_id).await? else {
        return Ok(false);
    };

    let status_db = turn_item_attempt_status_to_db(status).to_owned();

    let mut update = turn_item_attempt::Entity::update_many()
        .col_expr(
            turn_item_attempt::Column::Status,
            Expr::value(status_db.clone()),
        )
        .col_expr(
            turn_item_attempt::Column::FailureReason,
            Expr::value(failure_reason),
        )
        .col_expr(
            turn_item_attempt::Column::UpdatedAt,
            Expr::value(updated_at),
        );
    if clears_attempt_payload(status) {
        update = update.col_expr(turn_item_attempt::Column::Payload, Expr::value("{}"));
    }

    let affected = update
        .filter(turn_item_attempt::Column::Id.eq(running.id.clone()))
        .filter(
            turn_item_attempt::Column::Status.eq(turn_item_attempt_status_to_db(
                TurnItemAttemptStatus::Running,
            )),
        )
        .exec(db)
        .await
        .context("failed to update running attempt terminal status")?
        .rows_affected
        > 0;

    if !affected {
        return Ok(false);
    }

    turn_item::Entity::update_many()
        .col_expr(
            turn_item::Column::ActiveAttemptStatus,
            Expr::value(Some(status_db)),
        )
        .col_expr(turn_item::Column::UpdatedAt, Expr::value(updated_at))
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
        .exec(db)
        .await
        .context("failed to update turn_item active status")?;

    Ok(true)
}

fn clears_attempt_payload(status: TurnItemAttemptStatus) -> bool {
    matches!(
        status,
        TurnItemAttemptStatus::Completed
            | TurnItemAttemptStatus::Failed
            | TurnItemAttemptStatus::Cancelled
            | TurnItemAttemptStatus::Interrupted
            | TurnItemAttemptStatus::Exhausted
    )
}

pub async fn list_expired_running_attempts<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<RunningAttemptSnapshot>> {
    let running = turn_item_attempt_status_to_db(TurnItemAttemptStatus::Running);
    let rows = turn_item_attempt::Entity::find()
        .filter(turn_item_attempt::Column::Status.eq(running))
        // Legacy attempts remain non-destructive until the background data
        // migration has assigned their authoritative execution class.
        .filter(turn_item_attempt::Column::ExecutionClass.is_not_null())
        .filter(
            Expr::col(turn_item_attempt::Column::HardDeadlineAt)
                .lte(now)
                .or(Expr::col(turn_item_attempt::Column::IdleDeadlineAt).lte(now))
                .or(Expr::col(turn_item_attempt::Column::LeaseExpiresAt).lte(now)),
        )
        .order_by(turn_item_attempt::Column::StartedAt, Order::Asc)
        .limit(limit)
        .all(db)
        .await
        .context("failed to query expired running attempts")?;

    rows.into_iter().map(running_snapshot_from_model).collect()
}

pub async fn transition_running_attempt_to_timed_out<C: ConnectionTrait>(
    db: &C,
    attempt: &RunningAttemptSnapshot,
    timeout_reason: TurnItemTimeoutReason,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let timed_out_status = turn_item_attempt_status_to_db(TurnItemAttemptStatus::TimedOut);
    let running_status = turn_item_attempt_status_to_db(TurnItemAttemptStatus::Running);
    let timeout_reason_db = turn_item_timeout_reason_to_db(timeout_reason).to_owned();

    let last_heartbeat_matches = match attempt.last_heartbeat_at.as_ref() {
        Some(value) => turn_item_attempt::Column::LastHeartbeatAt.eq(value.clone()),
        None => turn_item_attempt::Column::LastHeartbeatAt.is_null(),
    };
    let lease_matches = match attempt.lease_expires_at.as_ref() {
        Some(value) => turn_item_attempt::Column::LeaseExpiresAt.eq(value.clone()),
        None => turn_item_attempt::Column::LeaseExpiresAt.is_null(),
    };
    let idle_matches = match attempt.idle_deadline_at.as_ref() {
        Some(value) => turn_item_attempt::Column::IdleDeadlineAt.eq(value.clone()),
        None => turn_item_attempt::Column::IdleDeadlineAt.is_null(),
    };
    let hard_matches = match attempt.hard_deadline_at.as_ref() {
        Some(value) => turn_item_attempt::Column::HardDeadlineAt.eq(value.clone()),
        None => turn_item_attempt::Column::HardDeadlineAt.is_null(),
    };
    let expired_deadline = match timeout_reason {
        TurnItemTimeoutReason::StartDeadlineExceeded => return Ok(false),
        TurnItemTimeoutReason::HardDeadlineExceeded => {
            let Some(deadline) = attempt.hard_deadline_at.as_ref() else {
                return Ok(false);
            };
            if deadline > &updated_at {
                return Ok(false);
            }
            turn_item_attempt::Column::HardDeadlineAt.lte(updated_at.clone())
        }
        TurnItemTimeoutReason::IdleDeadlineExceeded => {
            let Some(deadline) = attempt.idle_deadline_at.as_ref() else {
                return Ok(false);
            };
            if deadline > &updated_at {
                return Ok(false);
            }
            turn_item_attempt::Column::IdleDeadlineAt.lte(updated_at.clone())
        }
        TurnItemTimeoutReason::LeaseExpired => {
            let Some(deadline) = attempt.lease_expires_at.as_ref() else {
                return Ok(false);
            };
            if deadline > &updated_at {
                return Ok(false);
            }
            turn_item_attempt::Column::LeaseExpiresAt.lte(updated_at.clone())
        }
    };

    // The timeout candidate was read before runtime observation. Match its
    // complete liveness frontier in the terminal update so a heartbeat that
    // moves any deadline makes this stale candidate a no-op.
    let current_candidate = Condition::all()
        .add(turn_item_attempt::Column::Id.eq(attempt.id.clone()))
        .add(turn_item_attempt::Column::Status.eq(running_status))
        .add(last_heartbeat_matches)
        .add(lease_matches)
        .add(idle_matches)
        .add(hard_matches)
        .add(expired_deadline);

    let affected = turn_item_attempt::Entity::update_many()
        .col_expr(
            turn_item_attempt::Column::Status,
            Expr::value(timed_out_status.to_owned()),
        )
        .col_expr(
            turn_item_attempt::Column::TimeoutReason,
            Expr::value(Some(timeout_reason_db)),
        )
        .col_expr(
            turn_item_attempt::Column::UpdatedAt,
            Expr::value(updated_at),
        )
        .filter(current_candidate)
        .exec(db)
        .await
        .context("failed to transition attempt to timed_out")?
        .rows_affected
        > 0;

    if !affected {
        return Ok(false);
    }

    let item_row = turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(attempt.turn_id.clone()))
        .filter(turn_item::Column::ItemId.eq(attempt.item_id.clone()))
        .one(db)
        .await
        .context("failed to load turn_item row for timeout terminalization")?
        .context("timed out attempt missing turn_item row")?;

    let mut item: TurnItem =
        serde_json::from_str(item_row.payload.as_str()).with_context(|| {
            format!(
                "failed to decode timed out turn_item payload for turn `{}` item `{}`",
                attempt.turn_id, attempt.item_id
            )
        })?;
    terminalize_turn_item_payload(
        &mut item,
        TurnItemTerminalState::TimedOut {
            reason: timeout_reason,
        },
    );
    let payload_json =
        serde_json::to_string(&item).context("failed to encode terminalized item")?;
    let turn_item_status = Some(TURN_ITEM_STATUS_TIMED_OUT);

    turn_item::Entity::update_many()
        .col_expr(turn_item::Column::Status, Expr::value(turn_item_status))
        .col_expr(
            turn_item::Column::ActiveAttemptStatus,
            Expr::value(Some(timed_out_status)),
        )
        .col_expr(turn_item::Column::Payload, Expr::value(payload_json))
        .col_expr(turn_item::Column::UpdatedAt, Expr::value(updated_at))
        .filter(turn_item::Column::TurnId.eq(attempt.turn_id.clone()))
        .filter(turn_item::Column::ItemId.eq(attempt.item_id.clone()))
        .exec(db)
        .await
        .context("failed to mark turn_item timed_out")?;

    Ok(true)
}

pub async fn find_timed_out_attempt_by_id<C: ConnectionTrait>(
    db: &C,
    attempt_id: &str,
) -> Result<Option<TimedOutAttemptSnapshot>> {
    let timed_out_status = turn_item_attempt_status_to_db(TurnItemAttemptStatus::TimedOut);
    let Some(row) = turn_item_attempt::Entity::find_by_id(attempt_id.to_owned())
        .filter(turn_item_attempt::Column::Status.eq(timed_out_status))
        .one(db)
        .await
        .context("failed to query timed_out attempt by id")?
    else {
        return Ok(None);
    };
    let execution_class = row
        .execution_class
        .as_deref()
        .and_then(turn_item_execution_class_from_db)
        .with_context(|| format!("attempt `{attempt_id}` has an invalid execution class"))?;
    Ok(Some(TimedOutAttemptSnapshot {
        id: row.id,
        turn_id: row.turn_id,
        item_id: row.item_id,
        item_type: turn_item_type_from_db(row.item_type.as_str())
            .unwrap_or(TurnItemType::DynamicToolCall),
        execution_class,
        attempt_number: row.attempt_number,
        timeout_reason: row
            .timeout_reason
            .as_deref()
            .and_then(turn_item_timeout_reason_from_db)
            .unwrap_or(TurnItemTimeoutReason::HardDeadlineExceeded),
        started_at: row.started_at,
        started_event_sequence: row.started_event_sequence,
        last_heartbeat_at: row.last_heartbeat_at,
        lease_expires_at: row.lease_expires_at,
        idle_deadline_at: row.idle_deadline_at,
        hard_deadline_at: row.hard_deadline_at,
    }))
}

pub async fn reactivate_timed_out_attempt<C: ConnectionTrait>(
    db: &C,
    attempt_id: &str,
    heartbeat_at: DateTimeWithTimeZone,
    deadlines: AttemptDeadlines,
) -> Result<bool> {
    let timed_out_status = turn_item_attempt_status_to_db(TurnItemAttemptStatus::TimedOut);
    let running_status = turn_item_attempt_status_to_db(TurnItemAttemptStatus::Running);
    let Some(attempt) = turn_item_attempt::Entity::find_by_id(attempt_id.to_owned())
        .filter(turn_item_attempt::Column::Status.eq(timed_out_status))
        .one(db)
        .await
        .context("failed to load timed_out attempt for runtime rehydration")?
    else {
        return Ok(false);
    };
    if turn::Entity::find_by_id(attempt.turn_id.clone())
        .filter(turn::Column::Status.eq(turn_status_to_db(TurnStatus::InProgress)))
        .one(db)
        .await
        .context("failed to verify Turn status before attempt rehydration")?
        .is_none()
    {
        return Ok(false);
    }

    let item: TurnItem = serde_json::from_str(attempt.payload.as_str()).with_context(|| {
        format!(
            "failed to decode source payload for timed_out attempt `{}`",
            attempt.id
        )
    })?;
    if item.item_id() != attempt.item_id {
        anyhow::bail!(
            "timed_out attempt `{}` payload references item `{}` instead of `{}`",
            attempt.id,
            item.item_id(),
            attempt.item_id
        );
    }
    let payload_json =
        serde_json::to_string(&item).context("failed to encode rehydrated turn item")?;

    let attempt_affected = turn_item_attempt::Entity::update_many()
        .col_expr(
            turn_item_attempt::Column::Status,
            Expr::value(running_status.to_owned()),
        )
        .col_expr(
            turn_item_attempt::Column::TimeoutReason,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_item_attempt::Column::FailureReason,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_item_attempt::Column::RecoveryAction,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_item_attempt::Column::RecoverySuppressedReason,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_item_attempt::Column::RecoverySuppressedAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_item_attempt::Column::RecoverySuppressionContextJson,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_item_attempt::Column::LastHeartbeatAt,
            Expr::value(Some(heartbeat_at.clone())),
        )
        .col_expr(
            turn_item_attempt::Column::LeaseExpiresAt,
            Expr::value(deadlines.lease_expires_at.clone()),
        )
        .col_expr(
            turn_item_attempt::Column::IdleDeadlineAt,
            Expr::value(deadlines.idle_deadline_at.clone()),
        )
        .col_expr(
            turn_item_attempt::Column::HardDeadlineAt,
            Expr::value(deadlines.hard_deadline_at.clone()),
        )
        .col_expr(
            turn_item_attempt::Column::UpdatedAt,
            Expr::value(heartbeat_at.clone()),
        )
        .filter(turn_item_attempt::Column::Id.eq(attempt.id.clone()))
        .filter(turn_item_attempt::Column::Status.eq(timed_out_status))
        .exec(db)
        .await
        .context("failed to reactivate timed_out attempt")?
        .rows_affected;
    if attempt_affected == 0 {
        return Ok(false);
    }

    let item_affected = turn_item::Entity::update_many()
        .col_expr(
            turn_item::Column::Status,
            Expr::value(Some(TURN_ITEM_STATUS_IN_PROGRESS)),
        )
        .col_expr(
            turn_item::Column::ActiveAttemptStatus,
            Expr::value(Some(running_status)),
        )
        .col_expr(turn_item::Column::Payload, Expr::value(payload_json))
        .col_expr(
            turn_item::Column::LastHeartbeatAt,
            Expr::value(Some(heartbeat_at.clone())),
        )
        .col_expr(
            turn_item::Column::LeaseExpiresAt,
            Expr::value(deadlines.lease_expires_at),
        )
        .col_expr(turn_item::Column::UpdatedAt, Expr::value(heartbeat_at))
        .filter(turn_item::Column::TurnId.eq(attempt.turn_id.clone()))
        .filter(turn_item::Column::ItemId.eq(attempt.item_id.clone()))
        .filter(turn_item::Column::ActiveAttemptId.eq(attempt.id.clone()))
        .filter(turn_item::Column::ActiveAttemptStatus.eq(timed_out_status))
        .filter(turn_item::Column::Status.eq(TURN_ITEM_STATUS_TIMED_OUT))
        .exec(db)
        .await
        .context("failed to reactivate timed_out turn item")?
        .rows_affected;
    if item_affected != 1 {
        anyhow::bail!(
            "timed_out attempt `{}` no longer owns its terminal turn item",
            attempt.id
        );
    }

    Ok(true)
}

pub async fn list_running_attempts_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<RunningAttemptSnapshot>> {
    let running = turn_item_attempt_status_to_db(TurnItemAttemptStatus::Running);
    let rows = turn_item_attempt::Entity::find()
        .filter(turn_item_attempt::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item_attempt::Column::Status.eq(running))
        .order_by_asc(turn_item_attempt::Column::AttemptNumber)
        .all(db)
        .await
        .context("failed to list running attempts for turn")?;

    rows.into_iter().map(running_snapshot_from_model).collect()
}

pub async fn list_running_attempts_by_item_type<C: ConnectionTrait>(
    db: &C,
    item_type: TurnItemType,
    limit: u64,
) -> Result<Vec<RunningAttemptSnapshot>> {
    let running = turn_item_attempt_status_to_db(TurnItemAttemptStatus::Running);
    let rows = turn_item_attempt::Entity::find()
        .filter(turn_item_attempt::Column::Status.eq(running))
        .filter(turn_item_attempt::Column::ItemType.eq(turn_item_type_to_db(item_type)))
        .order_by_asc(turn_item_attempt::Column::StartedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list running attempts by item type")?;

    rows.into_iter().map(running_snapshot_from_model).collect()
}

pub async fn find_latest_running_attempt<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<RunningAttemptSnapshot>> {
    latest_running_attempt(db, turn_id, item_id)
        .await?
        .map(running_snapshot_from_model)
        .transpose()
}

pub async fn list_timed_out_without_recovery<C: ConnectionTrait>(
    db: &C,
    limit: u64,
) -> Result<Vec<TimedOutAttemptSnapshot>> {
    let timed_out_status = turn_item_attempt_status_to_db(TurnItemAttemptStatus::TimedOut);
    let rows = turn_item_attempt::Entity::find()
        .filter(turn_item_attempt::Column::Status.eq(timed_out_status))
        .filter(turn_item_attempt::Column::ExecutionClass.is_not_null())
        .filter(turn_item_attempt::Column::RecoveryAction.is_null())
        .filter(turn_item_attempt::Column::RecoverySuppressedReason.is_null())
        .order_by_asc(turn_item_attempt::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to query timed_out attempts without recovery")?;

    rows.into_iter()
        .map(|row| -> Result<TimedOutAttemptSnapshot> {
            let execution_class = row
                .execution_class
                .as_deref()
                .and_then(turn_item_execution_class_from_db)
                .with_context(|| format!("attempt `{}` has an invalid execution class", row.id))?;
            Ok(TimedOutAttemptSnapshot {
                id: row.id,
                turn_id: row.turn_id,
                item_id: row.item_id,
                item_type: turn_item_type_from_db(row.item_type.as_str())
                    .unwrap_or(TurnItemType::DynamicToolCall),
                execution_class,
                attempt_number: row.attempt_number,
                timeout_reason: row
                    .timeout_reason
                    .as_deref()
                    .and_then(turn_item_timeout_reason_from_db)
                    .unwrap_or(TurnItemTimeoutReason::HardDeadlineExceeded),
                started_at: row.started_at,
                started_event_sequence: row.started_event_sequence,
                last_heartbeat_at: row.last_heartbeat_at,
                lease_expires_at: row.lease_expires_at,
                idle_deadline_at: row.idle_deadline_at,
                hard_deadline_at: row.hard_deadline_at,
            })
        })
        .collect()
}

fn running_snapshot_from_model(row: turn_item_attempt::Model) -> Result<RunningAttemptSnapshot> {
    let execution_class = match row.execution_class.as_deref() {
        Some(value) => Some(turn_item_execution_class_from_db(value).with_context(|| {
            format!("attempt `{}` has unknown execution class `{value}`", row.id)
        })?),
        None => None,
    };
    Ok(RunningAttemptSnapshot {
        id: row.id,
        turn_id: row.turn_id,
        item_id: row.item_id,
        item_type: turn_item_type_from_db(row.item_type.as_str())
            .unwrap_or(TurnItemType::DynamicToolCall),
        execution_class,
        attempt_number: row.attempt_number,
        started_at: row.started_at,
        started_event_sequence: row.started_event_sequence,
        last_heartbeat_at: row.last_heartbeat_at,
        lease_expires_at: row.lease_expires_at,
        idle_deadline_at: row.idle_deadline_at,
        hard_deadline_at: row.hard_deadline_at,
    })
}

pub async fn suppress_timeout_recovery<C: ConnectionTrait>(
    db: &C,
    attempt_id: &str,
    reason: &str,
    context_json: String,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_item_attempt::Entity::update_many()
        .col_expr(
            turn_item_attempt::Column::RecoverySuppressedReason,
            Expr::value(Some(reason.to_owned())),
        )
        .col_expr(
            turn_item_attempt::Column::RecoverySuppressedAt,
            Expr::value(Some(updated_at)),
        )
        .col_expr(
            turn_item_attempt::Column::RecoverySuppressionContextJson,
            Expr::value(Some(context_json)),
        )
        .col_expr(
            turn_item_attempt::Column::UpdatedAt,
            Expr::value(updated_at),
        )
        .filter(turn_item_attempt::Column::Id.eq(attempt_id.to_owned()))
        .filter(turn_item_attempt::Column::Status.eq(ATTEMPT_STATUS_TIMED_OUT))
        .filter(turn_item_attempt::Column::RecoveryAction.is_null())
        .filter(turn_item_attempt::Column::RecoverySuppressedReason.is_null())
        .exec(db)
        .await
        .with_context(|| format!("failed to suppress timeout recovery for attempt `{attempt_id}`"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_recovery_action<C: ConnectionTrait>(
    db: &C,
    attempt_id: &str,
    recovery_action: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_item_attempt::Entity::update_many()
        .col_expr(
            turn_item_attempt::Column::RecoveryAction,
            Expr::value(Some(recovery_action.to_owned())),
        )
        .col_expr(
            turn_item_attempt::Column::UpdatedAt,
            Expr::value(updated_at),
        )
        .filter(turn_item_attempt::Column::Id.eq(attempt_id.to_owned()))
        .filter(turn_item_attempt::Column::RecoveryAction.is_null())
        .exec(db)
        .await
        .with_context(|| format!("failed to mark recovery action for attempt `{attempt_id}`"))?
        .rows_affected
        > 0;

    Ok(affected)
}

async fn next_attempt_number<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<i64> {
    let max_attempt = turn_item_attempt::Entity::find()
        .filter(turn_item_attempt::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item_attempt::Column::ItemId.eq(item_id.to_owned()))
        .select_only()
        .column_as(
            turn_item_attempt::Column::AttemptNumber.max(),
            "max_attempt",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to query max attempt_number for turn item")?
        .flatten()
        .unwrap_or(0);

    Ok(max_attempt.saturating_add(1))
}

async fn latest_running_attempt<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<turn_item_attempt::Model>> {
    turn_item_attempt::Entity::find()
        .filter(turn_item_attempt::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item_attempt::Column::ItemId.eq(item_id.to_owned()))
        .filter(
            turn_item_attempt::Column::Status.eq(turn_item_attempt_status_to_db(
                TurnItemAttemptStatus::Running,
            )),
        )
        .order_by_desc(turn_item_attempt::Column::AttemptNumber)
        .one(db)
        .await
        .context("failed to query latest running attempt")
}
