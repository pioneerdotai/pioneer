use anyhow::{Context, Result};
use pioneer_entity::turn_event_delivery;
use pioneer_protocol::generate_id;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::ExprTrait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

use crate::events::{AppendedTurnEvent, TurnEventPayload};

pub const CONSUMER_LIVE_NOTIFICATION: &str = "live_notification";
pub const CONSUMER_THREAD_EPISODIC: &str = "thread_episodic";
pub const DELIVERY_STATUS_PENDING: &str = "pending";
pub const DELIVERY_STATUS_DELIVERING: &str = "delivering";
pub const DELIVERY_STATUS_FAILED: &str = "failed";
pub const DELIVERY_STATUS_DELIVERED: &str = "delivered";
pub const DELIVERY_STATUS_EXHAUSTED: &str = "exhausted";

const DB_ID_LEN: usize = 21;

#[derive(Debug, Clone)]
pub struct ClaimedTurnEventDelivery {
    pub row: turn_event_delivery::Model,
    pub claim_token: String,
}

pub async fn insert_pending_for_event<C: ConnectionTrait>(
    db: &C,
    event: &AppendedTurnEvent,
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    if event_requires_live_delivery(&event.payload) {
        insert_pending(db, event, CONSUMER_LIVE_NOTIFICATION, created_at).await?;
    }
    if matches!(event.payload, TurnEventPayload::ItemCompleted(_)) {
        insert_pending(db, event, CONSUMER_THREAD_EPISODIC, created_at).await?;
    }
    Ok(())
}

fn event_requires_live_delivery(event: &TurnEventPayload) -> bool {
    matches!(
        event,
        TurnEventPayload::ItemStarted(_)
            | TurnEventPayload::ItemCompleted(_)
            | TurnEventPayload::ItemUpdated(_)
            | TurnEventPayload::ItemTimeoutDetected(_)
            | TurnEventPayload::ItemRecoveryOpened(_)
            | TurnEventPayload::ItemRecoveryAttached(_)
            | TurnEventPayload::ItemRetryScheduled(_)
            | TurnEventPayload::ItemRetryAttemptStarted(_)
            | TurnEventPayload::ItemRecoverySucceeded(_)
            | TurnEventPayload::ItemRecoveryExhausted(_)
            | TurnEventPayload::ItemToolRetryScheduled(_)
            | TurnEventPayload::ItemToolRetryResolved(_)
            | TurnEventPayload::ItemToolRetryExhausted(_)
            | TurnEventPayload::TurnToolLoopBudgetExceeded(_)
            | TurnEventPayload::TurnExecutionWindowStarted(_)
            | TurnEventPayload::TurnExecutionWindowExhausted(_)
            | TurnEventPayload::TurnExecutionWindowCheckpointed(_)
            | TurnEventPayload::TurnExecutionWindowContinued(_)
            | TurnEventPayload::TurnExecutionWindowBlocked(_)
            | TurnEventPayload::TurnCompleted(_)
            | TurnEventPayload::TurnFailed(_)
            | TurnEventPayload::TurnBlocked(_)
    )
}

pub async fn claim_due<C: ConnectionTrait>(
    db: &C,
    consumer: &str,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<ClaimedTurnEventDelivery>> {
    let due = Condition::all()
        .add(turn_event_delivery::Column::Consumer.eq(consumer.to_owned()))
        .add(
            turn_event_delivery::Column::Status
                .is_in([DELIVERY_STATUS_PENDING, DELIVERY_STATUS_FAILED]),
        )
        .add(turn_event_delivery::Column::NextRunAt.lte(now));
    let expired = Condition::all()
        .add(turn_event_delivery::Column::Consumer.eq(consumer.to_owned()))
        .add(turn_event_delivery::Column::Status.eq(DELIVERY_STATUS_DELIVERING))
        .add(turn_event_delivery::Column::ClaimExpiresAt.lte(now));
    let candidates = turn_event_delivery::Entity::find()
        .filter(Condition::any().add(due.clone()).add(expired.clone()))
        .order_by_asc(turn_event_delivery::Column::TurnId)
        .order_by_asc(turn_event_delivery::Column::Sequence)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list due turn event deliveries")?;

    let mut claimed = Vec::new();
    for candidate in candidates {
        let predecessor_pending = turn_event_delivery::Entity::find()
            .filter(turn_event_delivery::Column::Consumer.eq(consumer.to_owned()))
            .filter(turn_event_delivery::Column::TurnId.eq(candidate.turn_id.clone()))
            .filter(turn_event_delivery::Column::Sequence.lt(candidate.sequence))
            .filter(
                turn_event_delivery::Column::Status
                    .is_not_in([DELIVERY_STATUS_DELIVERED, DELIVERY_STATUS_EXHAUSTED]),
            )
            .one(db)
            .await
            .context("failed to check turn event delivery predecessor")?
            .is_some();
        if predecessor_pending {
            continue;
        }
        let claim_token = generate_id(DB_ID_LEN);
        let affected = turn_event_delivery::Entity::update_many()
            .col_expr(
                turn_event_delivery::Column::Status,
                sea_orm::sea_query::Expr::value(DELIVERY_STATUS_DELIVERING.to_owned()),
            )
            .col_expr(
                turn_event_delivery::Column::ClaimToken,
                sea_orm::sea_query::Expr::value(Some(claim_token.clone())),
            )
            .col_expr(
                turn_event_delivery::Column::ClaimExpiresAt,
                sea_orm::sea_query::Expr::value(Some(claim_expires_at)),
            )
            .col_expr(
                turn_event_delivery::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now),
            )
            .filter(turn_event_delivery::Column::Id.eq(candidate.id.clone()))
            .filter(Condition::any().add(due.clone()).add(expired.clone()))
            .exec(db)
            .await
            .context("failed to claim turn event delivery")?
            .rows_affected;
        if affected == 0 {
            continue;
        }
        if let Some(row) = turn_event_delivery::Entity::find_by_id(candidate.id)
            .one(db)
            .await
            .context("failed to reload claimed turn event delivery")?
        {
            claimed.push(ClaimedTurnEventDelivery { row, claim_token });
        }
    }
    Ok(claimed)
}

pub async fn mark_delivered<C: ConnectionTrait>(
    db: &C,
    id: &str,
    claim_token: &str,
    delivered_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_event_delivery::Entity::update_many()
        .col_expr(
            turn_event_delivery::Column::Status,
            sea_orm::sea_query::Expr::value(DELIVERY_STATUS_DELIVERED.to_owned()),
        )
        .col_expr(
            turn_event_delivery::Column::LastError,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_delivery::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_delivery::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_event_delivery::Column::DeliveredAt,
            sea_orm::sea_query::Expr::value(Some(delivered_at)),
        )
        .col_expr(
            turn_event_delivery::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(delivered_at),
        )
        .filter(turn_event_delivery::Column::Id.eq(id.to_owned()))
        .filter(turn_event_delivery::Column::Status.eq(DELIVERY_STATUS_DELIVERING))
        .filter(turn_event_delivery::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .context("failed to mark turn event delivery delivered")?
        .rows_affected;
    Ok(affected == 1)
}

pub async fn mark_failed<C: ConnectionTrait>(
    db: &C,
    id: &str,
    claim_token: &str,
    error: String,
    retry_at: DateTimeWithTimeZone,
    failed_at: DateTimeWithTimeZone,
    exhausted: bool,
) -> Result<bool> {
    let status = if exhausted {
        DELIVERY_STATUS_EXHAUSTED
    } else {
        DELIVERY_STATUS_FAILED
    };
    let affected = turn_event_delivery::Entity::update_many()
        .col_expr(
            turn_event_delivery::Column::Status,
            sea_orm::sea_query::Expr::value(status.to_owned()),
        )
        .col_expr(
            turn_event_delivery::Column::AttemptCount,
            sea_orm::sea_query::Expr::col(turn_event_delivery::Column::AttemptCount).add(1),
        )
        .col_expr(
            turn_event_delivery::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(error)),
        )
        .col_expr(
            turn_event_delivery::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(retry_at),
        )
        .col_expr(
            turn_event_delivery::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_delivery::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_event_delivery::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(failed_at),
        )
        .filter(turn_event_delivery::Column::Id.eq(id.to_owned()))
        .filter(turn_event_delivery::Column::Status.eq(DELIVERY_STATUS_DELIVERING))
        .filter(turn_event_delivery::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .context("failed to mark turn event delivery failed")?
        .rows_affected;
    Ok(affected == 1)
}

async fn insert_pending<C: ConnectionTrait>(
    db: &C,
    event: &AppendedTurnEvent,
    consumer: &str,
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    turn_event_delivery::ActiveModel {
        id: Set(generate_id(DB_ID_LEN)),
        event_id: Set(event.id.clone()),
        thread_id: Set(event.thread_id.clone()),
        turn_id: Set(event.turn_id.clone()),
        sequence: Set(event.sequence),
        consumer: Set(consumer.to_owned()),
        status: Set(DELIVERY_STATUS_PENDING.to_owned()),
        attempt_count: Set(0),
        last_error: Set(None),
        next_run_at: Set(created_at),
        claim_token: Set(None),
        claim_expires_at: Set(None),
        delivered_at: Set(None),
        created_at: Set(created_at),
        updated_at: Set(created_at),
    }
    .insert(db)
    .await
    .with_context(|| {
        format!(
            "failed to enqueue `{consumer}` delivery for turn event `{}`",
            event.id
        )
    })?;
    Ok(())
}
