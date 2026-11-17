use anyhow::{Context, Result};
use pioneer_entity::turn_event_delivery;
use pioneer_protocol::generate_id;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, Query, SimpleExpr};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, QueryTrait, Set,
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
/// One finite quantum; also bounds UPDATE bind counts and payload row reads.
pub const DELIVERY_BATCH_LIMIT: usize = 64;

#[derive(Debug, Clone)]
pub struct PreparedDeliveryClaim {
    pub id: String,
    pub token: String,
}

pub fn prepare_claims(candidates: Vec<String>) -> Vec<PreparedDeliveryClaim> {
    let mut seen = std::collections::HashSet::new();
    candidates
        .into_iter()
        .filter(|id| seen.insert(id.clone()))
        .take(DELIVERY_BATCH_LIMIT)
        .map(|id| PreparedDeliveryClaim {
            id,
            token: generate_id(DB_ID_LEN),
        })
        .collect()
}

#[derive(Debug, Clone)]
pub enum DeliveryOutcome {
    Delivered,
    ForegroundDeferred,
    Failed { attempt_count: i64, error: String },
}

#[derive(Debug, Clone)]
pub struct DeliverySettlement {
    pub id: String,
    pub claim_token: String,
    pub at_unix: i64,
    pub outcome: DeliveryOutcome,
}

#[derive(Debug, Clone)]
pub struct ClaimedTurnEventDelivery {
    pub row: turn_event_delivery::Model,
    pub claim_token: String,
}

/// A LIVE completion retains delivery ownership for later independent item
/// updates. A LIVE timeout retains it for recovery events in that source turn.
/// Receipts, including delivered ones, share canonical history's lifetime;
/// neither publication release nor projection-receipt compaction revokes them.
/// TaskRun occurrences keep their established presentation/delivery owner.
pub async fn independent_item_uses_live<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
) -> Result<bool> {
    use pioneer_entity::{compaction_event_revision, turn, turn_event};
    // Probe metadata by its source primary key for this turn's completions;
    // do not build an item-ID list by scanning the entire revision table.
    let item_event = Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from(compaction_event_revision::Entity)
            .and_where(
                Expr::col((
                    compaction_event_revision::Entity,
                    compaction_event_revision::Column::SourceId,
                ))
                .eq(Expr::col((turn_event::Entity, turn_event::Column::Id))),
            )
            .and_where(compaction_event_revision::Column::TurnId.eq(turn_id))
            .and_where(compaction_event_revision::Column::ItemId.eq(item_id))
            .and_where(compaction_event_revision::Column::Present.eq(1))
            .to_owned(),
    );
    let completions = turn_event::Entity::find()
        .select_only()
        .column(turn_event::Column::Id)
        .filter(turn_event::Column::TurnId.eq(turn_id))
        .filter(
            turn_event::Column::EventType.eq(pioneer_protocol::constants::events::ITEM_COMPLETED),
        )
        .filter(item_event)
        .into_query();
    let timeouts = turn_event::Entity::find()
        .select_only()
        .column(turn_event::Column::Id)
        .filter(turn_event::Column::TurnId.eq(turn_id))
        .filter(
            turn_event::Column::EventType
                .eq(pioneer_protocol::constants::events::ITEM_TIMEOUT_DETECTED),
        )
        .into_query();
    let conversations = turn::Entity::find()
        .select_only()
        .column(turn::Column::Id)
        .filter(turn::Column::Id.eq(turn_id))
        .filter(turn::Column::TurnKind.eq("conversation"))
        .into_query();
    Ok(turn_event_delivery::Entity::find()
        .select_only()
        .column(turn_event_delivery::Column::Id)
        .filter(turn_event_delivery::Column::ThreadId.eq(thread_id))
        .filter(turn_event_delivery::Column::TurnId.eq(turn_id))
        .filter(turn_event_delivery::Column::TurnId.in_subquery(conversations))
        .filter(turn_event_delivery::Column::Consumer.eq(CONSUMER_LIVE_NOTIFICATION))
        .filter(
            Condition::any()
                .add(turn_event_delivery::Column::EventId.in_subquery(completions))
                .add(turn_event_delivery::Column::EventId.in_subquery(timeouts)),
        )
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}

pub async fn insert_pending_for_event<C: ConnectionTrait>(
    db: &C,
    event: &AppendedTurnEvent,
    created_at: DateTimeWithTimeZone,
    include_live: bool,
) -> Result<()> {
    if include_live && event_requires_live_delivery(&event.payload) {
        insert_pending(db, event, CONSUMER_LIVE_NOTIFICATION, created_at).await?;
    }
    if matches!(
        event.payload,
        TurnEventPayload::ItemCompleted(_) | TurnEventPayload::ItemUpdated(_)
    ) {
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

/// Same eligible page as claim discovery; diagnostic callers need only its head.
/// Workers reuse the page they already fetched instead of issuing this query.
pub async fn next_deadline<C: ConnectionTrait>(
    db: &C,
    consumer: &str,
) -> Result<Option<DateTimeWithTimeZone>> {
    Ok(discover(db, consumer, 1).await?.first().map(|(_, at)| *at))
}

fn eligible_clock() -> sea_orm::sea_query::SimpleExpr {
    Expr::case(
        Expr::col(turn_event_delivery::Column::Status).eq(DELIVERY_STATUS_DELIVERING),
        Expr::col(turn_event_delivery::Column::ClaimExpiresAt),
    )
    .finally(Expr::col(turn_event_delivery::Column::NextRunAt))
    .into()
}

/// A bounded ID/clock page, including future causal heads. Ordering by clock
/// ensures a future row cannot hide due work. Causal/projection guards precede
/// LIMIT; the conditional claim repeats them after this advisory read.
pub async fn discover<C: ConnectionTrait>(
    db: &C,
    consumer: &str,
    limit: u64,
) -> Result<Vec<(String, DateTimeWithTimeZone)>> {
    turn_event_delivery::Entity::find()
        .select_only()
        .column(turn_event_delivery::Column::Id)
        .column_as(eligible_clock(), "eligible_at")
        .filter(no_pending_predecessor())
        .filter(projection_ready(consumer))
        .filter(turn_event_delivery::Column::Consumer.eq(consumer.to_owned()))
        .filter(turn_event_delivery::Column::Status.is_in([
            DELIVERY_STATUS_PENDING,
            DELIVERY_STATUS_FAILED,
            DELIVERY_STATUS_DELIVERING,
        ]))
        // In particular a NULL running lease must not fall back to next_run_at.
        .filter(eligible_clock().is_not_null())
        .order_by_asc(eligible_clock())
        .order_by_asc(turn_event_delivery::Column::TurnId)
        .order_by_asc(turn_event_delivery::Column::Sequence)
        .limit(std::cmp::min(limit, DELIVERY_BATCH_LIMIT as u64))
        .into_tuple::<(String, DateTimeWithTimeZone)>()
        .all(db)
        .await
        .context("failed to discover eligible event work")
}

#[cfg(test)]
pub async fn claim_due<C: ConnectionTrait>(
    db: &C,
    consumer: &str,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<ClaimedTurnEventDelivery>> {
    let candidates = list_due(db, consumer, now, limit).await?;
    claim_candidates(db, consumer, now, claim_expires_at, candidates).await
}

#[cfg(test)]
pub async fn claim_candidates<C: ConnectionTrait>(
    db: &C,
    consumer: &str,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    candidates: Vec<String>,
) -> Result<Vec<ClaimedTurnEventDelivery>> {
    claim_prepared(
        db,
        consumer,
        now,
        claim_expires_at,
        &prepare_claims(candidates),
    )
    .await
}

/// Discovery is advisory. The one UPDATE rechecks due time, projection and
/// predecessors atomically. Claiming never makes a predecessor terminal, so
/// the causal predicate remains valid while SQLite visits this statement's rows.
/// RETURNING replaces per-row reloads; every ID still has its own fresh token.
pub async fn claim_prepared<C: ConnectionTrait>(
    db: &C,
    consumer: &str,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    candidates: &[PreparedDeliveryClaim],
) -> Result<Vec<ClaimedTurnEventDelivery>> {
    anyhow::ensure!(
        candidates.len() <= DELIVERY_BATCH_LIMIT,
        "delivery claim batch exceeds bound"
    );
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let mut tokens = sea_orm::sea_query::CaseStatement::new();
    for candidate in candidates {
        tokens = tokens.case(
            turn_event_delivery::Column::Id.eq(candidate.id.clone()),
            candidate.token.clone(),
        );
    }
    let rows = turn_event_delivery::Entity::update_many()
        .col_expr(
            turn_event_delivery::Column::Status,
            Expr::value(DELIVERY_STATUS_DELIVERING),
        )
        .col_expr(
            turn_event_delivery::Column::ClaimToken,
            tokens.finally(Option::<String>::None).into(),
        )
        .col_expr(
            turn_event_delivery::Column::ClaimExpiresAt,
            Expr::value(Some(claim_expires_at)),
        )
        .col_expr(turn_event_delivery::Column::UpdatedAt, Expr::value(now))
        .filter(turn_event_delivery::Column::Id.is_in(candidates.iter().map(|c| c.id.clone())))
        .filter(due_at(consumer, now))
        .filter(no_pending_predecessor())
        .filter(projection_ready(consumer))
        .exec_with_returning(db)
        .await
        .context("failed to claim turn event delivery batch")?;
    // SQLite RETURNING order is unspecified. Restore discovery order.
    let mut rows = rows
        .into_iter()
        .map(|r| (r.id.clone(), r))
        .collect::<std::collections::HashMap<_, _>>();
    Ok(candidates
        .iter()
        .filter_map(|c| {
            rows.remove(&c.id).map(|row| ClaimedTurnEventDelivery {
                row,
                claim_token: c.token.clone(),
            })
        })
        .collect())
}

/// Settle independent claims in one statement, retaining each exact ID/token
/// fence. No external effects occur under the writer. A cancelled/unknown write
/// retains the ordinary WorkChange/lease recovery contract at the CrudStore.
pub async fn settle_batch<C: ConnectionTrait>(
    db: &C,
    settlements: &[DeliverySettlement],
) -> Result<u64> {
    use sea_orm::sea_query::CaseStatement;
    use turn_event_delivery::Column as Col;
    anyhow::ensure!(
        settlements.len() <= DELIVERY_BATCH_LIMIT,
        "delivery settlement batch exceeds bound"
    );
    if settlements.is_empty() {
        return Ok(0);
    }
    let mut seen = std::collections::HashSet::new();
    let mut guard = Condition::any();
    let mut status = CaseStatement::new();
    let mut errors = CaseStatement::new();
    let mut attempts = CaseStatement::new();
    let mut next = CaseStatement::new();
    let mut delivered = CaseStatement::new();
    let mut updated = CaseStatement::new();
    for settlement in settlements {
        anyhow::ensure!(seen.insert(&settlement.id), "duplicate delivery settlement");
        let id = Col::Id.eq(settlement.id.clone());
        let mut owned = Condition::all()
            .add(id.clone())
            .add(Col::ClaimToken.eq(settlement.claim_token.clone()));
        let at = crate::util::unix_to_datetime(settlement.at_unix);
        let (state, error, increment, next_at, delivered_at) = match &settlement.outcome {
            DeliveryOutcome::Delivered => (DELIVERY_STATUS_DELIVERED, None, false, None, Some(at)),
            DeliveryOutcome::ForegroundDeferred => {
                owned = owned.add(Col::Consumer.eq(CONSUMER_LIVE_NOTIFICATION));
                (
                    DELIVERY_STATUS_PENDING,
                    None,
                    false,
                    Some(at + chrono::Duration::seconds(1)),
                    None,
                )
            }
            DeliveryOutcome::Failed {
                attempt_count,
                error,
            } => {
                let exhausted = attempt_count.saturating_add(1) >= 10;
                let delay = std::cmp::min(
                    2_i64.saturating_pow(u32::try_from((*attempt_count).clamp(0, 8)).unwrap_or(8)),
                    300,
                );
                (
                    if exhausted {
                        DELIVERY_STATUS_EXHAUSTED
                    } else {
                        DELIVERY_STATUS_FAILED
                    },
                    Some(error.clone()),
                    true,
                    Some(at + chrono::Duration::seconds(delay)),
                    None,
                )
            }
        };
        guard = guard.add(owned);
        status = status.case(id.clone(), state);
        // Deferral preserves the preceding attempt/error budget exactly.
        let error_expr: SimpleExpr =
            if matches!(&settlement.outcome, DeliveryOutcome::ForegroundDeferred) {
                Expr::col(Col::LastError).into()
            } else {
                Expr::val(error).into()
            };
        errors = errors.case(id.clone(), error_expr);
        attempts = attempts.case(
            id.clone(),
            if increment {
                Expr::col(Col::AttemptCount).add(1)
            } else {
                Expr::col(Col::AttemptCount).into()
            },
        );
        next = next.case(
            id.clone(),
            next_at
                .map(|at| -> SimpleExpr { Expr::val(at).into() })
                .unwrap_or_else(|| Expr::col(Col::NextRunAt).into()),
        );
        delivered = delivered.case(
            id.clone(),
            delivered_at
                .map(|at| -> SimpleExpr { Expr::val(at).into() })
                .unwrap_or_else(|| Expr::col(Col::DeliveredAt).into()),
        );
        updated = updated.case(id, at);
    }
    Ok(turn_event_delivery::Entity::update_many()
        .col_expr(Col::Status, status.finally(Expr::col(Col::Status)).into())
        .col_expr(
            Col::LastError,
            errors.finally(Expr::col(Col::LastError)).into(),
        )
        .col_expr(
            Col::AttemptCount,
            attempts.finally(Expr::col(Col::AttemptCount)).into(),
        )
        .col_expr(
            Col::NextRunAt,
            next.finally(Expr::col(Col::NextRunAt)).into(),
        )
        .col_expr(
            Col::DeliveredAt,
            delivered.finally(Expr::col(Col::DeliveredAt)).into(),
        )
        .col_expr(
            Col::UpdatedAt,
            updated.finally(Expr::col(Col::UpdatedAt)).into(),
        )
        .col_expr(Col::ClaimToken, Expr::value(Option::<String>::None))
        .col_expr(
            Col::ClaimExpiresAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .filter(Col::Status.eq(DELIVERY_STATUS_DELIVERING))
        .filter(guard)
        .exec(db)
        .await
        .context("failed to settle turn event delivery batch")?
        .rows_affected)
}

fn projection_ready(consumer: &str) -> SimpleExpr {
    // Canonical append can commit before its read-model projection. The
    // contiguous watermark is authoritative even after receipt compaction
    // (see turn_event_projection_state::is_projected). Quarantine does not
    // invalidate an already projected prefix.
    if consumer == CONSUMER_THREAD_EPISODIC {
        use pioneer_entity::turn_event_projection_stream_state as stream;
        Expr::exists(
            Query::select()
                .expr(Expr::val(1_i64))
                .from(stream::Entity)
                .and_where(
                    Expr::col((stream::Entity, stream::Column::TurnId)).eq(Expr::col((
                        turn_event_delivery::Entity,
                        turn_event_delivery::Column::TurnId,
                    ))),
                )
                .and_where(
                    Expr::col((stream::Entity, stream::Column::ThreadId)).eq(Expr::col((
                        turn_event_delivery::Entity,
                        turn_event_delivery::Column::ThreadId,
                    ))),
                )
                .and_where(
                    Expr::col((
                        turn_event_delivery::Entity,
                        turn_event_delivery::Column::Sequence,
                    ))
                    .gt(0_i64),
                )
                .and_where(
                    Expr::col((
                        turn_event_delivery::Entity,
                        turn_event_delivery::Column::Sequence,
                    ))
                    .lte(Expr::col((
                        stream::Entity,
                        stream::Column::ProjectedThroughSequence,
                    ))),
                )
                .to_owned(),
        )
    } else {
        Expr::val(true).into()
    }
}

#[cfg(test)]
pub async fn list_due<C: ConnectionTrait>(
    db: &C,
    consumer: &str,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<String>> {
    Ok(discover(db, consumer, limit)
        .await?
        .into_iter()
        .filter(|(_, at)| *at <= now)
        .map(|(id, _)| id)
        .collect())
}

fn due_at(consumer: &str, now: DateTimeWithTimeZone) -> Condition {
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
    Condition::any().add(due).add(expired)
}

fn no_pending_predecessor() -> SimpleExpr {
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(turn_event_delivery::Entity, "predecessor")
            .and_where(
                Expr::col(("predecessor", turn_event_delivery::Column::Consumer)).eq(Expr::col((
                    turn_event_delivery::Entity,
                    turn_event_delivery::Column::Consumer,
                ))),
            )
            .and_where(
                Expr::col(("predecessor", turn_event_delivery::Column::TurnId)).eq(Expr::col((
                    turn_event_delivery::Entity,
                    turn_event_delivery::Column::TurnId,
                ))),
            )
            .and_where(
                Expr::col(("predecessor", turn_event_delivery::Column::Sequence)).lt(Expr::col((
                    turn_event_delivery::Entity,
                    turn_event_delivery::Column::Sequence,
                ))),
            )
            // Fixed literals must match the partial index predicate. Keep NOT
            // IN semantics: unknown nonterminal statuses also block delivery.
            .and_where(Expr::cust(
                "predecessor.status NOT IN ('delivered', 'exhausted')",
            ))
            .to_owned(),
    )
    .not()
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

/// A foreground start dependency is not a delivery attempt. Release its claim
/// with a short eligibility delay, preserving the error/attempt budget.
pub async fn defer_for_foreground_start<C: ConnectionTrait>(
    db: &C,
    id: &str,
    claim_token: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_event_delivery::Entity::update_many()
        .col_expr(
            turn_event_delivery::Column::Status,
            Expr::value(DELIVERY_STATUS_PENDING),
        )
        .col_expr(
            turn_event_delivery::Column::NextRunAt,
            Expr::value(now + chrono::Duration::seconds(1)),
        )
        .col_expr(
            turn_event_delivery::Column::ClaimToken,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_delivery::Column::ClaimExpiresAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(turn_event_delivery::Column::UpdatedAt, Expr::value(now))
        .filter(turn_event_delivery::Column::Id.eq(id.to_owned()))
        .filter(turn_event_delivery::Column::Consumer.eq(CONSUMER_LIVE_NOTIFICATION))
        .filter(turn_event_delivery::Column::Status.eq(DELIVERY_STATUS_DELIVERING))
        .filter(turn_event_delivery::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await?
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
