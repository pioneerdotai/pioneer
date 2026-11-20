use anyhow::{Context, Result};
use pioneer_entity::{
    turn_event_delivery, turn_event_projection_state,
    turn_event_projection_stream_state as stream_state_entity, turn_liveness,
};
use pioneer_protocol::generate_id;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

pub const PROJECTION_STATUS_PENDING: &str = "pending";
pub const PROJECTION_STATUS_PROJECTING: &str = "projecting";
pub const PROJECTION_STATUS_PROJECTED: &str = "projected";
pub const PROJECTION_STATUS_FAILED: &str = "failed";
pub const PROJECTION_STATUS_EXHAUSTED: &str = "exhausted";

const CLAIM_TOKEN_LEN: usize = 21;
#[derive(Debug, Clone)]
pub struct ClaimedTurnEventProjection {
    pub state: turn_event_projection_state::Model,
    pub claim_token: String,
}

#[derive(Debug, Clone)]
pub struct NewTurnEventProjectionState {
    pub event_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub sequence: i64,
    pub projection_context_json: String,
    pub claim_token: String,
    pub claim_expires_at: DateTimeWithTimeZone,
    pub created_at: DateTimeWithTimeZone,
}

pub async fn insert_claimed<C: ConnectionTrait>(
    db: &C,
    record: NewTurnEventProjectionState,
) -> Result<turn_event_projection_state::Model> {
    super::turn_event_projection_stream_state::ensure_healthy(
        db,
        record.thread_id.as_str(),
        record.turn_id.as_str(),
        record.created_at,
    )
    .await?;

    let active_model = turn_event_projection_state::ActiveModel {
        event_id: Set(record.event_id.clone()),
        thread_id: Set(record.thread_id),
        turn_id: Set(record.turn_id),
        sequence: Set(record.sequence),
        status: Set(PROJECTION_STATUS_PROJECTING.to_owned()),
        attempt_count: Set(0),
        last_error: Set(None),
        next_run_at: Set(record.created_at),
        claim_token: Set(Some(record.claim_token)),
        claim_expires_at: Set(Some(record.claim_expires_at)),
        projection_context_json: Set(record.projection_context_json),
        projected_at: Set(None),
        created_at: Set(record.created_at),
        updated_at: Set(record.created_at),
    };

    active_model.insert(db).await.with_context(|| {
        format!(
            "failed to insert turn_event_projection_state for event `{}`",
            record.event_id
        )
    })
}

pub async fn find_by_event_id<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
) -> Result<Option<turn_event_projection_state::Model>> {
    turn_event_projection_state::Entity::find_by_id(event_id.to_owned())
        .one(db)
        .await
        .with_context(|| {
            format!("failed to query turn_event_projection_state for event `{event_id}`")
        })
}

/// Uses the per-Turn contiguous projection watermark as the completion
/// authority while retaining the legacy receipt as an observation surface.
pub async fn is_projected<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
    turn_id: &str,
    sequence: i64,
) -> Result<bool> {
    if sequence <= 0 {
        anyhow::bail!(
            "turn event projection `{event_id}` has invalid sequence `{sequence}` for Turn `{turn_id}`"
        );
    }

    let stream = super::turn_event_projection_stream_state::find(db, turn_id)
        .await?
        .with_context(|| format!("projection stream state for Turn `{turn_id}` is missing"))?;
    let projected_by_watermark = sequence <= stream.projected_through_sequence;
    let receipt = find_by_event_id(db, event_id).await?;
    let projected_by_receipt = receipt.as_ref().is_some_and(|receipt| {
        receipt.turn_id == turn_id
            && receipt.sequence == sequence
            && receipt.status == PROJECTION_STATUS_PROJECTED
    });

    let receipt_was_compacted =
        receipt.is_none() && sequence <= stream.receipts_compacted_through_sequence;
    if projected_by_watermark != projected_by_receipt && !receipt_was_compacted {
        tracing::warn!(
            event_id,
            turn_id,
            sequence,
            projected_through_sequence = stream.projected_through_sequence,
            receipt_status = ?receipt.as_ref().map(|receipt| receipt.status.as_str()),
            "authoritative projection watermark differs from retained projection receipt"
        );
    }

    Ok(projected_by_watermark)
}

/// Same eligible page as claim discovery; diagnostic callers need only its head.
/// Workers reuse the page they already fetched instead of issuing this query.
pub async fn next_deadline<C: ConnectionTrait>(db: &C) -> Result<Option<DateTimeWithTimeZone>> {
    Ok(discover(db, 1).await?.first().map(|(_, at)| *at))
}

fn eligible_clock() -> sea_orm::sea_query::SimpleExpr {
    Expr::case(
        Expr::col(turn_event_projection_state::Column::Status).eq(PROJECTION_STATUS_PROJECTING),
        Expr::col(turn_event_projection_state::Column::ClaimExpiresAt),
    )
    .finally(Expr::col(turn_event_projection_state::Column::NextRunAt))
    .into()
}

/// A bounded ID/clock page, including future causal heads. Ordering by clock
/// ensures a future row cannot hide due work. Causal/projection guards precede
/// LIMIT; the conditional claim repeats them after this advisory read.
pub async fn discover<C: ConnectionTrait>(
    db: &C,
    limit: u64,
) -> Result<Vec<(String, DateTimeWithTimeZone)>> {
    discover_for_turns(db, limit, None).await
}

pub(crate) async fn discover_for_turns<C: ConnectionTrait>(
    db: &C,
    limit: u64,
    turn_ids: Option<&[String]>,
) -> Result<Vec<(String, DateTimeWithTimeZone)>> {
    let mut query = turn_event_projection_state::Entity::find();
    if let Some(turn_ids) = turn_ids {
        if turn_ids.is_empty() {
            return Ok(Vec::new());
        }
        query = query.filter(turn_event_projection_state::Column::TurnId.is_in(turn_ids.to_vec()));
    }
    query
        .select_only()
        .column(turn_event_projection_state::Column::EventId)
        .column_as(eligible_clock(), "eligible_at")
        .filter(next_for_healthy_stream())
        .filter(turn_event_projection_state::Column::Status.is_in([
            PROJECTION_STATUS_PENDING,
            PROJECTION_STATUS_FAILED,
            PROJECTION_STATUS_PROJECTING,
        ]))
        // In particular a NULL running lease must not fall back to next_run_at.
        .filter(eligible_clock().is_not_null())
        .order_by_asc(eligible_clock())
        .order_by_asc(turn_event_projection_state::Column::TurnId)
        .order_by_asc(turn_event_projection_state::Column::Sequence)
        .order_by_asc(turn_event_projection_state::Column::CreatedAt)
        .limit(limit)
        .into_tuple::<(String, DateTimeWithTimeZone)>()
        .all(db)
        .await
        .context("failed to discover eligible event work")
}

#[cfg(test)]
pub async fn claim_due<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<ClaimedTurnEventProjection>> {
    let candidates = list_due(db, now, limit).await?;
    claim_candidates(db, now, claim_expires_at, candidates).await
}

pub async fn claim_candidates<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    candidates: Vec<String>,
) -> Result<Vec<ClaimedTurnEventProjection>> {
    let claimable = due_at(now);
    let is_next_for_healthy_stream = next_for_healthy_stream();
    let mut claimed = Vec::new();

    for candidate in candidates {
        let claim_token = generate_id(CLAIM_TOKEN_LEN);
        let claimed_now = turn_event_projection_state::Entity::update_many()
            .col_expr(
                turn_event_projection_state::Column::Status,
                sea_orm::sea_query::Expr::value(PROJECTION_STATUS_PROJECTING.to_owned()),
            )
            .col_expr(
                turn_event_projection_state::Column::ClaimToken,
                sea_orm::sea_query::Expr::value(Some(claim_token.clone())),
            )
            .col_expr(
                turn_event_projection_state::Column::ClaimExpiresAt,
                sea_orm::sea_query::Expr::value(Some(claim_expires_at)),
            )
            .col_expr(
                turn_event_projection_state::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now),
            )
            .filter(turn_event_projection_state::Column::EventId.eq(candidate.clone()))
            .filter(claimable.clone())
            .filter(is_next_for_healthy_stream.clone())
            .exec(db)
            .await
            .with_context(|| format!("failed to claim turn event projection `{}`", candidate))?
            .rows_affected
            > 0;

        if !claimed_now {
            continue;
        }

        if let Some(state) = find_by_event_id(db, candidate.as_str()).await? {
            claimed.push(ClaimedTurnEventProjection { state, claim_token });
        }
    }

    Ok(claimed)
}

fn due_at(now: DateTimeWithTimeZone) -> Condition {
    let due_pending_or_failed = Condition::all()
        .add(
            turn_event_projection_state::Column::Status
                .is_in([PROJECTION_STATUS_PENDING, PROJECTION_STATUS_FAILED]),
        )
        .add(turn_event_projection_state::Column::NextRunAt.lte(now));
    let expired_projecting = Condition::all()
        .add(turn_event_projection_state::Column::Status.eq(PROJECTION_STATUS_PROJECTING))
        .add(turn_event_projection_state::Column::ClaimExpiresAt.lte(now));
    Condition::any()
        .add(due_pending_or_failed)
        .add(expired_projecting)
}

fn next_for_healthy_stream() -> sea_orm::sea_query::SimpleExpr {
    use sea_orm::sea_query::Query;
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from(stream_state_entity::Entity)
            .and_where(
                Expr::col((
                    stream_state_entity::Entity,
                    stream_state_entity::Column::TurnId,
                ))
                .eq(Expr::col((
                    turn_event_projection_state::Entity,
                    turn_event_projection_state::Column::TurnId,
                ))),
            )
            .and_where(
                Expr::col((
                    stream_state_entity::Entity,
                    stream_state_entity::Column::Status,
                ))
                .eq("healthy"),
            )
            .and_where(
                Expr::col((
                    turn_event_projection_state::Entity,
                    turn_event_projection_state::Column::Sequence,
                ))
                .eq(Expr::col((
                    stream_state_entity::Entity,
                    stream_state_entity::Column::ProjectedThroughSequence,
                ))
                .add(1_i64)),
            )
            .to_owned(),
    )
}

#[cfg(test)]
pub async fn list_due<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<String>> {
    Ok(discover(db, limit)
        .await?
        .into_iter()
        .filter(|(_, at)| *at <= now)
        .map(|(id, _)| id)
        .collect())
}

pub async fn is_next_projection_sequence<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    sequence: i64,
) -> Result<bool> {
    if sequence <= 0 {
        anyhow::bail!("projection sequence `{sequence}` is invalid for Turn `{turn_id}`");
    }

    let stream = super::turn_event_projection_stream_state::find(db, turn_id)
        .await?
        .with_context(|| format!("projection stream state for Turn `{turn_id}` is missing"))?;
    let is_next_by_watermark = stream.projected_through_sequence == sequence - 1;
    let incomplete_predecessor_receipts = turn_event_projection_state::Entity::find()
        .filter(turn_event_projection_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event_projection_state::Column::Sequence.lt(sequence))
        .filter(turn_event_projection_state::Column::Status.ne(PROJECTION_STATUS_PROJECTED))
        .count(db)
        .await
        .with_context(|| {
            format!(
                "failed to check turn event projection predecessors for turn `{turn_id}` sequence `{sequence}`"
            )
        })?;
    let is_next_by_receipts = incomplete_predecessor_receipts == 0;

    if is_next_by_watermark != is_next_by_receipts {
        tracing::warn!(
            turn_id,
            sequence,
            projected_through_sequence = stream.projected_through_sequence,
            incomplete_predecessor_receipts,
            "authoritative projection watermark differs from retained predecessor receipts"
        );
    }

    Ok(is_next_by_watermark)
}

/// Returns whether the canonical Turn event log extends beyond the
/// authoritative projection watermark. Retained receipts are compared only to
/// keep the phase-two rollout observable.
pub async fn has_unprojected_event<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<bool> {
    let max_sequence = super::turn_event::max_sequence_for_turn(db, turn_id).await?;
    if max_sequence == 0 {
        return Ok(false);
    }
    let stream = super::turn_event_projection_stream_state::find(db, turn_id)
        .await?
        .with_context(|| format!("projection stream state for Turn `{turn_id}` is missing"))?;
    if stream.projected_through_sequence > max_sequence {
        anyhow::bail!(
            "projection watermark `{}` is ahead of canonical sequence `{max_sequence}` for Turn `{turn_id}`",
            stream.projected_through_sequence
        );
    }

    let incomplete_receipts = turn_event_projection_state::Entity::find()
        .filter(turn_event_projection_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event_projection_state::Column::Status.ne(PROJECTION_STATUS_PROJECTED))
        .count(db)
        .await
        .with_context(|| {
            format!("failed to check incomplete turn event projections for turn `{turn_id}`")
        })?;
    let has_unprojected_by_watermark = stream.projected_through_sequence < max_sequence;
    let has_unprojected_by_receipts = incomplete_receipts > 0;

    if has_unprojected_by_watermark != has_unprojected_by_receipts {
        tracing::warn!(
            turn_id,
            max_sequence,
            projected_through_sequence = stream.projected_through_sequence,
            incomplete_receipts,
            "authoritative projection watermark differs from retained projection receipts"
        );
    }

    Ok(has_unprojected_by_watermark)
}

pub async fn mark_projected_claimed(
    db: &DatabaseTransaction,
    event_id: &str,
    turn_id: &str,
    sequence: i64,
    claim_token: &str,
    projected_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if sequence <= 0 {
        anyhow::bail!(
            "turn event projection `{event_id}` has invalid sequence `{sequence}` for Turn `{turn_id}`"
        );
    }

    let affected = turn_event_projection_state::Entity::update_many()
        .col_expr(
            turn_event_projection_state::Column::Status,
            sea_orm::sea_query::Expr::value(PROJECTION_STATUS_PROJECTED.to_owned()),
        )
        .col_expr(
            turn_event_projection_state::Column::LastError,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::ProjectedAt,
            sea_orm::sea_query::Expr::value(Some(projected_at)),
        )
        .col_expr(
            turn_event_projection_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(projected_at),
        )
        .filter(turn_event_projection_state::Column::EventId.eq(event_id.to_owned()))
        .filter(turn_event_projection_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event_projection_state::Column::Sequence.eq(sequence))
        .filter(turn_event_projection_state::Column::Status.eq(PROJECTION_STATUS_PROJECTING))
        .filter(turn_event_projection_state::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark turn event projection `{event_id}` projected"))?
        .rows_affected
        > 0;

    if affected
        && !super::turn_event_projection_stream_state::advance_projected_through(
            db,
            turn_id,
            sequence - 1,
            sequence,
            projected_at,
        )
        .await?
    {
        let stored_sequence = super::turn_event_projection_stream_state::find(db, turn_id)
            .await?
            .map(|stream| stream.projected_through_sequence);
        anyhow::bail!(
            "projection watermark for Turn `{turn_id}` did not advance atomically from `{}` through `{sequence}` while completing event `{event_id}`; stored watermark: {stored_sequence:?}",
            sequence - 1
        );
    }

    Ok(affected)
}

pub async fn release_claim_as_pending<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
    claim_token: &str,
    next_run_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_event_projection_state::Entity::update_many()
        .col_expr(
            turn_event_projection_state::Column::Status,
            sea_orm::sea_query::Expr::value(PROJECTION_STATUS_PENDING.to_owned()),
        )
        .col_expr(
            turn_event_projection_state::Column::LastError,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            turn_event_projection_state::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(updated_at),
        )
        .filter(turn_event_projection_state::Column::EventId.eq(event_id.to_owned()))
        .filter(turn_event_projection_state::Column::Status.eq(PROJECTION_STATUS_PROJECTING))
        .filter(turn_event_projection_state::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to release turn event projection `{event_id}` as pending")
        })?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_failed_claimed<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
    claim_token: &str,
    last_error: String,
    next_run_at: DateTimeWithTimeZone,
    failed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    mark_claimed_failure_with_status(
        db,
        event_id,
        claim_token,
        PROJECTION_STATUS_FAILED,
        last_error,
        next_run_at,
        failed_at,
    )
    .await
}

pub async fn mark_exhausted_claimed<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
    claim_token: &str,
    last_error: String,
    exhausted_at: DateTimeWithTimeZone,
) -> Result<bool> {
    mark_claimed_failure_with_status(
        db,
        event_id,
        claim_token,
        PROJECTION_STATUS_EXHAUSTED,
        last_error,
        exhausted_at,
        exhausted_at,
    )
    .await
}

pub async fn reset_exhausted_as_pending<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
    turn_id: &str,
    next_run_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_event_projection_state::Entity::update_many()
        .col_expr(
            turn_event_projection_state::Column::Status,
            sea_orm::sea_query::Expr::value(PROJECTION_STATUS_PENDING.to_owned()),
        )
        .col_expr(
            turn_event_projection_state::Column::AttemptCount,
            sea_orm::sea_query::Expr::value(0),
        )
        .col_expr(
            turn_event_projection_state::Column::LastError,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            turn_event_projection_state::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::ProjectedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .filter(turn_event_projection_state::Column::EventId.eq(event_id.to_owned()))
        .filter(turn_event_projection_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event_projection_state::Column::Status.eq(PROJECTION_STATUS_EXHAUSTED))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to reset exhausted projection `{event_id}` for operator replay")
        })?
        .rows_affected
        > 0;

    Ok(affected)
}

async fn mark_claimed_failure_with_status<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
    claim_token: &str,
    status: &str,
    last_error: String,
    next_run_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_event_projection_state::Entity::update_many()
        .col_expr(
            turn_event_projection_state::Column::Status,
            sea_orm::sea_query::Expr::value(status.to_owned()),
        )
        .col_expr(
            turn_event_projection_state::Column::AttemptCount,
            sea_orm::sea_query::Expr::col(turn_event_projection_state::Column::AttemptCount).add(1),
        )
        .col_expr(
            turn_event_projection_state::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(last_error)),
        )
        .col_expr(
            turn_event_projection_state::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            turn_event_projection_state::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            turn_event_projection_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(updated_at),
        )
        .filter(turn_event_projection_state::Column::EventId.eq(event_id.to_owned()))
        .filter(turn_event_projection_state::Column::Status.eq(PROJECTION_STATUS_PROJECTING))
        .filter(turn_event_projection_state::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to mark turn event projection `{event_id}` as `{status}`")
        })?
        .rows_affected
        > 0;

    Ok(affected)
}
