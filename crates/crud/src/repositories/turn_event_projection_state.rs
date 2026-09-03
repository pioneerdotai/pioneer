use anyhow::{Context, Result};
use pioneer_entity::{
    turn_event_delivery, turn_event_projection_state,
    turn_event_projection_stream_state as stream_state_entity, turn_liveness,
};
use pioneer_protocol::generate_id;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set, Statement,
};

pub const PROJECTION_STATUS_PENDING: &str = "pending";
pub const PROJECTION_STATUS_PROJECTING: &str = "projecting";
pub const PROJECTION_STATUS_PROJECTED: &str = "projected";
pub const PROJECTION_STATUS_FAILED: &str = "failed";
pub const PROJECTION_STATUS_EXHAUSTED: &str = "exhausted";

const CLAIM_TOKEN_LEN: usize = 21;
const OBSERVE_PROJECTED_WATERMARK_SQL: &str = r#"
WITH ordered_events AS (
    SELECT
        event.id,
        event.turn_id,
        event.sequence,
        ROW_NUMBER() OVER (ORDER BY event.sequence) AS expected_sequence
    FROM turn_event AS event
    WHERE event.turn_id = ?
      AND event.sequence > 0
),
evaluated_events AS (
    SELECT
        ordered.sequence,
        ordered.expected_sequence,
        EXISTS (
            SELECT 1
            FROM turn_event_projection_state AS projection
            WHERE projection.event_id = ordered.id
              AND projection.turn_id = ordered.turn_id
              AND projection.sequence = ordered.sequence
              AND projection.status = 'projected'
        ) AS has_projected_receipt
    FROM ordered_events AS ordered
)
SELECT COALESCE(
    MIN(
        CASE
            WHEN sequence <> expected_sequence OR has_projected_receipt = 0
                THEN expected_sequence - 1
        END
    ),
    COUNT(*)
) AS projected_through_sequence
FROM evaluated_events
"#;

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

#[derive(Debug, Clone, PartialEq, Eq, FromQueryResult)]
pub struct TurnEventProjectionStreamBackfillCandidate {
    pub turn_id: String,
    pub first_thread_id: String,
    pub last_thread_id: String,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionWatermarkObservation {
    pub observed_projected_through_sequence: i64,
    pub stored_projected_through_sequence: i64,
    pub watermark_advanced: bool,
}

impl ProjectionWatermarkObservation {
    pub fn matches(&self) -> bool {
        self.observed_projected_through_sequence == self.stored_projected_through_sequence
    }
}

pub async fn list_stream_backfill_candidates<C: ConnectionTrait>(
    db: &C,
    after_turn_id: Option<&str>,
    limit: u64,
) -> Result<Vec<TurnEventProjectionStreamBackfillCandidate>> {
    let mut query = turn_event_projection_state::Entity::find()
        .select_only()
        .column(turn_event_projection_state::Column::TurnId)
        .column_as(
            turn_event_projection_state::Column::ThreadId.min(),
            "first_thread_id",
        )
        .column_as(
            turn_event_projection_state::Column::ThreadId.max(),
            "last_thread_id",
        )
        .column_as(
            turn_event_projection_state::Column::CreatedAt.min(),
            "created_at",
        );
    if let Some(after_turn_id) = after_turn_id {
        query =
            query.filter(turn_event_projection_state::Column::TurnId.gt(after_turn_id.to_owned()));
    }

    query
        .group_by(turn_event_projection_state::Column::TurnId)
        .order_by_asc(turn_event_projection_state::Column::TurnId)
        .limit(std::cmp::max(limit, 1))
        .into_model::<TurnEventProjectionStreamBackfillCandidate>()
        .all(db)
        .await
        .context("failed to list projection stream state backfill candidates")
}

pub async fn list_exhausted_causal_heads_for_turns<C: ConnectionTrait>(
    db: &C,
    turn_ids: Vec<String>,
) -> Result<Vec<turn_event_projection_state::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }
    let is_causal_head = sea_orm::sea_query::Expr::cust(
        "NOT EXISTS (\
            SELECT 1 FROM turn_event_projection_state AS predecessor \
            WHERE predecessor.turn_id = turn_event_projection_state.turn_id \
              AND predecessor.sequence < turn_event_projection_state.sequence \
              AND predecessor.status <> 'projected'\
        )",
    );

    turn_event_projection_state::Entity::find()
        .filter(turn_event_projection_state::Column::TurnId.is_in(turn_ids))
        .filter(turn_event_projection_state::Column::Status.eq(PROJECTION_STATUS_EXHAUSTED))
        .filter(is_causal_head)
        .order_by_asc(turn_event_projection_state::Column::TurnId)
        .order_by_asc(turn_event_projection_state::Column::Sequence)
        .order_by_asc(turn_event_projection_state::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list exhausted projection stream causal heads")
}

/// Canonicalizes the narrow legacy corruption produced by final CLI agent-diff
/// snapshots that used the reusable session thread instead of their Turn's
/// durable thread binding. Unsupported cross-thread events remain hard errors;
/// this repair must never guess ownership for arbitrary event payloads.
pub async fn repair_legacy_agent_diff_thread_owners_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    canonical_thread_id: &str,
) -> Result<usize> {
    let mismatches = turn_event_projection_state::Entity::find()
        .filter(turn_event_projection_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event_projection_state::Column::ThreadId.ne(canonical_thread_id.to_owned()))
        .order_by_asc(turn_event_projection_state::Column::Sequence)
        .order_by_asc(turn_event_projection_state::Column::EventId)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to load cross-thread projection events for Turn `{turn_id}`")
        })?;

    for mismatch in &mismatches {
        super::turn_event::repair_legacy_agent_diff_thread_owner(
            db,
            mismatch.event_id.as_str(),
            turn_id,
            mismatch.thread_id.as_str(),
            canonical_thread_id,
        )
        .await?;

        let changed = turn_event_projection_state::Entity::update_many()
            .col_expr(
                turn_event_projection_state::Column::ThreadId,
                Expr::value(canonical_thread_id.to_owned()),
            )
            .filter(turn_event_projection_state::Column::EventId.eq(mismatch.event_id.clone()))
            .filter(turn_event_projection_state::Column::TurnId.eq(turn_id.to_owned()))
            .filter(turn_event_projection_state::Column::ThreadId.eq(mismatch.thread_id.clone()))
            .exec(db)
            .await
            .with_context(|| {
                format!(
                    "failed to repair projection owner for event `{}`",
                    mismatch.event_id
                )
            })?
            .rows_affected;
        if changed != 1 {
            anyhow::bail!(
                "projection event `{}` changed during ownership repair",
                mismatch.event_id
            );
        }

        turn_event_delivery::Entity::update_many()
            .col_expr(
                turn_event_delivery::Column::ThreadId,
                Expr::value(canonical_thread_id.to_owned()),
            )
            .filter(turn_event_delivery::Column::EventId.eq(mismatch.event_id.clone()))
            .filter(turn_event_delivery::Column::TurnId.eq(turn_id.to_owned()))
            .filter(turn_event_delivery::Column::ThreadId.eq(mismatch.thread_id.clone()))
            .exec(db)
            .await
            .with_context(|| {
                format!(
                    "failed to repair delivery owner for event `{}`",
                    mismatch.event_id
                )
            })?;
    }

    if mismatches.is_empty() {
        return Ok(0);
    }

    if let Some(remaining) = turn_event_projection_state::Entity::find()
        .filter(turn_event_projection_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event_projection_state::Column::ThreadId.ne(canonical_thread_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to verify repaired projection stream for Turn `{turn_id}`")
        })?
    {
        anyhow::bail!(
            "projection event `{}` still has a non-canonical thread after repair",
            remaining.event_id
        );
    }

    turn_liveness::Entity::update_many()
        .col_expr(
            turn_liveness::Column::ThreadId,
            Expr::value(canonical_thread_id.to_owned()),
        )
        .filter(turn_liveness::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_liveness::Column::ThreadId.ne(canonical_thread_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to repair liveness owner for Turn `{turn_id}`"))?;

    stream_state_entity::Entity::update_many()
        .col_expr(
            stream_state_entity::Column::ThreadId,
            Expr::value(canonical_thread_id.to_owned()),
        )
        .filter(stream_state_entity::Column::TurnId.eq(turn_id.to_owned()))
        .filter(stream_state_entity::Column::ThreadId.ne(canonical_thread_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to repair stream-state owner for Turn `{turn_id}`"))?;

    Ok(mismatches.len())
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

pub async fn claim_due<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<ClaimedTurnEventProjection>> {
    let due_pending_or_failed = Condition::all()
        .add(
            turn_event_projection_state::Column::Status
                .is_in([PROJECTION_STATUS_PENDING, PROJECTION_STATUS_FAILED]),
        )
        .add(turn_event_projection_state::Column::NextRunAt.lte(now));
    let expired_projecting = Condition::all()
        .add(turn_event_projection_state::Column::Status.eq(PROJECTION_STATUS_PROJECTING))
        .add(turn_event_projection_state::Column::ClaimExpiresAt.lte(now));
    // Claim only the causal head of each Turn stream. Without this predicate,
    // every successor behind one exhausted record remains due and can occupy
    // the whole global batch forever. Healthy Turn streams must remain
    // independently replayable even when another stream requires operator
    // repair.
    let is_causal_head = sea_orm::sea_query::Expr::cust(
        "NOT EXISTS (\
            SELECT 1 FROM turn_event_projection_state AS predecessor \
            WHERE predecessor.turn_id = turn_event_projection_state.turn_id \
              AND predecessor.sequence < turn_event_projection_state.sequence \
              AND predecessor.status <> 'projected'\
        )",
    );
    let stream_is_healthy = sea_orm::sea_query::Expr::cust(
        "NOT EXISTS (\
            SELECT 1 FROM turn_event_projection_stream_state AS stream_state \
            WHERE stream_state.turn_id = turn_event_projection_state.turn_id \
              AND stream_state.status = 'quarantined'\
        )",
    );

    let candidates = turn_event_projection_state::Entity::find()
        .filter(
            Condition::any()
                .add(due_pending_or_failed.clone())
                .add(expired_projecting.clone()),
        )
        .filter(is_causal_head)
        .filter(stream_is_healthy.clone())
        .order_by_asc(turn_event_projection_state::Column::NextRunAt)
        .order_by_asc(turn_event_projection_state::Column::TurnId)
        .order_by_asc(turn_event_projection_state::Column::Sequence)
        .order_by_asc(turn_event_projection_state::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to load due turn event projections")?;

    let mut claimed = Vec::new();

    for candidate in candidates {
        let claim_token = generate_id(CLAIM_TOKEN_LEN);
        let claimable = Condition::any()
            .add(due_pending_or_failed.clone())
            .add(expired_projecting.clone());
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
            .filter(turn_event_projection_state::Column::EventId.eq(candidate.event_id.clone()))
            .filter(claimable)
            .filter(stream_is_healthy.clone())
            .exec(db)
            .await
            .with_context(|| {
                format!(
                    "failed to claim turn event projection `{}`",
                    candidate.event_id
                )
            })?
            .rows_affected
            > 0;

        if !claimed_now {
            continue;
        }

        if let Some(state) = find_by_event_id(db, candidate.event_id.as_str()).await? {
            claimed.push(ClaimedTurnEventProjection { state, claim_token });
        }
    }

    Ok(claimed)
}

pub async fn has_unprojected_predecessor<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    sequence: i64,
) -> Result<bool> {
    let count = turn_event_projection_state::Entity::find()
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

    Ok(count > 0)
}

/// Returns whether this Turn already owns any durable event whose authoritative
/// projection is incomplete. Every existing event is a causal predecessor of a
/// finalization event that has not been appended yet.
pub async fn has_unprojected_event<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<bool> {
    let count = turn_event_projection_state::Entity::find()
        .filter(turn_event_projection_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event_projection_state::Column::Status.ne(PROJECTION_STATUS_PROJECTED))
        .count(db)
        .await
        .with_context(|| {
            format!("failed to check incomplete turn event projections for turn `{turn_id}`")
        })?;

    Ok(count > 0)
}

pub async fn mark_projected_claimed<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
    turn_id: &str,
    sequence: i64,
    claim_token: &str,
    projected_at: DateTimeWithTimeZone,
) -> Result<bool> {
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
        .filter(turn_event_projection_state::Column::Status.eq(PROJECTION_STATUS_PROJECTING))
        .filter(turn_event_projection_state::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark turn event projection `{event_id}` projected"))?
        .rows_affected
        > 0;

    if affected {
        // Observation-only rollout: receipts remain authoritative. This shadow
        // value advances only across an already projected contiguous prefix.
        match super::turn_event_projection_stream_state::advance_projected_through(
            db,
            turn_id,
            sequence.saturating_sub(1),
            sequence,
            projected_at,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => tracing::warn!(
                event_id,
                turn_id,
                sequence,
                "shadow projection watermark did not advance; projection receipts remain authoritative"
            ),
            Err(error) => tracing::warn!(
                event_id,
                turn_id,
                sequence,
                error = %format!("{error:#}"),
                "shadow projection watermark update failed; projection receipts remain authoritative"
            ),
        }
    }

    Ok(affected)
}

/// Reconciles the shadow watermark with the contiguous canonical prefix whose
/// projection receipts are all `projected`. It never mutates those receipts.
pub async fn backfill_projected_watermark<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<ProjectionWatermarkObservation> {
    let stream = super::turn_event_projection_stream_state::find(db, turn_id)
        .await?
        .with_context(|| format!("projection stream state for Turn `{turn_id}` is missing"))?;
    if stream.projected_through_sequence < 0 {
        anyhow::bail!("projection watermark for Turn `{turn_id}` is negative");
    }

    let observed_projected_through_sequence = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            OBSERVE_PROJECTED_WATERMARK_SQL,
            [turn_id.to_owned().into()],
        ))
        .await
        .with_context(|| {
            format!("failed to observe continuous projection watermark for Turn `{turn_id}`")
        })?
        .with_context(|| {
            format!("continuous projection watermark query returned no row for Turn `{turn_id}`")
        })?
        .try_get::<i64>("", "projected_through_sequence")
        .with_context(|| format!("failed to decode projection watermark for Turn `{turn_id}`"))?;

    let watermark_advanced =
        if observed_projected_through_sequence > stream.projected_through_sequence {
            super::turn_event_projection_stream_state::advance_projected_through(
                db,
                turn_id,
                stream.projected_through_sequence,
                observed_projected_through_sequence,
                updated_at,
            )
            .await?
        } else {
            false
        };
    let stored_projected_through_sequence = super::turn_event_projection_stream_state::find(
        db, turn_id,
    )
    .await?
    .with_context(|| {
        format!(
            "projection stream state for Turn `{turn_id}` disappeared during watermark observation"
        )
    })?
    .projected_through_sequence;

    Ok(ProjectionWatermarkObservation {
        observed_projected_through_sequence,
        stored_projected_through_sequence,
        watermark_advanced,
    })
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
