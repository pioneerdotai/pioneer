use anyhow::{Context, Result};
use pioneer_entity::turn_event_projection_state;
use pioneer_protocol::generate_id;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::ExprTrait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, Set,
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

    let candidates = turn_event_projection_state::Entity::find()
        .filter(
            Condition::any()
                .add(due_pending_or_failed.clone())
                .add(expired_projecting.clone()),
        )
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

pub async fn mark_projected_claimed<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
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
