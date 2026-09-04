use anyhow::{Context, Result};
use pioneer_entity::{recovery_job, recovery_terminalization_outbox};
use pioneer_protocol::generate_id;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::ExprTrait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

pub const STATUS_PENDING: &str = "pending";
pub const STATUS_DELIVERING: &str = "delivering";
pub const STATUS_FAILED: &str = "failed";
pub const STATUS_DELIVERED: &str = "delivered";
pub const STATUS_CANCELLED: &str = "cancelled";
const DB_ID_LEN: usize = 21;

#[derive(Debug, Clone)]
pub struct ClaimedRecoveryTerminalization {
    pub row: recovery_terminalization_outbox::Model,
    pub claim_token: String,
}

pub async fn enqueue_for_terminal_job<C: ConnectionTrait>(
    db: &C,
    job: &recovery_job::Model,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    let error_message = job
        .last_error
        .clone()
        .or_else(|| job.reason.clone())
        .unwrap_or_else(|| "recovery reached a terminal outcome".to_owned());
    // Terminal-after-attempt transitions increment run_count in the same
    // transaction before enqueuing this row. Pending jobs terminalized before
    // activation still report logical attempt 1.
    let attempt_number = std::cmp::Ord::max(job.run_count, 1);
    if let Some(existing) = recovery_terminalization_outbox::Entity::find_by_id(job.id.clone())
        .one(db)
        .await
        .context("failed to query recovery terminalization outbox")?
    {
        if existing.turn_id != job.turn_id
            || existing.item_id != job.item_id
            || existing.item_type != job.item_type
            || existing.recovery_status != job.status
            || existing.error_message != error_message
        {
            anyhow::bail!(
                "recovery job `{}` has a conflicting terminalization outbox row",
                job.id
            );
        }
        return Ok(());
    }
    recovery_terminalization_outbox::ActiveModel {
        recovery_job_id: Set(job.id.clone()),
        turn_id: Set(job.turn_id.clone()),
        item_id: Set(job.item_id.clone()),
        item_type: Set(job.item_type.clone()),
        recovery_status: Set(job.status.clone()),
        attempt_number: Set(attempt_number),
        error_message: Set(error_message),
        status: Set(STATUS_PENDING.to_owned()),
        attempt_count: Set(0),
        last_error: Set(None),
        next_run_at: Set(now),
        claim_token: Set(None),
        claim_expires_at: Set(None),
        delivered_at: Set(None),
        quarantine_reason_code: Set(None),
        quarantined_at: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(db)
    .await
    .context("failed to enqueue recovery terminalization")?;
    Ok(())
}

pub async fn claim_due<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<ClaimedRecoveryTerminalization>> {
    let due = Condition::all()
        .add(recovery_terminalization_outbox::Column::Status.is_in([STATUS_PENDING, STATUS_FAILED]))
        .add(recovery_terminalization_outbox::Column::NextRunAt.lte(now));
    let expired = Condition::all()
        .add(recovery_terminalization_outbox::Column::Status.eq(STATUS_DELIVERING))
        .add(recovery_terminalization_outbox::Column::ClaimExpiresAt.lte(now));
    let candidates = recovery_terminalization_outbox::Entity::find()
        .filter(Condition::any().add(due.clone()).add(expired.clone()))
        .order_by_asc(recovery_terminalization_outbox::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list due recovery terminalizations")?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let candidate_ids = candidates
        .into_iter()
        .map(|row| row.recovery_job_id)
        .collect::<Vec<_>>();
    // A batch token is safe because every transition is still fenced by both
    // recovery_job_id and token. It lets selection, claim and reload remain a
    // bounded three-statement transaction instead of an N+1 loop.
    let claim_token = generate_id(DB_ID_LEN);
    recovery_terminalization_outbox::Entity::update_many()
        .col_expr(
            recovery_terminalization_outbox::Column::Status,
            sea_orm::sea_query::Expr::value(STATUS_DELIVERING.to_owned()),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Some(claim_token.clone())),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Some(claim_expires_at)),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_terminalization_outbox::Column::RecoveryJobId.is_in(candidate_ids.clone()))
        .filter(Condition::any().add(due).add(expired))
        .exec(db)
        .await
        .context("failed to claim recovery terminalization batch")?;
    let rows = recovery_terminalization_outbox::Entity::find()
        .filter(recovery_terminalization_outbox::Column::RecoveryJobId.is_in(candidate_ids))
        .filter(recovery_terminalization_outbox::Column::Status.eq(STATUS_DELIVERING))
        .filter(recovery_terminalization_outbox::Column::ClaimToken.eq(claim_token.clone()))
        .order_by_asc(recovery_terminalization_outbox::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to reload claimed recovery terminalization batch")?;
    Ok(rows
        .into_iter()
        .map(|row| ClaimedRecoveryTerminalization {
            row,
            claim_token: claim_token.clone(),
        })
        .collect())
}

pub async fn mark_delivered<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    transition_claim(
        db,
        job_id,
        claim_token,
        STATUS_DELIVERED,
        None,
        now,
        now,
        true,
    )
    .await
}

pub async fn mark_failed<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    error: String,
    retry_at: DateTimeWithTimeZone,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    transition_claim(
        db,
        job_id,
        claim_token,
        STATUS_FAILED,
        Some(error),
        retry_at,
        now,
        false,
    )
    .await
}

/// Permanently removes a semantically invalid row from the retry set while
/// retaining a bounded, queryable reason. The exact claim token is the fence:
/// an expired worker cannot quarantine a row reclaimed by another worker.
pub async fn quarantine_claim<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    reason_code: &str,
    error: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = recovery_terminalization_outbox::Entity::update_many()
        .col_expr(
            recovery_terminalization_outbox::Column::Status,
            sea_orm::sea_query::Expr::value(STATUS_CANCELLED.to_owned()),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::AttemptCount,
            sea_orm::sea_query::Expr::col(recovery_terminalization_outbox::Column::AttemptCount)
                .add(1),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(error.to_owned())),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::QuarantineReasonCode,
            sea_orm::sea_query::Expr::value(Some(reason_code.to_owned())),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::QuarantinedAt,
            sea_orm::sea_query::Expr::value(Some(now)),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_terminalization_outbox::Column::RecoveryJobId.eq(job_id.to_owned()))
        .filter(recovery_terminalization_outbox::Column::Status.eq(STATUS_DELIVERING))
        .filter(recovery_terminalization_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .context("failed to quarantine recovery terminalization claim")?
        .rows_affected;
    if affected == 1 {
        resolve_recovery_episode(db, job_id, now).await?;
    }
    Ok(affected == 1)
}

pub async fn quarantine_claim_batch<C: ConnectionTrait>(
    db: &C,
    job_ids: &[String],
    claim_token: &str,
    reason_code: &str,
    error: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    if job_ids.is_empty() {
        return Ok(0);
    }
    let affected = recovery_terminalization_outbox::Entity::update_many()
        .col_expr(
            recovery_terminalization_outbox::Column::Status,
            sea_orm::sea_query::Expr::value(STATUS_CANCELLED.to_owned()),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::AttemptCount,
            sea_orm::sea_query::Expr::col(recovery_terminalization_outbox::Column::AttemptCount)
                .add(1),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(error.to_owned())),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::QuarantineReasonCode,
            sea_orm::sea_query::Expr::value(Some(reason_code.to_owned())),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::QuarantinedAt,
            sea_orm::sea_query::Expr::value(Some(now)),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_terminalization_outbox::Column::RecoveryJobId.is_in(job_ids.to_vec()))
        .filter(recovery_terminalization_outbox::Column::Status.eq(STATUS_DELIVERING))
        .filter(recovery_terminalization_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .context("failed to quarantine recovery terminalization batch")?
        .rows_affected;
    if affected > 0 {
        resolve_recovery_episodes(db, job_ids, now).await?;
    }
    Ok(affected)
}

/// Supersedes the exact claim owned by a worker after a newer terminal Turn
/// status has made the recovery outcome obsolete. The claim token is part of
/// the fence so an expired worker cannot cancel a subsequently reclaimed row.
pub async fn cancel_claim<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    reason: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = recovery_terminalization_outbox::Entity::update_many()
        .col_expr(
            recovery_terminalization_outbox::Column::Status,
            sea_orm::sea_query::Expr::value(STATUS_CANCELLED.to_owned()),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(reason.to_owned())),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_terminalization_outbox::Column::RecoveryJobId.eq(job_id.to_owned()))
        .filter(recovery_terminalization_outbox::Column::Status.eq(STATUS_DELIVERING))
        .filter(recovery_terminalization_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .context("failed to cancel superseded recovery terminalization claim")?
        .rows_affected;
    if affected == 1 {
        resolve_recovery_episode(db, job_id, now).await?;
    }
    Ok(affected == 1)
}

pub async fn cancel_claim_batch<C: ConnectionTrait>(
    db: &C,
    job_ids: &[String],
    claim_token: &str,
    reason: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    if job_ids.is_empty() {
        return Ok(0);
    }
    let affected = recovery_terminalization_outbox::Entity::update_many()
        .col_expr(
            recovery_terminalization_outbox::Column::Status,
            sea_orm::sea_query::Expr::value(STATUS_CANCELLED.to_owned()),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(reason.to_owned())),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_terminalization_outbox::Column::RecoveryJobId.is_in(job_ids.to_vec()))
        .filter(recovery_terminalization_outbox::Column::Status.eq(STATUS_DELIVERING))
        .filter(recovery_terminalization_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .context("failed to cancel superseded recovery terminalization batch")?
        .rows_affected;
    if affected > 0 {
        resolve_recovery_episodes(db, job_ids, now).await?;
    }
    Ok(affected)
}

/// Supersedes an undelivered terminalization when an explicit, fenced resume
/// wins before the terminal outcome is applied. Delivered rows remain an
/// immutable audit record.
pub async fn cancel_undelivered_for_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    reason: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = recovery_terminalization_outbox::Entity::update_many()
        .col_expr(
            recovery_terminalization_outbox::Column::Status,
            sea_orm::sea_query::Expr::value(STATUS_CANCELLED.to_owned()),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(reason.to_owned())),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_terminalization_outbox::Column::RecoveryJobId.eq(job_id.to_owned()))
        .filter(recovery_terminalization_outbox::Column::Status.is_in([
            STATUS_PENDING,
            STATUS_FAILED,
            STATUS_DELIVERING,
        ]))
        .exec(db)
        .await
        .context("failed to cancel superseded recovery terminalization")?
        .rows_affected;
    Ok(affected == 1)
}

async fn transition_claim<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    status: &str,
    error: Option<String>,
    next_run_at: DateTimeWithTimeZone,
    now: DateTimeWithTimeZone,
    delivered: bool,
) -> Result<bool> {
    let affected = recovery_terminalization_outbox::Entity::update_many()
        .col_expr(
            recovery_terminalization_outbox::Column::Status,
            sea_orm::sea_query::Expr::value(status.to_owned()),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::AttemptCount,
            sea_orm::sea_query::Expr::col(recovery_terminalization_outbox::Column::AttemptCount)
                .add((!delivered) as i32),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(error),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::DeliveredAt,
            sea_orm::sea_query::Expr::value(delivered.then_some(now)),
        )
        .col_expr(
            recovery_terminalization_outbox::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_terminalization_outbox::Column::RecoveryJobId.eq(job_id.to_owned()))
        .filter(recovery_terminalization_outbox::Column::Status.eq(STATUS_DELIVERING))
        .filter(recovery_terminalization_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .context("failed to transition recovery terminalization claim")?
        .rows_affected;
    if affected == 1 && delivered {
        resolve_recovery_episode(db, job_id, now).await?;
    }
    Ok(affected == 1)
}

async fn resolve_recovery_episode<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::ResolutionPending,
            sea_orm::sea_query::Expr::value(false),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::ResolutionPending.eq(true))
        .exec(db)
        .await
        .context("failed to resolve recovery episode")?;
    Ok(())
}

async fn resolve_recovery_episodes<C: ConnectionTrait>(
    db: &C,
    job_ids: &[String],
    now: DateTimeWithTimeZone,
) -> Result<()> {
    recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::ResolutionPending,
            sea_orm::sea_query::Expr::value(false),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.is_in(job_ids.to_vec()))
        .filter(recovery_job::Column::ResolutionPending.eq(true))
        .exec(db)
        .await
        .context("failed to resolve recovery episode batch")?;
    Ok(())
}
