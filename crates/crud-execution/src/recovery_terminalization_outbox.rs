use crate::recovery_job as recovery_repository;
use anyhow::{Context, Result};
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_conversations::turn_item::turn_item_type_from_db;
use pioneer_entity::{recovery_job, recovery_terminalization_outbox};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use pioneer_protocol::{RecoveryJobStatus, TurnItemType, TurnStatus, generate_id};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    time::Duration,
};

use crate::recovery_values::recovery_job_status_from_db;
use crate::time::unix_to_datetime;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{ExprTrait, LockBehavior, LockType};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait,
    QueryFilter, QueryOrder, QuerySelect, Set, TransactionTrait,
};

pub async fn require_claim_revision(
    db: &impl ConnectionTrait,
    job_id: &str,
    claim_token: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    let row = recovery_terminalization_outbox::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await?
        .context("recovery terminalization outbox row disappeared")?;
    anyhow::ensure!(
        row.status == STATUS_DELIVERING
            && row.claim_token.as_deref() == Some(claim_token)
            && row.updated_at == updated_at,
        "recovery terminalization claim is no longer owned"
    );
    Ok(())
}

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
    // Internal transition fingerprint only. Public terminalization renders the
    // typed job diagnostic; legacy last_error must never be used as public prose.
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

pub async fn claim_due(
    db: &DatabaseTransaction,
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
        .order_by_asc(recovery_terminalization_outbox::Column::RecoveryJobId)
        .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
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
        .order_by_asc(recovery_terminalization_outbox::Column::RecoveryJobId)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedRecoveryTerminalizationRecord {
    pub recovery_job_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_status: RecoveryJobStatus,
    pub attempt_number: u32,
    pub error_message: String,
    pub attempt_count: u32,
    pub claim_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryTerminalizationClaimDisposition {
    NeedsCommit,
    AlreadyTerminal,
    SupersededByTerminalStatus(TurnStatus),
    Quarantined { reason_code: String },
}

#[derive(Debug)]
pub struct PermanentRecoveryTerminalizationError {
    pub reason_code: &'static str,
    pub message: String,
}

impl std::fmt::Display for PermanentRecoveryTerminalizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for PermanentRecoveryTerminalizationError {}

pub fn permanent_recovery_terminalization_error(
    reason_code: &'static str,
    message: impl Into<String>,
) -> anyhow::Error {
    PermanentRecoveryTerminalizationError {
        reason_code,
        message: message.into(),
    }
    .into()
}

async fn retry_write<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    retry_with_backoff(
        operation,
        is_retryable_database_conflict,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

/// Release the dependent workflow state as part of the same settlement.
pub trait RecoverySettlement: Sync {
    fn release<'a>(
        &'a self,
        tx: &'a DatabaseTransaction,
        job_ids: &'a [String],
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

pub async fn claim_records(
    db: &PostgresDatabase,
    now_unix: i64,
    claim_lease_secs: u64,
    limit: u64,
    settlement: &impl RecoverySettlement,
) -> Result<(Vec<ClaimedRecoveryTerminalizationRecord>, bool)> {
    let now = unix_to_datetime(now_unix);
    let claim_expires_at = unix_to_datetime(
        now_unix.saturating_add(i64::try_from(claim_lease_secs).unwrap_or(i64::MAX)),
    );
    let claimed = retry_write(|| async {
        let transaction = db
            .begin()
            .await
            .context("failed to begin recovery terminalization batch claim")?;
        let claimed = match claim_due(&transaction, now, claim_expires_at, limit).await {
            Ok(claimed) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit recovery terminalization batch claim")?;
                claimed
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };
        Ok(claimed)
    })
    .await?;
    let mut valid = Vec::with_capacity(claimed.len());
    let mut dependency_released = false;
    for claimed in claimed {
        let row = claimed.row;
        let recovery_job_id = row.recovery_job_id.clone();
        let claim_token = claimed.claim_token.clone();
        let decoded = (|| -> Result<ClaimedRecoveryTerminalizationRecord> {
            let item_type = turn_item_type_from_db(row.item_type.as_str()).ok_or_else(|| {
                permanent_recovery_terminalization_error(
                    "unknown_item_type",
                    "recovery terminalization has an unknown item type",
                )
            })?;
            let recovery_status = recovery_job_status_from_db(row.recovery_status.as_str())
                .ok_or_else(|| {
                    permanent_recovery_terminalization_error(
                        "unknown_recovery_status",
                        "recovery terminalization has an unknown recovery status",
                    )
                })?;
            let attempt_number = u32::try_from(row.attempt_number).map_err(|_| {
                permanent_recovery_terminalization_error(
                    "invalid_attempt_number",
                    "recovery terminalization attempt number is invalid",
                )
            })?;
            Ok(ClaimedRecoveryTerminalizationRecord {
                recovery_job_id: row.recovery_job_id,
                turn_id: row.turn_id,
                item_id: row.item_id,
                item_type,
                recovery_status,
                attempt_number,
                error_message: row.error_message,
                attempt_count: u32::try_from(row.attempt_count).unwrap_or(u32::MAX),
                claim_token: claimed.claim_token,
            })
        })();
        match decoded {
            Ok(record) => valid.push(record),
            Err(error) => {
                let permanent = error
                    .downcast_ref::<PermanentRecoveryTerminalizationError>()
                    .expect("claim decoding only returns permanent validation errors");
                let quarantine_tx = db.begin().await?;
                if !quarantine_claim(
                    &quarantine_tx,
                    recovery_job_id.as_str(),
                    claim_token.as_str(),
                    permanent.reason_code,
                    permanent.message.as_str(),
                    now,
                )
                .await?
                {
                    anyhow::bail!("recovery terminalization claim was lost before quarantine");
                }
                settlement
                    .release(&quarantine_tx, std::slice::from_ref(&recovery_job_id))
                    .await?;
                quarantine_tx.commit().await?;
                dependency_released = true;
            }
        }
    }
    Ok((valid, dependency_released))
}

pub async fn classify_claim(
    db: &PostgresDatabase,
    record: &ClaimedRecoveryTerminalizationRecord,
    now_unix: i64,
    settlement: &impl RecoverySettlement,
) -> Result<RecoveryTerminalizationClaimDisposition> {
    enum Decision {
        NeedsCommit,
        AlreadyTerminal,
        Superseded(TurnStatus),
        Quarantine {
            reason_code: &'static str,
            message: String,
        },
    }

    let result = async {
        let record = record.clone();
        retry_write(|| async {
            let now = unix_to_datetime(now_unix);
            let tx = db
                .begin()
                .await
                .context("failed to begin recovery terminalization classification")?;
            let result = async {
                let outbox = pioneer_entity::recovery_terminalization_outbox::Entity::find_by_id(
                    record.recovery_job_id.clone(),
                )
                .one(&tx)
                .await?
                .context("recovery terminalization outbox row disappeared")?;
                if outbox.status != STATUS_DELIVERING
                    || outbox.claim_token.as_deref() != Some(record.claim_token.as_str())
                {
                    anyhow::bail!("recovery terminalization claim is no longer owned");
                }
                let Some(job) =
                    recovery_repository::find_job_by_id(&tx, record.recovery_job_id.as_str())
                        .await?
                else {
                    return Ok(Decision::Quarantine {
                        reason_code: "missing_recovery_job",
                        message: "recovery terminalization job is missing".to_owned(),
                    });
                };
                let Some(job_status) = recovery_job_status_from_db(job.status.as_str()) else {
                    return Ok(Decision::Quarantine {
                        reason_code: "unknown_recovery_status",
                        message: "recovery terminalization job has an unknown status".to_owned(),
                    });
                };
                let Some(job_item_type) = turn_item_type_from_db(job.item_type.as_str()) else {
                    return Ok(Decision::Quarantine {
                        reason_code: "unknown_item_type",
                        message: "recovery terminalization job has an unknown item type".to_owned(),
                    });
                };
                let Ok(job_attempt_number) = u32::try_from(std::cmp::max(job.run_count, 1)) else {
                    return Ok(Decision::Quarantine {
                        reason_code: "invalid_attempt_number",
                        message: "recovery terminalization job has an invalid attempt number"
                            .to_owned(),
                    });
                };
                let job_error_message = job
                    .last_error
                    .clone()
                    .or_else(|| job.reason.clone())
                    .unwrap_or_else(|| "recovery reached a terminal outcome".to_owned());
                if job.turn_id != record.turn_id
                    || job.item_id != record.item_id
                    || job_item_type != record.item_type
                    || job_status != record.recovery_status
                    || job_attempt_number != record.attempt_number
                    || job_error_message != record.error_message
                {
                    return Ok(Decision::Quarantine {
                        reason_code: "job_identity_mismatch",
                        message: "recovery terminalization no longer matches its recovery job"
                            .to_owned(),
                    });
                }
                if !matches!(
                    job_status,
                    RecoveryJobStatus::Blocked
                        | RecoveryJobStatus::Failed
                        | RecoveryJobStatus::Exhausted
                ) {
                    return Ok(Decision::Quarantine {
                        reason_code: "non_terminal_recovery_job",
                        message: "recovery terminalization job is not terminalizable".to_owned(),
                    });
                }
                let Some(turn_model) = pioneer_crud_conversations::turn_reads::find_turn_by_id(
                    &tx,
                    record.turn_id.as_str(),
                )
                .await?
                else {
                    return Ok(Decision::Quarantine {
                        reason_code: "missing_turn",
                        message: "recovery terminalization Turn is missing".to_owned(),
                    });
                };
                let Some(current_status) = turn_status_from_db(turn_model.status.as_str()) else {
                    return Ok(Decision::Quarantine {
                        reason_code: "unknown_turn_status",
                        message: "recovery terminalization Turn has an unknown status".to_owned(),
                    });
                };
                let desired_status = if job_status == RecoveryJobStatus::Blocked {
                    TurnStatus::Blocked
                } else {
                    TurnStatus::Failed
                };
                if current_status == TurnStatus::InProgress {
                    Ok(Decision::NeedsCommit)
                } else if current_status == desired_status {
                    Ok(Decision::AlreadyTerminal)
                } else {
                    Ok(Decision::Superseded(current_status))
                }
            }
            .await;

            let disposition = match result {
                Ok(Decision::NeedsCommit) => RecoveryTerminalizationClaimDisposition::NeedsCommit,
                Ok(Decision::AlreadyTerminal) => {
                    RecoveryTerminalizationClaimDisposition::AlreadyTerminal
                }
                Ok(Decision::Superseded(current_status)) => {
                    let reason = format!(
                        "superseded by authoritative terminal Turn status `{:?}`",
                        current_status
                    );
                    if !cancel_claim(
                        &tx,
                        record.recovery_job_id.as_str(),
                        record.claim_token.as_str(),
                        reason.as_str(),
                        now,
                    )
                    .await?
                    {
                        anyhow::bail!(
                            "recovery terminalization claim was lost before supersession"
                        );
                    }
                    RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(
                        current_status,
                    )
                }
                Ok(Decision::Quarantine {
                    reason_code,
                    message,
                }) => {
                    if !quarantine_claim(
                        &tx,
                        record.recovery_job_id.as_str(),
                        record.claim_token.as_str(),
                        reason_code,
                        message.as_str(),
                        now,
                    )
                    .await?
                    {
                        anyhow::bail!("recovery terminalization claim was lost before quarantine");
                    }
                    RecoveryTerminalizationClaimDisposition::Quarantined {
                        reason_code: reason_code.to_owned(),
                    }
                }
                Err(error) => {
                    let _ = tx.rollback().await;
                    return Err(error);
                }
            };
            if matches!(
                disposition,
                RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(_)
                    | RecoveryTerminalizationClaimDisposition::Quarantined { .. }
            ) {
                settlement
                    .release(&tx, std::slice::from_ref(&record.recovery_job_id))
                    .await?;
            }
            tx.commit()
                .await
                .context("failed to commit recovery terminalization classification")?;
            Ok(disposition)
        })
        .await
    }
    .await;
    result
}

pub async fn classify_claim_batch(
    db: &PostgresDatabase,
    records: &[ClaimedRecoveryTerminalizationRecord],
    now_unix: i64,
    settlement: &impl RecoverySettlement,
) -> Result<
    Vec<(
        ClaimedRecoveryTerminalizationRecord,
        RecoveryTerminalizationClaimDisposition,
    )>,
> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    anyhow::ensure!(
        records.len() <= 100,
        "recovery terminalization classification batch exceeds its bounded limit"
    );
    let claim_token = records[0].claim_token.clone();
    anyhow::ensure!(
        records
            .iter()
            .all(|record| record.claim_token == claim_token),
        "recovery terminalization classification batch crosses claim tokens"
    );
    anyhow::ensure!(
        records
            .iter()
            .map(|record| record.recovery_job_id.as_str())
            .collect::<HashSet<_>>()
            .len()
            == records.len(),
        "recovery terminalization classification batch contains duplicate jobs"
    );
    let result = async {
        let records = records.to_vec();
        retry_write(|| {
            let records = records.clone();
            let claim_token = claim_token.clone();
            async move {
                let now = unix_to_datetime(now_unix);
                let tx = db
                    .begin()
                    .await
                    .context("failed to begin recovery terminalization batch classification")?;
                let result = async {
                    let rows = crate::recovery_reads::load_claimed_recovery_facts(
                        &tx,
                        &claim_token,
                        records.iter().map(|record| record.recovery_job_id.clone()),
                    )
                    .await?;
                    if rows.len() != records.len() {
                        anyhow::bail!(
                            "one or more recovery terminalization claims are no longer owned"
                        );
                    }

                    let records_by_id = records
                        .iter()
                        .map(|record| (record.recovery_job_id.clone(), record))
                        .collect::<HashMap<_, _>>();
                    let mut dispositions = HashMap::with_capacity(records.len());
                    let mut superseded = Vec::new();
                    let mut quarantined = HashMap::<String, (String, Vec<String>)>::new();
                    for row in rows {
                        let recovery_job_id = row.recovery_job_id;
                        let record = records_by_id
                            .get(&recovery_job_id)
                            .context("classification returned an unexpected recovery job")?;
                        let quarantine = |reason_code: &str, message: &str| {
                            (
                                RecoveryTerminalizationClaimDisposition::Quarantined {
                                    reason_code: reason_code.to_owned(),
                                },
                                Some((reason_code.to_owned(), message.to_owned())),
                            )
                        };
                        let (disposition, quarantine_reason) = 'decision: {
                            if row.job_id.is_none() {
                                break 'decision quarantine(
                                    "missing_recovery_job",
                                    "recovery terminalization job is missing",
                                );
                            }
                            let job_turn_id =
                                row.job_turn_id.context("missing recovery job Turn")?;
                            let job_item_id =
                                row.job_item_id.context("missing recovery job item")?;
                            let job_item_type_db = row
                                .job_item_type
                                .context("missing recovery job item type")?;
                            let job_status_db =
                                row.job_status.context("missing recovery job status")?;
                            let job_attempt_number = row.job_attempt_number;
                            let job_error_message = row.job_error_message;
                            let Some(job_item_type) =
                                turn_item_type_from_db(job_item_type_db.as_str())
                            else {
                                break 'decision quarantine(
                                    "unknown_item_type",
                                    "recovery terminalization job has an unknown item type",
                                );
                            };
                            let Some(job_status) =
                                recovery_job_status_from_db(job_status_db.as_str())
                            else {
                                break 'decision quarantine(
                                    "unknown_recovery_status",
                                    "recovery terminalization job has an unknown status",
                                );
                            };
                            let Ok(job_attempt_number) = u32::try_from(job_attempt_number) else {
                                break 'decision quarantine(
                                    "invalid_attempt_number",
                                    "recovery terminalization job has an invalid attempt number",
                                );
                            };
                            if job_turn_id != record.turn_id
                                || job_item_id != record.item_id
                                || job_item_type != record.item_type
                                || job_status != record.recovery_status
                                || job_attempt_number != record.attempt_number
                                || job_error_message != record.error_message
                            {
                                break 'decision quarantine(
                                    "job_identity_mismatch",
                                    "recovery terminalization no longer matches its recovery job",
                                );
                            }
                            if !matches!(
                                job_status,
                                RecoveryJobStatus::Blocked
                                    | RecoveryJobStatus::Failed
                                    | RecoveryJobStatus::Exhausted
                            ) {
                                break 'decision quarantine(
                                    "non_terminal_recovery_job",
                                    "recovery terminalization job is not terminalizable",
                                );
                            }
                            if row.turn_row_id.is_none() {
                                break 'decision quarantine(
                                    "missing_turn",
                                    "recovery terminalization Turn is missing",
                                );
                            }
                            let turn_status_db = row
                                .turn_status
                                .context("missing terminalization Turn status")?;
                            let Some(turn_status) = turn_status_from_db(turn_status_db.as_str())
                            else {
                                break 'decision quarantine(
                                    "unknown_turn_status",
                                    "recovery terminalization Turn has an unknown status",
                                );
                            };
                            let desired_status = if job_status == RecoveryJobStatus::Blocked {
                                TurnStatus::Blocked
                            } else {
                                TurnStatus::Failed
                            };
                            if turn_status == TurnStatus::InProgress {
                                break 'decision (
                                    RecoveryTerminalizationClaimDisposition::NeedsCommit,
                                    None,
                                );
                            }
                            if turn_status == desired_status {
                                break 'decision (
                                    RecoveryTerminalizationClaimDisposition::AlreadyTerminal,
                                    None,
                                );
                            }
                            superseded.push(recovery_job_id.clone());
                            break 'decision (
                                RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(
                                    turn_status,
                                ),
                                None,
                            );
                        };
                        if let Some((reason_code, message)) = quarantine_reason {
                            quarantined
                                .entry(reason_code)
                                .or_insert_with(|| (message, Vec::new()))
                                .1
                                .push(recovery_job_id.clone());
                        }
                        dispositions.insert(recovery_job_id, disposition);
                    }
                    if !superseded.is_empty() {
                        let affected = cancel_claim_batch(
                            &tx,
                            &superseded,
                            claim_token.as_str(),
                            "superseded by authoritative terminal Turn status",
                            now,
                        )
                        .await?;
                        anyhow::ensure!(
                            affected == superseded.len() as u64,
                            "recovery terminalization claim was lost during batch supersession"
                        );
                        settlement.release(&tx, &superseded).await?;
                    }
                    for (reason_code, (message, job_ids)) in quarantined {
                        let affected = quarantine_claim_batch(
                            &tx,
                            &job_ids,
                            claim_token.as_str(),
                            reason_code.as_str(),
                            message.as_str(),
                            now,
                        )
                        .await?;
                        anyhow::ensure!(
                            affected == job_ids.len() as u64,
                            "recovery terminalization claim was lost during batch quarantine"
                        );
                        settlement.release(&tx, &job_ids).await?;
                    }
                    let classified = records
                        .into_iter()
                        .map(|record| {
                            let disposition = dispositions
                                .remove(record.recovery_job_id.as_str())
                                .context("recovery terminalization was not classified")?;
                            Ok((record, disposition))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok::<_, anyhow::Error>(classified)
                }
                .await;
                match result {
                    Ok(classified) => {
                        tx.commit()
                            .await
                            .context("failed to commit terminalization batch classification")?;
                        Ok(classified)
                    }
                    Err(error) => {
                        let _ = tx.rollback().await;
                        Err(error)
                    }
                }
            }
        })
        .await
    }
    .await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_postgres_test::with_database;
    use pioneer_protocol::{RecoveryAction, RecoveryTrigger};
    use sea_orm::sea_query::Expr;

    struct ObserveResolution;

    impl RecoverySettlement for ObserveResolution {
        async fn release(&self, tx: &DatabaseTransaction, job_ids: &[String]) -> Result<()> {
            assert_eq!(job_ids.len(), 1);
            let row = recovery_repository::find_job_by_id(tx, &job_ids[0])
                .await?
                .context("resolution available within settlement transaction")?;
            assert!(!row.resolution_pending);
            Ok(())
        }
    }

    #[tokio::test]
    async fn malformed_terminalization_is_quarantined_without_blocking_later_rows() -> Result<()> {
        with_database(async |fixture| {
            async fn terminal_job(
                db: &PostgresDatabase,
                turn_id: &str,
                item_id: &str,
                now_unix: i64,
            ) -> Result<recovery_job::Model> {
                let now = unix_to_datetime(now_unix);
                let terminal_at = unix_to_datetime(now_unix + 1);
                let tx = db.begin().await?;
                let job = recovery_repository::enqueue_recovery_job(
                    &tx,
                    recovery_repository::NewRecoveryJob {
                        diagnostic_json: None,
                        turn_id: turn_id.to_owned(),
                        item_id: item_id.to_owned(),
                        item_type: TurnItemType::Reasoning,
                        source_attempt_id: None,
                        trigger: RecoveryTrigger::RuntimeFailure,
                        action: RecoveryAction::MarkFailed,
                        reason: Some("terminal recovery".to_owned()),
                        policy_json: "{}".to_owned(),
                        error_class: None,
                        transport_stage: None,
                        retry_after_ms: None,
                        provider_attempt_number: 0,
                        policy_snapshot_json: "{}".to_owned(),
                        max_attempts: 1,
                        created_at: now,
                        scheduled_at: now,
                        next_run_at: now,
                    },
                ).await?;
                assert!(recovery_repository::mark_job_terminal(
                    &tx, &job.id, RecoveryJobStatus::Failed, Some("terminal recovery".to_owned()), terminal_at,
                ).await?);
                let terminal = recovery_repository::find_job_by_id(&tx, &job.id)
                    .await?.context("terminal recovery job")?;
                enqueue_for_terminal_job(&tx, &terminal, terminal_at).await?;
                tx.commit().await?;
                Ok(terminal)
            }

            let db = PostgresDatabase::new(fixture.connect_read_only().await?, fixture.connection().clone());
            db.execute_unprepared(
                "INSERT INTO workspace (id, name) VALUES ('ws_recovery_poison', 'Recovery');
                 INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status) VALUES
                   ('thr_recovery_poison','ws_recovery_poison','','agent','model','provider','active'),
                   ('thr_recovery_valid','ws_recovery_poison','','agent','model','provider','active');
                 INSERT INTO turn (id,thread_id,status) VALUES
                   ('turn_recovery_poison','thr_recovery_poison','in_progress'),
                   ('turn_recovery_valid','thr_recovery_valid','in_progress');",
            ).await?;
            let poison = terminal_job(&db, "turn_recovery_poison", "runtime:poison", 1_700_000_001).await?;
            let valid = terminal_job(&db, "turn_recovery_valid", "runtime:valid", 1_700_000_003).await?;

            pioneer_entity::recovery_terminalization_outbox::Entity::update_many()
                .col_expr(
                    pioneer_entity::recovery_terminalization_outbox::Column::ItemType,
                    Expr::value("retired_item_type"),
                )
                .filter(
                    pioneer_entity::recovery_terminalization_outbox::Column::RecoveryJobId
                        .eq(poison.id.clone()),
                )
                .exec(&db)
                .await
                .expect("poison fixture should persist");

            let (claimed, released) = claim_records(&db, 1_700_000_010, 45, 10, &ObserveResolution)
                .await
                .expect("poison row must not fail the batch");
            assert!(released);
            assert_eq!(claimed.len(), 1);
            assert_eq!(claimed[0].recovery_job_id, valid.id);
            let quarantined = pioneer_entity::recovery_terminalization_outbox::Entity::find_by_id(
                poison.id.clone(),
            )
            .one(&db)
            .await
            .expect("quarantine lookup should succeed")
            .expect("quarantined audit row should remain");
            assert_eq!(
                quarantined.status,
                STATUS_CANCELLED
            );
            assert_eq!(
                quarantined.quarantine_reason_code.as_deref(),
                Some("unknown_item_type")
            );
            assert!(quarantined.quarantined_at.is_some());
            let poison_job = recovery_repository::find_job_by_id(&db, poison.id.as_str())
                .await
                .expect("poison recovery lookup should succeed")
                .expect("poison recovery should remain for audit");
            assert!(!poison_job.resolution_pending);
            Ok(())
        })
        .await
    }
}
