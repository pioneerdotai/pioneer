use anyhow::{Context, Result};
use pioneer_entity::recovery_job;
use pioneer_protocol::{
    ProviderFailureClass, ProviderFailureStage, RecoveryAction, RecoveryJobStatus, RecoveryTrigger,
    TurnItemType, generate_id,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, ExprTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    provider_failure_class_to_db, provider_failure_stage_to_db, recovery_action_to_db,
    recovery_job_status_to_db, recovery_trigger_to_db, turn_item_type_to_db,
};

const DB_ID_LEN: usize = 21;
const CLAIM_TOKEN_LEN: usize = 21;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimedJobActivation {
    Activated,
    BlockedByActiveRecovery,
    ClaimNotFound,
}

#[derive(Debug, Clone)]
pub struct NewRecoveryJob {
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub source_attempt_id: Option<String>,
    pub trigger: RecoveryTrigger,
    pub action: RecoveryAction,
    pub reason: Option<String>,
    pub policy_json: String,
    pub error_class: Option<ProviderFailureClass>,
    pub transport_stage: Option<ProviderFailureStage>,
    pub retry_after_ms: Option<i64>,
    pub provider_attempt_number: i64,
    pub policy_snapshot_json: String,
    pub max_attempts: i64,
    pub scheduled_at: DateTimeWithTimeZone,
    pub next_run_at: DateTimeWithTimeZone,
}

pub async fn enqueue_recovery_job<C: ConnectionTrait>(
    db: &C,
    job: NewRecoveryJob,
) -> Result<recovery_job::Model> {
    let id = generate_id(DB_ID_LEN);

    recovery_job::Entity::insert(recovery_job::ActiveModel {
        id: Set(id.clone()),
        turn_id: Set(job.turn_id),
        item_id: Set(job.item_id),
        item_type: Set(turn_item_type_to_db(job.item_type).to_owned()),
        source_attempt_id: Set(job.source_attempt_id),
        status: Set(recovery_job_status_to_db(RecoveryJobStatus::Pending).to_owned()),
        trigger: Set(recovery_trigger_to_db(job.trigger).to_owned()),
        action: Set(recovery_action_to_db(job.action).to_owned()),
        reason: Set(job.reason),
        policy: Set(job.policy_json),
        error_class: Set(job
            .error_class
            .map(provider_failure_class_to_db)
            .map(str::to_owned)),
        transport_stage: Set(job
            .transport_stage
            .map(provider_failure_stage_to_db)
            .map(str::to_owned)),
        retry_after_ms: Set(job.retry_after_ms),
        provider_attempt_number: Set(job.provider_attempt_number),
        policy_snapshot: Set(job.policy_snapshot_json),
        run_count: Set(0),
        max_attempts: Set(job.max_attempts),
        last_error: Set(None),
        scheduled_at: Set(job.scheduled_at),
        next_run_at: Set(job.next_run_at),
        claim_token: Set(None),
        claimed_at: Set(None),
        claim_expires_at: Set(None),
        active_attempt_id: Set(None),
        active_attempt_started_at: Set(None),
        created_at: Set(job.scheduled_at),
        updated_at: Set(job.scheduled_at),
    })
    .exec(db)
    .await
    .context("failed to insert recovery job")?;

    recovery_job::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload recovery job row")?
        .context("recovery job row missing after insert")
}

pub async fn count_jobs_for_turn<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<u64> {
    recovery_job::Entity::find()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .count(db)
        .await
        .with_context(|| format!("failed to count recovery jobs for turn `{turn_id}`"))
}

pub async fn claim_due_jobs<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<recovery_job::Model>> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);

    let candidates = recovery_job::Entity::find()
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::NextRunAt.lte(now))
        .filter(
            Condition::any()
                .add(recovery_job::Column::ClaimExpiresAt.is_null())
                .add(recovery_job::Column::ClaimExpiresAt.lte(now)),
        )
        .order_by_asc(recovery_job::Column::NextRunAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to load due recovery jobs")?;

    let mut claimed = Vec::new();

    for candidate in candidates {
        let claim_token = generate_id(CLAIM_TOKEN_LEN);
        let claimed_now = recovery_job::Entity::update_many()
            .col_expr(
                recovery_job::Column::ClaimToken,
                sea_orm::sea_query::Expr::value(Some(claim_token.clone())),
            )
            .col_expr(
                recovery_job::Column::ClaimedAt,
                sea_orm::sea_query::Expr::value(Some(now)),
            )
            .col_expr(
                recovery_job::Column::ClaimExpiresAt,
                sea_orm::sea_query::Expr::value(Some(claim_expires_at)),
            )
            .col_expr(
                recovery_job::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now),
            )
            .filter(recovery_job::Column::Id.eq(candidate.id.clone()))
            .filter(recovery_job::Column::Status.eq(pending))
            .filter(
                Condition::any()
                    .add(recovery_job::Column::ClaimExpiresAt.is_null())
                    .add(recovery_job::Column::ClaimExpiresAt.lte(now)),
            )
            .exec(db)
            .await
            .with_context(|| format!("failed to claim recovery job `{}`", candidate.id))?
            .rows_affected
            > 0;

        if !claimed_now {
            continue;
        }

        if let Some(job) = recovery_job::Entity::find_by_id(candidate.id.clone())
            .one(db)
            .await
            .with_context(|| format!("failed to reload claimed recovery job `{}`", candidate.id))?
        {
            claimed.push(job);
        }
    }

    Ok(claimed)
}

pub async fn list_due_pending_jobs_by_action<C: ConnectionTrait>(
    db: &C,
    action: RecoveryAction,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<recovery_job::Model>> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let action_db = recovery_action_to_db(action);

    recovery_job::Entity::find()
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::Action.eq(action_db))
        .filter(recovery_job::Column::NextRunAt.lte(now))
        .filter(
            Condition::any()
                .add(recovery_job::Column::ClaimExpiresAt.is_null())
                .add(recovery_job::Column::ClaimExpiresAt.lte(now)),
        )
        .order_by_asc(recovery_job::Column::NextRunAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to load due pending recovery jobs for action `{}`",
                recovery_action_to_db(action)
            )
        })
}

pub async fn find_blocked_job_by_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    job_id: Option<&str>,
) -> Result<Option<recovery_job::Model>> {
    let blocked = recovery_job_status_to_db(RecoveryJobStatus::Blocked);
    let mut query = recovery_job::Entity::find()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(blocked));
    if let Some(job_id) = job_id {
        query = query.filter(recovery_job::Column::Id.eq(job_id.to_owned()));
    }
    query
        .order_by_desc(recovery_job::Column::UpdatedAt)
        .one(db)
        .await
        .with_context(|| format!("failed to find blocked recovery job for turn `{turn_id}`"))
}

pub async fn resume_blocked_job<C: ConnectionTrait>(
    db: &C,
    job: &recovery_job::Model,
    action: RecoveryAction,
    max_attempts: i64,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let blocked = recovery_job_status_to_db(RecoveryJobStatus::Blocked);
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(pending.to_owned()),
        )
        .col_expr(
            recovery_job::Column::Action,
            sea_orm::sea_query::Expr::value(recovery_action_to_db(action).to_owned()),
        )
        .col_expr(
            recovery_job::Column::MaxAttempts,
            sea_orm::sea_query::Expr::value(max_attempts),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job.id.clone()))
        .filter(recovery_job::Column::Status.eq(blocked))
        .exec(db)
        .await
        .with_context(|| format!("failed to resume blocked recovery job `{}`", job.id))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_job_retrying<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    active_attempt_id: &str,
    next_run_at: DateTimeWithTimeZone,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(pending.to_owned()),
        )
        .col_expr(
            recovery_job::Column::RunCount,
            sea_orm::sea_query::Expr::col(recovery_job::Column::RunCount).add(1),
        )
        .col_expr(
            recovery_job::Column::ProviderAttemptNumber,
            sea_orm::sea_query::Expr::col(recovery_job::Column::ProviderAttemptNumber).add(1),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error.clone()),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::ActiveAttemptId.eq(active_attempt_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark recovery job `{job_id}` retrying"))?
        .rows_affected
        > 0;

    Ok(affected)
}

/// Returns an active observation attempt to the pending queue without
/// consuming a recovery attempt. A temporarily unavailable authority or
/// runtime is not evidence that the recovered execution made no progress.
pub async fn defer_active_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    active_attempt_id: &str,
    next_run_at: DateTimeWithTimeZone,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(pending.to_owned()),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::ActiveAttemptId.eq(active_attempt_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to defer active recovery job `{job_id}`"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_claimed_job_retrying<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    next_run_at: DateTimeWithTimeZone,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(pending.to_owned()),
        )
        .col_expr(
            recovery_job::Column::RunCount,
            sea_orm::sea_query::Expr::col(recovery_job::Column::RunCount).add(1),
        )
        .col_expr(
            recovery_job::Column::ProviderAttemptNumber,
            sea_orm::sea_query::Expr::col(recovery_job::Column::ProviderAttemptNumber).add(1),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark claimed recovery job `{job_id}` retrying"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_claimed_job_active<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    active_attempt_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<ClaimedJobActivation> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);

    let Some(job) = recovery_job::Entity::find()
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .one(db)
        .await
        .with_context(|| format!("failed to load claimed recovery job `{job_id}`"))?
    else {
        return Ok(ClaimedJobActivation::ClaimNotFound);
    };

    let active_for_turn = recovery_job::Entity::find()
        .filter(recovery_job::Column::TurnId.eq(job.turn_id.clone()))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::Id.ne(job_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to check active recovery job for turn `{}`",
                job.turn_id
            )
        })?
        .is_some();

    if active_for_turn {
        return Ok(ClaimedJobActivation::BlockedByActiveRecovery);
    }

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(active.to_owned()),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Some(active_attempt_id.to_owned())),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Some(now)),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark claimed recovery job `{job_id}` active"))?
        .rows_affected
        > 0;

    if affected {
        Ok(ClaimedJobActivation::Activated)
    } else {
        Ok(ClaimedJobActivation::ClaimNotFound)
    }
}

pub async fn release_claimed_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    next_run_at: DateTimeWithTimeZone,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(next_run_at),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to release claimed recovery job `{job_id}`"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_due_pending_job_terminal_if_turn_idle<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    action: RecoveryAction,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);
    let action_db = recovery_action_to_db(action);
    let status_db = recovery_job_status_to_db(status).to_owned();

    let claim_is_available = || {
        Condition::any()
            .add(recovery_job::Column::ClaimExpiresAt.is_null())
            .add(recovery_job::Column::ClaimExpiresAt.lte(now))
    };

    let Some(job) = recovery_job::Entity::find()
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::Action.eq(action_db))
        .filter(recovery_job::Column::NextRunAt.lte(now))
        .filter(claim_is_available())
        .one(db)
        .await
        .with_context(|| format!("failed to load due pending recovery job `{job_id}`"))?
    else {
        return Ok(false);
    };

    let active_for_turn = recovery_job::Entity::find()
        .filter(recovery_job::Column::TurnId.eq(job.turn_id))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::Id.ne(job_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to check active recovery jobs before terminalizing `{job_id}`")
        })?;
    if active_for_turn.is_some() {
        return Ok(false);
    }

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::Action.eq(action_db))
        .filter(recovery_job::Column::NextRunAt.lte(now))
        .filter(claim_is_available())
        .exec(db)
        .await
        .with_context(|| format!("failed to mark due pending recovery job `{job_id}` terminal"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_claimed_job_terminal<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending);
    let status_db = recovery_job_status_to_db(status).to_owned();
    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(pending))
        .filter(recovery_job::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark claimed recovery job `{job_id}` terminal"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_job_terminal<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let status_db = recovery_job_status_to_db(status).to_owned();

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark recovery job `{job_id}` terminal"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_active_without_attempt_terminal<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let status_db = recovery_job_status_to_db(status).to_owned();
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active);

    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(active))
        .filter(recovery_job::Column::ActiveAttemptId.is_null())
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to mark malformed active recovery job `{job_id}` terminal")
        })?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn mark_job_terminal_after_attempt<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    active_attempt_id: &str,
    status: RecoveryJobStatus,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let status_db = recovery_job_status_to_db(status).to_owned();
    let affected = recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(status_db),
        )
        .col_expr(
            recovery_job::Column::RunCount,
            sea_orm::sea_query::Expr::col(recovery_job::Column::RunCount).add(1),
        )
        .col_expr(
            recovery_job::Column::ProviderAttemptNumber,
            sea_orm::sea_query::Expr::col(recovery_job::Column::ProviderAttemptNumber).add(1),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .filter(recovery_job::Column::Id.eq(job_id.to_owned()))
        .filter(
            recovery_job::Column::Status.eq(recovery_job_status_to_db(RecoveryJobStatus::Active)),
        )
        .filter(recovery_job::Column::ActiveAttemptId.eq(active_attempt_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark recovery job `{job_id}` terminal after attempt"))?
        .rows_affected
        > 0;

    Ok(affected)
}

pub async fn find_job_by_id<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
) -> Result<Option<recovery_job::Model>> {
    recovery_job::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to load recovery job `{job_id}`"))
}

pub async fn find_open_jobs_by_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<recovery_job::Model>> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending).to_owned();
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active).to_owned();

    recovery_job::Entity::find()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .filter(recovery_job::Column::Status.is_in([pending, active]))
        .order_by_asc(recovery_job::Column::ScheduledAt)
        .all(db)
        .await
        .with_context(|| format!("failed to load open recovery jobs for turn `{turn_id}`"))
}

pub async fn cancel_open_jobs_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    exclude_job_id: Option<&str>,
    reason: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<Vec<recovery_job::Model>> {
    let pending = recovery_job_status_to_db(RecoveryJobStatus::Pending).to_owned();
    let active = recovery_job_status_to_db(RecoveryJobStatus::Active).to_owned();
    let cancelled = recovery_job_status_to_db(RecoveryJobStatus::Cancelled).to_owned();

    let mut query = recovery_job::Entity::find()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .filter(recovery_job::Column::Status.is_in([pending.clone(), active.clone()]));
    if let Some(excluded) = exclude_job_id {
        query = query.filter(recovery_job::Column::Id.ne(excluded.to_owned()));
    }

    let jobs = query
        .all(db)
        .await
        .with_context(|| format!("failed to load open recovery jobs for turn `{turn_id}`"))?;

    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let ids = jobs.iter().map(|job| job.id.clone()).collect::<Vec<_>>();
    recovery_job::Entity::update_many()
        .col_expr(
            recovery_job::Column::Status,
            sea_orm::sea_query::Expr::value(cancelled),
        )
        .col_expr(
            recovery_job::Column::LastError,
            sea_orm::sea_query::Expr::value(reason),
        )
        .col_expr(
            recovery_job::Column::ClaimToken,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ClaimExpiresAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptId,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            recovery_job::Column::ActiveAttemptStartedAt,
            sea_orm::sea_query::Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            recovery_job::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(recovery_job::Column::Id.is_in(ids))
        .exec(db)
        .await
        .with_context(|| format!("failed to cancel open recovery jobs for turn `{turn_id}`"))?;

    Ok(jobs)
}

pub async fn find_jobs_by_turn_and_status<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    status: RecoveryJobStatus,
) -> Result<Vec<recovery_job::Model>> {
    recovery_job::Entity::find()
        .filter(recovery_job::Column::TurnId.eq(turn_id.to_owned()))
        .filter(recovery_job::Column::Status.eq(recovery_job_status_to_db(status)))
        .all(db)
        .await
        .with_context(|| {
            format!("failed to load recovery jobs for turn `{turn_id}` with status `{status:?}`")
        })
}

pub async fn list_active_jobs<C: ConnectionTrait>(
    db: &C,
    limit: u64,
) -> Result<Vec<recovery_job::Model>> {
    recovery_job::Entity::find()
        .filter(
            recovery_job::Column::Status.eq(recovery_job_status_to_db(RecoveryJobStatus::Active)),
        )
        .order_by_asc(recovery_job::Column::ActiveAttemptStartedAt)
        .order_by_asc(recovery_job::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to load active recovery jobs")
}
