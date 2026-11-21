//! Coordinate recovery settlement and shell-control release in one transaction.

use anyhow::Result;
use pioneer_crud_execution::recovery_terminalization_outbox as execution;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

pub(crate) struct ReleaseShellControls;

impl execution::RecoverySettlement for ReleaseShellControls {
    async fn release(&self, tx: &DatabaseTransaction, job_ids: &[String]) -> Result<()> {
        super::turn_llm_context::settle_shell_controls_for_recovery_jobs(tx, job_ids).await?;
        Ok(())
    }
}

pub use execution::{
    STATUS_CANCELLED, STATUS_DELIVERED, STATUS_DELIVERING, STATUS_FAILED, STATUS_PENDING,
    cancel_undelivered_for_job, claim_due, enqueue_for_terminal_job, mark_failed,
};

pub async fn mark_delivered<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = execution::mark_delivered(db, job_id, claim_token, now).await?;
    if affected {
        super::turn_llm_context::settle_shell_controls_for_recovery_jobs(db, &[job_id.to_owned()])
            .await?;
    }
    Ok(affected)
}

pub async fn quarantine_claim<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    reason_code: &str,
    error: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected =
        execution::quarantine_claim(db, job_id, claim_token, reason_code, error, now).await?;
    if affected {
        super::turn_llm_context::settle_shell_controls_for_recovery_jobs(db, &[job_id.to_owned()])
            .await?;
    }
    Ok(affected)
}

pub async fn quarantine_claim_batch<C: ConnectionTrait>(
    db: &C,
    job_ids: &[String],
    claim_token: &str,
    reason_code: &str,
    error: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let affected =
        execution::quarantine_claim_batch(db, job_ids, claim_token, reason_code, error, now)
            .await?;
    if affected > 0 {
        super::turn_llm_context::settle_shell_controls_for_recovery_jobs(db, job_ids).await?;
    }
    Ok(affected)
}

pub async fn cancel_claim<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    reason: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = execution::cancel_claim(db, job_id, claim_token, reason, now).await?;
    if affected {
        super::turn_llm_context::settle_shell_controls_for_recovery_jobs(db, &[job_id.to_owned()])
            .await?;
    }
    Ok(affected)
}

pub async fn cancel_claim_batch<C: ConnectionTrait>(
    db: &C,
    job_ids: &[String],
    claim_token: &str,
    reason: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let affected = execution::cancel_claim_batch(db, job_ids, claim_token, reason, now).await?;
    if affected > 0 {
        super::turn_llm_context::settle_shell_controls_for_recovery_jobs(db, job_ids).await?;
    }
    Ok(affected)
}
