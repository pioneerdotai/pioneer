//! Coordinate recovery settlement and shell-control release in one transaction.

use crate::store::conversations::context_writes;
use anyhow::Result;
use pioneer_crud_execution::recovery_terminalization_outbox as execution;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

pub(crate) struct ReleaseShellControls;

impl execution::RecoverySettlement for ReleaseShellControls {
    async fn release(&self, tx: &DatabaseTransaction, job_ids: &[String]) -> Result<()> {
        context_writes::settle_shell_controls_for_recovery_jobs(tx, job_ids).await?;
        Ok(())
    }
}

pub async fn mark_delivered<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    claim_token: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = execution::mark_delivered(db, job_id, claim_token, now).await?;
    if affected {
        context_writes::settle_shell_controls_for_recovery_jobs(db, &[job_id.to_owned()]).await?;
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
        context_writes::settle_shell_controls_for_recovery_jobs(db, &[job_id.to_owned()]).await?;
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
        context_writes::settle_shell_controls_for_recovery_jobs(db, job_ids).await?;
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
        context_writes::settle_shell_controls_for_recovery_jobs(db, &[job_id.to_owned()]).await?;
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
        context_writes::settle_shell_controls_for_recovery_jobs(db, job_ids).await?;
    }
    Ok(affected)
}
