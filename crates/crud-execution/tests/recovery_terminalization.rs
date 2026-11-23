use std::time::Duration;

use anyhow::{Context, Result};
use chrono::TimeDelta;
use pioneer_crud_execution::recovery_terminalization_outbox::{
    claim_due, enqueue_for_terminal_job, mark_delivered,
};
use pioneer_entity::recovery_job;
use pioneer_postgres_test::with_database;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};

#[tokio::test]
async fn independent_claimers_skip_held_rows_and_rollback_makes_the_claim_retryable() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace (id,name) VALUES ('ws-recovery','Recovery');
             INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status)
                 VALUES ('thread-recovery','ws-recovery','','agent','model','provider','active');
             INSERT INTO turn (id, thread_id, status, turn_kind, origin)
                 VALUES ('turn-recovery-a','thread-recovery','failed','conversation','system'),
                        ('turn-recovery-b','thread-recovery','failed','conversation','system');
             INSERT INTO recovery_job (id, turn_id, item_id, item_type, status, trigger, action)
                 VALUES ('job-recovery-a','turn-recovery-a','item-a','reasoning','failed','runtime_failure','mark_failed'),
                        ('job-recovery-b','turn-recovery-b','item-b','reasoning','failed','runtime_failure','mark_failed');",
        ).await?;
        let now = DateTimeWithTimeZone::parse_from_rfc3339("2026-11-19T10:00:00Z")?;
        let expiry = now + TimeDelta::minutes(1);
        for id in ["job-recovery-a", "job-recovery-b"] {
            let job = recovery_job::Entity::find_by_id(id)
                .one(db)
                .await?
                .context("expected recovery fixture job")?;
            enqueue_for_terminal_job(db, &job, now).await?;
        }

        let first = db.begin().await?;
        let first_batch = claim_due(&first, now, expiry, 1).await?;
        assert_eq!(first_batch[0].row.recovery_job_id, "job-recovery-a");
        let rolled_back_token = &first_batch[0].claim_token;

        let other_pool = fixture.connect().await?;
        let second = other_pool.begin().await?;
        let other_batch = tokio::time::timeout(
            Duration::from_secs(2),
            claim_due(&second, now, expiry, 2),
        ).await.context("second claimant waited for the first row")??;
        assert_eq!(other_batch.len(), 1);
        assert_eq!(other_batch[0].row.recovery_job_id, "job-recovery-b");
        second.commit().await?;
        first.rollback().await?;

        let retry = other_pool.begin().await?;
        let retried = claim_due(&retry, now, expiry, 2).await?;
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].row.recovery_job_id, "job-recovery-a");
        assert_ne!(retried[0].claim_token, *rolled_back_token);
        assert!(!mark_delivered(&retry, "job-recovery-a", rolled_back_token, now).await?);
        assert!(mark_delivered(&retry, "job-recovery-a", &retried[0].claim_token, now).await?);
        retry.commit().await?;
        Ok(())
    })
    .await
}
