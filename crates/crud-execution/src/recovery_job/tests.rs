use super::*;
use chrono::{TimeZone, Utc};
use pioneer_postgres_test::{postgres_test, scoped_database};
use sea_orm::ActiveModelTrait;

fn at(seconds: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_opt(seconds, 0).unwrap().fixed_offset()
}

fn job(action: RecoveryAction, next_run_at: DateTimeWithTimeZone) -> NewRecoveryJob {
    NewRecoveryJob {
        diagnostic_json: None,
        turn_id: "recovery-discovery-turn".into(),
        item_id: "item".into(),
        item_type: TurnItemType::Reasoning,
        source_attempt_id: None,
        trigger: RecoveryTrigger::ProviderError,
        action,
        reason: None,
        policy_json: "{}".into(),
        error_class: None,
        transport_stage: None,
        retry_after_ms: None,
        provider_attempt_number: 0,
        policy_snapshot_json: "{}".into(),
        max_attempts: 3,
        created_at: at(1_700_000_000),
        scheduled_at: at(1_700_000_000),
        next_run_at,
    }
}

async fn claim(db: &impl ConnectionTrait, now: i64) -> Vec<recovery_job::Model> {
    let due = list_due_job_ids(db, at(now), 1).await.unwrap();
    claim_due_job_ids(db, &due, at(now), at(now + 45))
        .await
        .unwrap()
}

#[postgres_test]
async fn recovery_due_deadline_tracks_schedule_and_pending_claim_lease() {
    let fixture = scoped_database().await.unwrap();
    let writer = fixture.connection();
    let reader = fixture.connect_read_only().await.unwrap();
    let now = 1_700_000_000;
    assert_eq!(next_due_at(&reader).await.unwrap(), None);

    let job = enqueue_recovery_job(writer, job(RecoveryAction::RetryAttempt, at(now + 10)))
        .await
        .unwrap();
    assert_eq!(next_due_at(&reader).await.unwrap(), Some(now + 10));
    let mut fractional: recovery_job::ActiveModel = job.clone().into();
    fractional.next_run_at = Set(at(now + 10) + chrono::Duration::milliseconds(500));
    let fractional = fractional.update(writer).await.unwrap();
    assert_eq!(next_due_at(&reader).await.unwrap(), Some(now + 11));
    assert!(claim(writer, now + 10).await.is_empty());
    let mut exact: recovery_job::ActiveModel = fractional.into();
    exact.next_run_at = Set(at(now + 10));
    exact.update(writer).await.unwrap();

    let claimed = claim(writer, now + 10).await;
    assert_eq!(claimed.len(), 1);
    assert_eq!(next_due_at(&reader).await.unwrap(), Some(now + 55));
    assert_eq!(
        mark_claimed_job_active(
            writer,
            &job.id,
            claimed[0].claim_token.as_deref().unwrap(),
            "deadline-attempt",
            at(now + 10),
        )
        .await
        .unwrap(),
        ClaimedJobActivation::Activated
    );
    assert_eq!(next_due_at(&reader).await.unwrap(), None);
}

#[postgres_test]
async fn recovery_claim_revalidates_reschedule_and_renewed_lease() {
    let fixture = scoped_database().await.unwrap();
    let writer = fixture.connection();
    let reader = fixture.connect_read_only().await.unwrap();
    let producer = fixture.connect().await.unwrap();
    let now = 1_700_000_000;
    let job = enqueue_recovery_job(writer, job(RecoveryAction::RetryWithBackoff, at(now)))
        .await
        .unwrap();
    assert!(list_active_jobs(&reader, 1).await.unwrap().is_empty());
    let ids = list_due_job_ids(&reader, at(now), 1).await.unwrap();
    assert_eq!(ids, vec![job.id.clone()]);
    let mut rescheduled: recovery_job::ActiveModel = job.clone().into();
    rescheduled.next_run_at = Set(at(now + 10));
    rescheduled.update(&producer).await.unwrap();

    assert!(
        claim_due_job_ids(writer, &ids, at(now), at(now + 45))
            .await
            .unwrap()
            .is_empty(),
        "stale page must respect rescheduled next_run_at"
    );
    let first = claim(writer, now + 10).await.remove(0);
    assert!(claim(writer, now + 54).await.is_empty());
    let second = claim(writer, now + 55).await.remove(0);
    assert_ne!(first.claim_token, second.claim_token);
    assert!(
        claim_due_job_ids(writer, &ids, at(now + 55), at(now + 100))
            .await
            .unwrap()
            .is_empty(),
        "the original candidate must respect a renewed foreign claim"
    );
    assert!(
        !release_claimed_job(
            writer,
            &job.id,
            first.claim_token.as_deref().unwrap(),
            at(now + 55),
            None,
            at(now + 55),
        )
        .await
        .unwrap()
    );
    assert_eq!(
        mark_claimed_job_active(
            writer,
            &job.id,
            second.claim_token.as_deref().unwrap(),
            "attempt",
            at(now + 55),
        )
        .await
        .unwrap(),
        ClaimedJobActivation::Activated
    );
    assert_eq!(list_active_jobs(&reader, 1).await.unwrap()[0].id, job.id);
}
