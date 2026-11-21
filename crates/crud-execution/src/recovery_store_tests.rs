use super::*;
use pioneer_sqlite::{
    SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteEvent, SqliteWriteExecutor,
    SqliteWriteObserver,
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::sync::Mutex;
use std::time::Duration;

#[derive(Default)]
struct RecoveryRoutes {
    reads: Mutex<Vec<SqliteReadClass>>,
    events: Mutex<Vec<SqliteWriteEvent>>,
}

impl SqliteReadObserver for RecoveryRoutes {
    fn observe(&self, event: SqliteReadEvent) {
        if let SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}

impl SqliteWriteObserver for RecoveryRoutes {
    fn observe(&self, event: SqliteWriteEvent) {
        self.events.lock().unwrap().push(event);
    }
}

async fn recovery_discovery_fixture() -> (SqliteDatabase, Arc<RecoveryRoutes>) {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let observer = Arc::new(RecoveryRoutes::default());
    let executor = SqliteWriteExecutor::with_observer(writer, observer.clone());
    (
        SqliteDatabase::from_executor_with_read_observer(reader, executor, observer.clone()),
        observer,
    )
}

#[pioneer_postgres_test::postgres_test]
async fn empty_recovery_discovery_and_active_reads_do_not_reserve_writer() {
    use pioneer_sqlite::SqliteReadClass;
    let (db, observer) = recovery_discovery_fixture().await;
    let wake = Arc::new(Notify::new());
    let held_writer = db.begin().await.unwrap();
    for (scoped, expected_class) in [
        (db.clone(), SqliteReadClass::Interactive),
        (db.maintenance(), SqliteReadClass::Maintenance),
        (
            db.maintenance().with_critical_writes(),
            SqliteReadClass::Maintenance,
        ),
    ] {
        let scoped = RecoveryStore::new(&scoped, &wake);
        observer.events.lock().unwrap().clear();
        observer.reads.lock().unwrap().clear();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            assert!(
                scoped
                    .claim_due_recovery_jobs(1_700_000_000, 45, 64)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                scoped
                    .active_recovery_job_upper_id()
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                scoped
                    .list_active_recovery_jobs(64)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                scoped
                    .list_due_pending_recovery_jobs_by_action(
                        RecoveryAction::RetryWithBackoff,
                        1_700_000_000,
                        64,
                    )
                    .await
                    .unwrap()
                    .is_empty()
            );
        })
        .await
        .expect("recovery reads must complete while writer is occupied");
        assert!(
            observer.events.lock().unwrap().is_empty(),
            "empty discovery must not request writer capacity"
        );
        assert_eq!(*observer.reads.lock().unwrap(), vec![expected_class; 4]);
    }
    held_writer.rollback().await.unwrap();
}

#[tokio::test]
async fn claimed_recovery_job_can_be_marked_active_and_retried() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = SqliteDatabase::new(
            fixture.connect_read_only().await.unwrap(),
            fixture.connection().clone(),
        );
        let wake = Arc::new(Notify::new());
        let store = RecoveryStore::new(&db, &wake);
        let turn_id = "turn_recovery_budget";

        let job = store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_1".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::Fallback,
                Some("provider failed".to_owned()),
                None,
                None,
                None,
                0,
                1,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_000,
            )
            .await
            .expect("job should enqueue");

        let claimed = store
            .claim_due_recovery_jobs(1_700_000_001, 45, 1)
            .await
            .expect("job should claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, job.id);
        let claim_token = claimed[0]
            .claim_token
            .as_deref()
            .expect("claimed job should have claim token");
        assert!(matches!(
            store
                .mark_claimed_recovery_job_active(
                    job.id.as_str(),
                    claim_token,
                    "recovery_attempt_1",
                    1_700_000_001,
                )
                .await
                .expect("job should transition to active"),
            ClaimedRecoveryActivation::Activated
        ));
        assert!(
            store
                .mark_recovery_job_retrying(
                    job.id.as_str(),
                    "recovery_attempt_1",
                    1_700_000_010,
                    None,
                    Some("provider failed during recovery".to_owned()),
                    1_700_000_002,
                )
                .await
                .expect("active job should requeue")
        );

        let job = store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("job should reload")
            .expect("job should exist");
        assert_eq!(job.status, RecoveryJobStatus::Pending);
        assert_eq!(job.run_count, 1);
        assert_eq!(job.provider_attempt_number, 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unresolved_recovery_episode_rejects_duplicate_jobs_for_turn() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = SqliteDatabase::new(
            fixture.connect_read_only().await.unwrap(),
            fixture.connection().clone(),
        );
        let wake = Arc::new(Notify::new());
        let store = RecoveryStore::new(&db, &wake);
        let turn_id = "turn_single_active_recovery";

        for index in 0..2 {
            let enqueued = store
                .enqueue_recovery_job(
                    turn_id.to_owned(),
                    format!("reasoning_{index}"),
                    TurnItemType::Reasoning,
                    None,
                    RecoveryTrigger::ProviderError,
                    RecoveryAction::RetryWithBackoff,
                    Some("provider failed".to_owned()),
                    None,
                    None,
                    None,
                    0,
                    3,
                    serde_json::json!({}),
                    serde_json::json!({}),
                    1_700_000_000,
                )
                .await;
            if index == 0 {
                enqueued.expect("first recovery episode should enqueue");
            } else {
                let error = enqueued.expect_err("a second unresolved episode must be rejected");
                assert!(
                    matches!(
                        error.downcast_ref::<sea_orm::DbErr>().and_then(sea_orm::DbErr::sql_err),
                        Some(sea_orm::SqlErr::UniqueConstraintViolation(message))
                            if message.contains("uq_recovery_job_unresolved_turn")
                    ),
                    "expected the unresolved turn constraint, got {error:#}"
                );
            }
        }

        let claimed = store
            .claim_due_recovery_jobs(1_700_000_001, 45, 2)
            .await
            .expect("jobs should claim");
        assert_eq!(claimed.len(), 1);

        let first_token = claimed[0]
            .claim_token
            .as_deref()
            .expect("first claimed job should have claim token");
        assert!(matches!(
            store
                .mark_claimed_recovery_job_active(
                    claimed[0].id.as_str(),
                    first_token,
                    "recovery_attempt_1",
                    1_700_000_001,
                )
                .await
                .expect("first job should activate"),
            ClaimedRecoveryActivation::Activated
        ));
        let first = store
            .get_recovery_job(claimed[0].id.as_str())
            .await
            .expect("job should reload")
            .expect("job should exist");
        assert_eq!(first.status, RecoveryJobStatus::Active);
        assert_eq!(first.run_count, claimed[0].run_count);
        assert!(first.claim_token.is_none());
        assert_eq!(
            first.active_attempt_id.as_deref(),
            Some("recovery_attempt_1")
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn watchdog_rejects_case_only_policy_race_under_case_insensitive_collation() {
    use pioneer_entity::recovery_job as entity;
    let (connection, _observer) = recovery_discovery_fixture().await;
    connection.validate_reader().await.unwrap();
    let wake = Arc::new(Notify::new());
    let store = RecoveryStore::new(&connection, &wake);
    connection
        .execute_unprepared(
            "CREATE COLLATION recovery_case_insensitive (
                provider = icu, locale = 'und-u-ks-level2', deterministic = false
            )",
        )
        .await
        .unwrap();
    connection
        .execute_unprepared(
            "ALTER TABLE recovery_job ALTER COLUMN policy_snapshot
             TYPE text COLLATE recovery_case_insensitive",
        )
        .await
        .unwrap();
    let now = 1_700_000_000;
    let upper = serde_json::json!({"MAX_WALL_CLOCK_SECS":3600});
    let lower = serde_json::json!({"max_wall_clock_secs":3600});
    let job = store
        .enqueue_recovery_job(
            "nocase-turn".into(),
            "item".into(),
            TurnItemType::Reasoning,
            None,
            RecoveryTrigger::ProviderError,
            RecoveryAction::RetryAttempt,
            None,
            None,
            None,
            None,
            0,
            3,
            serde_json::json!({}),
            upper.clone(),
            now,
        )
        .await
        .unwrap();
    let claimed = store.claim_due_recovery_jobs(now + 1, 45, 1).await.unwrap();
    assert!(matches!(
        store
            .mark_claimed_recovery_job_active(
                &job.id,
                claimed[0].claim_token.as_deref().unwrap(),
                "nocase-attempt",
                now + 1
            )
            .await
            .unwrap(),
        ClaimedRecoveryActivation::Activated
    ));
    let expected = store.get_recovery_job(&job.id).await.unwrap().unwrap();
    assert!(
        expected
            .policy_snapshot
            .get("max_wall_clock_secs")
            .is_none()
    );
    // Old Rust policy falls back to 60s; elapsed=100 would terminalize.
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let worker_entered = entered.clone();
    let worker_release = release.clone();
    let worker_connection = connection.clone();
    let worker_wake = wake.clone();
    let worker_expected = expected.clone();
    let worker = tokio::spawn(async move {
        let worker_store = RecoveryStore::new(&worker_connection, &worker_wake);
        *worker_store.watchdog_preparation_gate.lock().unwrap() =
            Some((worker_entered, worker_release));
        worker_store
            .apply_recovery_watchdog_failure(
                &worker_expected,
                RecoveryJobStatus::Exhausted,
                None,
                Some("old fallback budget".into()),
                None,
                now + 100,
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    entity::Entity::update_many()
        .col_expr(
            entity::Column::PolicySnapshot,
            sea_orm::sea_query::Expr::value(lower.to_string()),
        )
        .filter(entity::Column::Id.eq(job.id.clone()))
        .exec(&connection)
        .await
        .unwrap();
    // Demonstrate the old SQL fence's actual false equality in this fixture.
    let sql_equal = entity::Entity::find_by_id(job.id.clone())
        .filter(entity::Column::PolicySnapshot.eq(upper.to_string()))
        .one(&connection)
        .await
        .unwrap();
    assert!(
        sql_equal.is_some(),
        "fixture must defeat ordinary TEXT equality"
    );
    release.notify_one();
    assert!(!worker.await.unwrap().unwrap());
    let retained = store.get_recovery_job(&job.id).await.unwrap().unwrap();
    assert_eq!(retained.status, RecoveryJobStatus::Active);
    assert_eq!(retained.policy_snapshot, lower);
    assert_eq!(retained.run_count, expected.run_count);
    assert_eq!(
        retained.claim_token, expected.claim_token,
        "mismatch must roll back temporary token"
    );
}
