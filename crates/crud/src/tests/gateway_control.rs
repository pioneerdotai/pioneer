use super::*;

#[tokio::test]
async fn gateway_control_heads_park_empty_and_terminal_upload_history() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_control_heads").await;
        let at = 1_700_000_000;
        assert_eq!(store.next_gateway_control_deadline(at).await.unwrap(), None);
        let db = store.database_connection();
        let mut wake = db.work_signal("gateway.control").subscribe();
        wake.begin_pass(&db).await.unwrap();
        let record = SkillUploadSessionRecord {
            purpose: "skill".into(),
            upload_id: "UUUUUUUUUUUUUUUUUUUUU".into(),
            workspace_id: "ws_control_heads".into(),
            connection_id: 7,
            status: "receiving".into(),
            file_name: "skills.tar.gz".into(),
            archive_format: "tar_gz".into(),
            compressed_size_bytes: 1,
            received_bytes: 0,
            sha256: "a".repeat(64),
            payload_path: "/unused/control-upload".into(),
            created_at_unix: at,
            expires_at_unix: at + 100,
            finalized_at_unix: None,
            consumed_at_unix: None,
            aborted_at_unix: None,
        };
        store.insert_skill_upload_session(&record).await.unwrap();
        assert!(futures_util::FutureExt::now_or_never(wake.wait(None)).is_some());
        wake.begin_pass(&db).await.unwrap();
        assert_eq!(
            store.next_gateway_control_deadline(at).await.unwrap(),
            Some(at + 100)
        );
        assert!(
            store
                .transition_skill_upload_status(
                    &record.upload_id,
                    &["receiving"],
                    "aborted",
                    None,
                    None,
                    Some(at + 1),
                    at + 1
                )
                .await
                .unwrap()
                .is_some()
        );
        assert!(futures_util::FutureExt::now_or_never(wake.wait(None)).is_some());
        wake.begin_pass(&db).await.unwrap();
        assert_eq!(
            store.next_gateway_control_deadline(at + 1).await.unwrap(),
            None,
            "terminal retained rows must not manufacture a periodic cleanup obligation"
        );
        assert!(
            store
                .transition_skill_upload_status(
                    &record.upload_id,
                    &["receiving"],
                    "aborted",
                    None,
                    None,
                    Some(at + 2),
                    at + 2
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            futures_util::FutureExt::now_or_never(wake.wait(None)).is_none(),
            "a stale transition must not restart its own discovery"
        );
        Ok(())
    })
    .await
    .unwrap();
}

/// Exercise a PostgreSQL COMMIT that settles after the calling future is cancelled.
#[pioneer_postgres_test::postgres_test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_sent_acquisition_commit_settles_before_owner_can_park() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Default)]
    struct WriterBoundary {
        armed: AtomicBool,
        acquired: tokio::sync::Notify,
    }
    impl SqliteWriteObserver for WriterBoundary {
        fn observe(&self, event: SqliteWriteEvent) {
            if matches!(event, SqliteWriteEvent::Acquired { .. })
                && self.armed.swap(false, Ordering::SeqCst)
            {
                self.acquired.notify_one();
            }
        }
    }
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(2).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let boundary = Arc::new(WriterBoundary::default());
    let database = SqliteDatabase::from_executor(
        reader,
        SqliteWriteExecutor::with_observer(writer.clone(), boundary.clone()),
    );
    database.validate_reader().await.unwrap();
    let store = CrudStore::new(database);
    let now = utc_now();
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_late_commit".to_owned()),
        name: Set("Late commit".to_owned()),
        is_active: Set(true),
        is_current: Set(false),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(&store.connection)
    .await
    .unwrap();
    let (store, thread, turn) = start_test_turn(
        store,
        "ws_late_commit",
        "thr_late_commit",
        "turn_late_commit",
    )
    .await;
    let owner = "late-commit-owner";
    assert!(
        store
            .supervise_turn_execution_heartbeat(owner, 30)
            .await
            .unwrap()
    );
    // The test-only constraint runs when the server starts COMMIT. The
    // blocker is on another pool in the fixture's database.
    fixture
        .connection()
        .execute_unprepared(
            "CREATE FUNCTION wait_for_acquisition_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_advisory_xact_lock(7312491); RETURN NEW; END $$;
         CREATE CONSTRAINT TRIGGER wait_for_acquisition_commit AFTER INSERT ON turn_execution
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wait_for_acquisition_commit()",
        )
        .await
        .unwrap();
    let blocker = fixture.connection().begin().await.unwrap();
    blocker
        .execute_unprepared("SELECT pg_advisory_xact_lock(7312491)")
        .await
        .unwrap();
    let blocker_pid: i32 = blocker
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT pg_backend_pid() AS pid",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "pid")
        .unwrap();
    let receipt_lease = now + chrono::Duration::seconds(1);
    let acquisition_store = store.clone();
    let turn_id = turn.id.clone();
    let acquisition = tokio::spawn(async move {
        acquisition_store
            .materialize_turn_events_atomically_with_optional_admission(
                Vec::new(),
                now.timestamp(),
                None,
                None,
                None,
                Some(NewTurnExecution {
                    turn_id,
                    thread_id: thread.id,
                    workspace_id: thread.workspace_id,
                    executor_kind: TurnExecutorKind::ApiProvider,
                    executor_key: Some("openai".to_owned()),
                    status: TurnExecutionStatus::Starting,
                    owner_id: owner.to_owned(),
                    lease_until: receipt_lease,
                    created_at: now,
                }),
                None,
                None,
                None,
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = fixture
                .connection()
                .query_one_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT EXISTS (
                        SELECT 1 FROM pg_stat_activity
                        WHERE datname=current_database() AND query='COMMIT'
                          AND $1 = ANY(pg_blocking_pids(pid))
                     ) AS waiting",
                    [blocker_pid.into()],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "waiting")
                .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server must receive COMMIT before cancelling the caller");
    acquisition.abort();
    assert!(acquisition.await.unwrap_err().is_cancelled());
    assert!(store.execution_heartbeats.owner(owner).observation().1);
    // No Rust acquisition guard remains. The independent reader really
    // observes empty while the already-sent COMMIT is still blocked.
    assert!(
        turn_execution::list_owned_active(&store.connection, owner, 200)
            .await
            .unwrap()
            .is_empty()
    );
    let wake = store.turn_execution_heartbeat_notification(owner);
    tokio::time::timeout(Duration::from_secs(5), wake.notified())
        .await
        .unwrap();
    // An independent session of the real writer pool remains available
    // while the first backend is still inside its cancelled caller's COMMIT.
    tokio::time::timeout(
        Duration::from_secs(5),
        writer.execute_unprepared("SELECT 1"),
    )
    .await
    .expect("another writer is available before the ownership fence")
    .unwrap();
    boundary.armed.store(true, Ordering::SeqCst);
    let heartbeat = store.supervise_turn_execution_heartbeat(owner, 30);
    tokio::pin!(heartbeat);
    // An UPDATE on the second session alone cannot settle the first
    // session's COMMIT. Wait for the scoped writer admission and poll the
    // heartbeat while the server-side COMMIT blocker still holds.
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut heartbeat => panic!("parked before late COMMIT settled: {result:?}"),
            _ = boundary.acquired.notified() => {}
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut heartbeat)
            .await
            .is_err(),
        "owner must keep waiting while the first backend is still committing"
    );
    blocker.commit().await.unwrap();
    assert!(
        !tokio::time::timeout(Duration::from_secs(5), &mut heartbeat)
            .await
            .unwrap()
            .unwrap()
    );
    let renewed = store.get_turn_execution(&turn.id).await.unwrap().unwrap();
    assert_eq!(renewed.owner_id, owner);
    assert_eq!(renewed.owner_generation, 1);
    assert!(renewed.lease_until > receipt_lease);
    assert!(!store.execution_heartbeats.owner(owner).observation().2);
    // No producer registered or sent another kick after cancellation.
    assert!(
        tokio::time::timeout(Duration::ZERO, wake.notified())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn atomic_execution_admission_wakes_parked_owner_and_terminal_receipt_parks_again() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) = postgres_store_with_started_turn(
            fixture,
            "ws_heartbeat_admission",
            "thr_heartbeat_admission",
            "turn_heartbeat_admission",
        )
        .await;
        let scoped = store.with_maintenance_reads_and_critical_writes();
        let owner = "heartbeat-admission-owner";
        assert!(
            scoped
                .supervise_turn_execution_heartbeat(owner, 30)
                .await
                .unwrap()
        );
        let wake = scoped.turn_execution_heartbeat_notification(owner);
        let now = utc_now();
        // Exercise the same atomic admission owner used by public Turn-start
        // APIs. The fixture already created the Turn, so only its receipt is new.
        store
            .materialize_turn_events_atomically_with_optional_admission(
                Vec::new(),
                now.timestamp(),
                None,
                None,
                None,
                Some(NewTurnExecution {
                    turn_id: turn.id.clone(),
                    thread_id: thread.id.clone(),
                    workspace_id: thread.workspace_id.clone(),
                    executor_kind: TurnExecutorKind::ApiProvider,
                    executor_key: Some("openai".to_owned()),
                    status: TurnExecutionStatus::Starting,
                    owner_id: owner.to_owned(),
                    lease_until: now + chrono::Duration::seconds(1),
                    created_at: now,
                }),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), wake.notified())
            .await
            .unwrap();
        assert!(
            !scoped
                .supervise_turn_execution_heartbeat(owner, 30)
                .await
                .unwrap()
        );
        let renewed = store.get_turn_execution(&turn.id).await.unwrap().unwrap();
        assert!(renewed.lease_until > now + chrono::Duration::seconds(1));
        assert_eq!(renewed.owner_generation, 1);
        store
            .update_turn_status(
                &thread.id,
                &turn.id,
                TurnStatus::Completed,
                None,
                now.timestamp(),
            )
            .await
            .unwrap();
        assert!(
            scoped
                .supervise_turn_execution_heartbeat(owner, 30)
                .await
                .unwrap()
        );
        let receipt = store.get_turn_execution(&turn.id).await.unwrap().unwrap();
        assert_eq!(receipt.status, TurnExecutionStatus::Completed);
        Ok(())
    })
    .await
    .unwrap();
}
