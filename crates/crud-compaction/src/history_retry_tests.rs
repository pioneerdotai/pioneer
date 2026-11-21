use super::{CompactionStore, store};
use crate::history_check_result::{HistoryCheckDiagnostic as D, HistoryCheckOutcome as O};
use pioneer_crud_conversations::{
    events::TurnEventPayload,
    turn_item::{prepare_turn_item_projection, upsert_prepared_turn_item},
    turn_reads::get_turn_for_thread,
};
use pioneer_crud_execution::{
    event_append::{PreparedTurnEvent, append_prepared_event},
    time::unix_to_datetime,
};
use pioneer_entity::compaction_history_check as check;
use pioneer_postgres_test::with_database;
use pioneer_protocol::{
    ItemCompletedNotification, TurnCompletedNotification, TurnItem, TurnStatus,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DbBackend, EntityTrait, Statement, TransactionTrait};

async fn start_history_check_root(store: &CompactionStore, turn_id: &str) {
    let db = store.database_connection();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "DELETE FROM turn WHERE id = $1",
        [turn_id.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "DELETE FROM compaction_turn_creation WHERE turn_id = $1",
        [turn_id.into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO turn (id, thread_id, status, turn_kind, origin,
                           initiated_by_actor_kind, initiated_by_actor_id, created_at, updated_at)
         VALUES ($1, 'thread', 'in_progress', 'conversation', 'user',
                 'principal', 'P00000000000000000001', to_timestamp(1), to_timestamp(1))",
        [turn_id.into()],
    ))
    .await
    .unwrap();
}

async fn publish_history_check_root(store: &CompactionStore, turn_id: &str) {
    let db = store.database_connection();
    let (_, mut turn) = get_turn_for_thread(&db, "thread", turn_id)
        .await
        .unwrap()
        .unwrap();
    turn.status = TurnStatus::Completed;
    let item = TurnItem::AgentMessage {
        id: "root-answer".into(),
        text: "ready root answer".into(),
        phase: Default::default(),
        markdown: None,
        markdown_version: None,
    };
    let timestamp = unix_to_datetime(1);
    let projection =
        prepare_turn_item_projection(turn_id, &item, Some("completed"), timestamp, timestamp)
            .unwrap();
    let item_event = PreparedTurnEvent::prepare_for_storage(TurnEventPayload::ItemCompleted(
        ItemCompletedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: turn_id.into(),
            item,
        },
    ))
    .unwrap();
    let completed_event = PreparedTurnEvent::prepare_for_storage(TurnEventPayload::TurnCompleted(
        TurnCompletedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn,
        },
    ))
    .unwrap();

    let tx = db.begin().await.unwrap();
    append_prepared_event(&tx, item_event, timestamp)
        .await
        .unwrap();
    upsert_prepared_turn_item(&tx, projection).await.unwrap();
    append_prepared_event(&tx, completed_event, timestamp)
        .await
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE turn SET status = 'completed' WHERE id = $1",
        [turn_id.into()],
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn history_check_retries_are_durable_bounded_and_cas_protected() {
    let store = store().await.with_maintenance_access();
    start_history_check_root(&store, "turn").await;
    store
        .compaction_enqueue_native_history_check("ws", "thread", "turn", "{}")
        .await
        .unwrap();
    let db = store.database_connection();
    let mut duplicate = db.work_signal("compaction").subscribe();
    duplicate.begin_pass(&db).await.unwrap();
    store
        .compaction_enqueue_native_history_check("ws", "thread", "turn", "{}")
        .await
        .unwrap();
    assert!(
        futures_util::FutureExt::now_or_never(duplicate.wait(None)).is_none(),
        "an unchanged pending descriptor must not wake discovery"
    );
    publish_history_check_root(&store, "turn").await;
    let mut now = 1000;
    // Finish the real lifecycle bootstrap/scope obligations first. They have
    // independent deadlines even while this history root is owned.
    for _ in 0..16 {
        let seed = store
            .compaction_expand_lifecycle_scope(true, &|| 1000)
            .await
            .unwrap();
        let scopes = store
            .compaction_expand_lifecycle_scope(false, &|| 1000)
            .await
            .unwrap();
        if seed + scopes == 0 {
            break;
        }
    }
    let owned_root = [("ws".to_owned(), "thread".to_owned())];
    assert!(
        store
            .compaction_due_history_checks_excluding(now, &owned_root)
            .await
            .unwrap()
            .is_empty(),
        "an older owned check blocks this whole conversation"
    );
    assert!(
        store
            .next_compaction_background_deadline(&owned_root)
            .await
            .unwrap()
            .is_none(),
        "completion notification owns capacity release; no due SQL spin while it is held"
    );
    assert_eq!(
        store
            .compaction_due_history_checks_excluding(
                now,
                &[("other-workspace".to_owned(), "thread".to_owned())]
            )
            .await
            .unwrap()
            .len(),
        1
    );
    for (failure, delay) in [60_000, 300_000, 900_000, 0].into_iter().enumerate() {
        let page = store.compaction_due_history_checks(now).await.unwrap();
        assert_eq!(page.len(), 1);
        let claim = store
            .compaction_claim_history_check("turn", page[0].revision, now)
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .compaction_claim_history_check("turn", page[0].revision, now)
                .await
                .unwrap()
                .is_none()
        );
        let deadline = store
            .compaction_begin_history_attempt("turn", claim.revision, "{}", "hash", now)
            .await
            .unwrap()
            .unwrap();
        // A new worker after shutdown inherits the exact attempt deadline.
        let restarted = store
            .compaction_claim_history_check("turn", claim.revision, now + 10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .compaction_begin_history_attempt(
                    "turn",
                    restarted.revision,
                    "{}",
                    "hash",
                    now + 10
                )
                .await
                .unwrap(),
            Some(deadline)
        );
        let mut d = D::new(
            "history_capture",
            "database_error",
            "Temporary database failure",
        );
        d.observed_ms = now as u64;
        assert!(
            !store
                .compaction_record_history_result(
                    "turn",
                    claim.revision,
                    failure as i64,
                    O::Retryable,
                    &d,
                    now
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .compaction_record_history_result(
                    "turn",
                    restarted.revision,
                    failure as i64,
                    O::Retryable,
                    &d,
                    now
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .compaction_record_history_result(
                    "turn",
                    restarted.revision,
                    failure as i64,
                    O::Retryable,
                    &d,
                    now
                )
                .await
                .unwrap()
        );
        let saved = check::Entity::find_by_id("turn")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.failures, failure as i64 + 1);
        assert_eq!(
            serde_json::from_value::<D>(saved.diagnostic.unwrap()).unwrap(),
            d
        );
        assert!(saved.attempt_deadline_ms.is_none());
        if delay == 0 {
            assert_eq!(saved.state, "finished");
            assert_eq!(saved.outcome.as_deref(), Some("failed"));
            assert!(
                store
                    .compaction_due_history_checks(i64::MAX)
                    .await
                    .unwrap()
                    .is_empty()
            );
        } else {
            assert_eq!(saved.next_attempt_ms, now + delay);
            assert!(
                store
                    .compaction_due_history_checks(now + delay - 1)
                    .await
                    .unwrap()
                    .is_empty()
            );
            now += delay;
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn history_check_waits_do_not_spend_retries_and_stop_wins_over_stale_results() {
    let store = store().await.with_maintenance_access();
    start_history_check_root(&store, "turn").await;
    store
        .compaction_enqueue_native_history_check("ws", "thread", "turn", "{}")
        .await
        .unwrap();
    publish_history_check_root(&store, "turn").await;
    let mut now = 0;
    for outcome in [
        O::Preparing,
        O::WaitingCatalog,
        O::WaitingExecutor,
        O::WaitingSettings,
    ] {
        let row = store
            .compaction_due_history_checks(now)
            .await
            .unwrap()
            .remove(0);
        let claim = store
            .compaction_claim_history_check("turn", row.revision, now)
            .await
            .unwrap()
            .unwrap();
        store
            .compaction_begin_history_attempt("turn", claim.revision, "{}", "original", now)
            .await
            .unwrap();
        let d = D::new("preparation", outcome.as_str(), "Waiting for readiness");
        store
            .compaction_record_history_result("turn", claim.revision, 0, outcome, &d, now)
            .await
            .unwrap();
        let row = check::Entity::find_by_id("turn")
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.failures, 0);
        assert_eq!(row.config_hash.as_deref(), Some("original"));
        assert!(
            store
                .compaction_due_history_checks(now + 59_999)
                .await
                .unwrap()
                .is_empty()
        );
        now += 60_000;
    }
    let row = store
        .compaction_due_history_checks(now)
        .await
        .unwrap()
        .remove(0);
    let claim = store
        .compaction_claim_history_check("turn", row.revision, now)
        .await
        .unwrap()
        .unwrap();
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    assert!(
        !store
            .compaction_record_history_result(
                "turn",
                claim.revision,
                0,
                O::Retryable,
                &D::default(),
                now
            )
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_due_history_checks(i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    let row = check::Entity::find_by_id("turn")
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.outcome.as_deref(), Some("cancelled"));
}

async fn scalar(store: &CompactionStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(
            store.database_connection().get_database_backend(),
            sql,
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn postgres_store(database: &TestDatabase) -> CompactionStore {
    let writer = database.connection();
    writer
        .execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws','Compaction fixtures');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,access_class)
         VALUES ('thread','ws','','agent','model','provider','active','workspace'),
                ('other','ws','','agent','model','provider','active','workspace');
         INSERT INTO turn(id,thread_id,status) VALUES ('turn','thread','completed'),
                ('other-turn','other','completed');",
        )
        .await
        .unwrap();
    let reader = database.connect_read_only().await.unwrap();
    CompactionStore::for_test(SqliteDatabase::new(reader, writer.clone())).with_maintenance_access()
}

#[tokio::test]
async fn history_check_retry_survives_reopening_the_pool_and_a_cancelled_claim() {
    with_database(async |fixture| {
        use std::time::Duration;
        let store = postgres_store(fixture).await;
        start_history_check_root(&store, "root-turn").await;
        publish_history_check_root(&store, "root-turn").await;
        let page = store.compaction_due_history_checks(1000).await.unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].turn_id, "root-turn");
        let revision = page[0].revision;
        let writer = fixture.connect().await.unwrap();
        let held = writer.begin().await.unwrap();
        let holder_pid: i32 = held
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT pg_backend_pid()",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index(0)
            .unwrap();
        held.query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT turn_id FROM compaction_history_check WHERE turn_id='root-turn' FOR UPDATE",
        ))
        .await
        .unwrap()
        .unwrap();
        let claim_store = store.clone();
        let contender = tokio::spawn(async move {
            claim_store
                .compaction_claim_history_check("root-turn", revision, 1000)
                .await
        });
        let reader = fixture.connect_read_only().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting: bool = reader.query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
                [holder_pid.into()],
            )).await.unwrap().unwrap().try_get_by_index(0).unwrap();
                if waiting {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the competing claim must reach the PostgreSQL row lock");
        assert_eq!(scalar(&store, "SELECT count(*) AS n FROM turn").await, 3);
        contender.abort();
        assert!(contender.await.unwrap_err().is_cancelled());
        held.rollback().await.unwrap();
        assert_eq!(
            scalar(
                &store,
                "SELECT revision AS n FROM compaction_history_check WHERE turn_id='root-turn'"
            )
            .await,
            revision
        );
        let claim = store
            .compaction_claim_history_check("root-turn", revision, 1000)
            .await
            .unwrap()
            .unwrap();
        store
            .compaction_begin_history_attempt(
                "root-turn",
                claim.revision,
                "{}",
                "safe-digest",
                1000,
            )
            .await
            .unwrap();
        store
            .compaction_record_history_result(
                "root-turn",
                claim.revision,
                0,
                O::Retryable,
                &D::new("history_capture", "database_error", "Temporary failure"),
                1000,
            )
            .await
            .unwrap();
        drop(store);
        let reopened = CompactionStore::for_test(SqliteDatabase::new(
            fixture.connect_read_only().await.unwrap(),
            fixture.connect().await.unwrap(),
        ))
        .with_maintenance_access();
        assert!(
            reopened
                .compaction_due_history_checks(60999)
                .await
                .unwrap()
                .is_empty()
        );
        let page = reopened.compaction_due_history_checks(61000).await.unwrap();
        assert_eq!(page[0].failures, 1);
        assert_eq!(page[0].config_hash.as_deref(), Some("safe-digest"));
        let row = pioneer_entity::compaction_history_check::Entity::find_by_id("root-turn")
            .one(&reopened.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert!(
            row.diagnostic
                .unwrap()
                .to_string()
                .contains("database_error")
        );
        Ok(())
    })
    .await
    .unwrap();
}
