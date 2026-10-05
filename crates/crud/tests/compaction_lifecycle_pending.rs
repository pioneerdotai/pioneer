//! Proposal 72/04 regression code. These scenarios are intentionally deferred
//! until implementation review; cargo check --tests compiles but never runs them.
use migration::{Migrator, MigratorTrait};
use pioneer_compaction::{
    ModelBudget,
    runner::{FailureKind, RunnerPhase, RunnerState},
};
use pioneer_crud::{CanonicalTurnEventPayload as Event, CrudStore, compaction::*};
use pioneer_entity::{
    compaction_lifecycle_pending as pending, compaction_lifecycle_scope as scope,
    compaction_lifecycle_sequence as sequence, compaction_runner_state as runner,
};
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DbBackend, EntityTrait, Statement, TransactionTrait,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
const TRACKING: &str = "m20261004_000004_compaction_lifecycle_pending";
const NOW: i64 = 2_000_000_000_000;

struct TestFile(PathBuf);
impl Drop for TestFile {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}
async fn open(path: &Path) -> (CrudStore, SqliteWriteExecutor) {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    let executor = SqliteWriteExecutor::new(writer.clone());
    SqliteDatabase::from_executor(writer, executor.clone())
        .maintenance()
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options
        .max_connections(2)
        .min_connections(1)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let db = SqliteDatabase::from_executor(reader, executor.clone());
    assert!(db.reader_query_only_enabled().await.unwrap());
    (CrudStore::new(db), executor)
}
async fn fixture(before_tracking: bool) -> (TestFile, CrudStore, SqliteWriteExecutor) {
    let file = TestFile(
        std::env::temp_dir().join(format!("pioneer-lifecycle-{}.sqlite", uuid::Uuid::new_v4())),
    );
    let (store, writer) = open(&file.0).await;
    let count = before_tracking.then(|| {
        Migrator::migrations()
            .iter()
            .position(|m| m.name() == TRACKING)
            .unwrap() as u32
    });
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, count)
        .await
        .unwrap();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES ('owner','ws','thread',1)",
    ] {
        store
            .with_maintenance_access()
            .database_connection()
            .execute_unprepared(sql)
            .await
            .unwrap();
    }
    (file, store, writer)
}
async fn operation(store: &CrudStore, id: &str, status: &str, deadline: i64) {
    store.with_maintenance_access().database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,execution_turn,deadline_ms) VALUES(?,'owner',?,?, '{}','turn',?)",
        [id.into(),id.into(),status.into(),deadline.into()])).await.unwrap();
}
async fn state(store: &CrudStore, id: &str) -> RunnerState {
    let state = RunnerState::new(
        NOW as u64,
        &ModelBudget::new(Some(128000), None, Some(16384)),
        1000,
        None,
    )
    .unwrap();
    store
        .with_maintenance_access()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES (?,?,?)",
            [
                id.into(),
                (state.generation as i64).into(),
                serde_json::to_string(&state).unwrap().into(),
            ],
        ))
        .await
        .unwrap();
    state
}
async fn row(store: &CrudStore, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id)
        .one(&store.with_maintenance_access().database_connection())
        .await
        .unwrap()
}
async fn scalar(store: &CrudStore, sql: &str) -> i64 {
    store
        .with_maintenance_access()
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}
async fn claim(store: &CrudStore, id: &str, now: i64) -> CompactionLifecycleClaim {
    let candidate = row(store, id).await.unwrap();
    store
        .compaction_claim_lifecycle(&candidate, &|| now)
        .await
        .unwrap()
        .unwrap()
}
fn lifecycle_event(prepared: &PreparedCompactionLifecycle) -> Option<Event> {
    if !prepared.needs_publication() {
        return None;
    }
    let state = prepared.state().unwrap();
    let status = if matches!(state.phase, RunnerPhase::Applied { .. }) {
        "completed"
    } else if matches!(
        state.phase,
        RunnerPhase::Failed {
            kind: FailureKind::Cancelled
        }
    ) {
        "cancelled"
    } else {
        "failed"
    };
    let (workspace, thread, turn) = prepared.scope().unwrap();
    Some(Event::ItemCompleted(
        pioneer_protocol::ItemCompletedNotification {
            workspace_id: workspace.into(),
            thread_id: thread.into(),
            turn_id: turn.into(),
            item: pioneer_protocol::TurnItem::SystemEvent {
                id: format!("compaction:{}", prepared.operation_id()),
                code: Some("agent_context_compaction".into()),
                message: status.into(),
                level: pioneer_protocol::SystemEventLevel::Info,
                details: Some(serde_json::json!({"status":status})),
            },
        },
    ))
}
async fn apply(store: &CrudStore, id: &str, now: i64) -> bool {
    let claimed = claim(store, id, now).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, now)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    store
        .compaction_repair_lifecycle(prepared, event, now / 1000)
        .await
        .unwrap()
}
async fn expand_all(store: &CrudStore, now: i64) {
    for _ in 0..100 {
        let seed = store
            .compaction_expand_lifecycle_scope(true, &|| now)
            .await
            .unwrap();
        let scope = store
            .compaction_expand_lifecycle_scope(false, &|| now)
            .await
            .unwrap();
        assert!(seed + scope <= 8);
        if seed + scope == 0 {
            return;
        }
    }
    panic!("bounded fixture scopes must drain");
}
async fn enable(store: &CrudStore, column: &str) {
    store.with_maintenance_access().database_connection().query_one_write_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT zstd_enable_transparent(?)",[serde_json::json!({"table":"turn_item","column":column,"compression_level":3,"dict_chooser":"'[nodict]'"}).to_string().into()])).await.unwrap();
}

#[tokio::test]
async fn lifecycle_deadline_units_stop_future_deadline_and_missing_runner() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "deadline", "running", NOW + 60_000).await;
    expand_all(&store, NOW).await;
    assert!(apply(&store, "deadline", NOW).await); // scope evaluation restores deadline
    let future = row(&store, "deadline").await.unwrap();
    assert_eq!(future.due_at, NOW + 60_000);
    assert_eq!(future.retry_not_before, 0);
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_claim_lifecycle(&future, &|| NOW)
            .await
            .unwrap()
            .is_none()
    );
    assert!(apply(&store, "deadline", NOW + 60_000).await);
    assert_eq!(scalar(&store,"SELECT count(*) n FROM compaction_operation WHERE id='deadline' AND status='failed' AND outcome='deadline'").await,1);
    assert!(
        row(&store, "deadline").await.is_some(),
        "finish keeps terminal debt"
    );
    assert!(apply(&store, "deadline", NOW + 60_000).await);
    assert!(
        row(&store, "deadline").await.is_none(),
        "terminal without runner follows old predicate"
    );
    operation(&store, "stop", "running", NOW + 600_000).await;
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    expand_all(&store, NOW).await;
    assert!(apply(&store, "stop", NOW).await);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE id='stop' AND status='cancelled'"
        )
        .await,
        1
    );
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    assert!(apply(&store, "stop", NOW).await);
}

#[tokio::test]
async fn lifecycle_stop_after_finish_preserves_terminal_deadline_classification() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "running", NOW).await;
    state(&store, "op").await;
    assert!(apply(&store, "op", NOW).await); // finish before runner/publication
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    expand_all(&store, NOW).await;
    assert!(apply(&store, "op", NOW).await);
    assert!(row(&store, "op").await.is_none());
    assert_eq!(scalar(&store, "SELECT count(*) n FROM compaction_operation WHERE status='failed' AND outcome='deadline'").await, 1);
    assert!(matches!(
        store
            .compaction_runner_state("op")
            .await
            .unwrap()
            .unwrap()
            .phase,
        RunnerPhase::Failed {
            kind: FailureKind::Deadline
        }
    ));
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_item WHERE item_id='compaction:op' AND json_extract(payload,'$.details.status')='failed'").await, 1);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn WHERE status='completed'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn lifecycle_source_refresh_preserves_real_error_backoff_not_old_deadline() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "running", NOW + 600_000).await;
    expand_all(&store, NOW).await;
    let claimed = claim(&store, "op", NOW).await; // pretend preparation was interrupted
    let reserved = row(&store, "op").await.unwrap();
    assert_eq!(reserved.retry_not_before, NOW + 5_000);
    assert!(reserved.claim_token.is_some());
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    expand_all(&store, NOW).await;
    let refreshed = row(&store, "op").await.unwrap();
    assert!(refreshed.generation > reserved.generation);
    assert_eq!(refreshed.eligible_at, 0);
    assert_eq!(refreshed.due_at, NOW + 5_000);
    assert_eq!(refreshed.attempts, 1);
    assert!(refreshed.claim_token.is_none());
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(apply(&store, "op", NOW + 5_000).await);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='cancelled'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn lifecycle_finish_poison_runner_and_restart_publication_pipeline() {
    let (file, store, _) = fixture(false).await;
    operation(&store, "poison", "running", NOW).await;
    let initial = state(&store, "poison").await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared(
        "UPDATE compaction_runner_state SET state='broken' WHERE operation_id='poison'",
    )
    .await
    .unwrap();
    assert!(
        apply(&store, "poison", NOW).await,
        "poison runner cannot prevent timeout"
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='failed'"
        )
        .await,
        1
    );
    let claimed = claim(&store, "poison", NOW).await;
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW)
            .await
            .is_err()
    );
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_runner_state SET state=? WHERE operation_id='poison'",
        [serde_json::to_string(&initial).unwrap().into()],
    ))
    .await
    .unwrap();
    assert_eq!(row(&store, "poison").await.unwrap().due_at, NOW + 5_000);
    drop(db);
    drop(store);
    let (store, _) = open(&file.0).await;
    assert!(apply(&store, "poison", NOW + 5_000).await);
    assert!(row(&store, "poison").await.is_none());
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn_item WHERE item_id='compaction:poison'"
        )
        .await,
        1
    );
    assert!(matches!(
        store
            .compaction_runner_state("poison")
            .await
            .unwrap()
            .unwrap()
            .phase,
        RunnerPhase::Failed {
            kind: FailureKind::Deadline
        }
    ));
}

#[tokio::test]
async fn lifecycle_fresh_values_fence_repeated_generation_and_concurrent_apply() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "failed", NOW).await;
    let state = state(&store, "op").await;
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    let mut changed = state.clone();
    changed.attempts += 1;
    store
        .with_maintenance_access()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_runner_state SET state=? WHERE operation_id='op'",
            [serde_json::to_string(&changed).unwrap().into()],
        ))
        .await
        .unwrap();
    assert!(
        !store
            .compaction_repair_lifecycle(prepared, event, NOW / 1000)
            .await
            .unwrap()
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 0);
    assert!(row(&store, "op").await.unwrap().claim_token.is_none());
    // A concurrent successful domain apply changes operation/runner atomically.
    // This test exercises stale repair; real successful-apply coverage lives in
    // compaction.rs with its full manifest/checkpoint fixture.
    let claimed = claim(&store, "op", NOW + 5_000).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW + 5_000)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    store
        .with_maintenance_access()
        .database_connection()
        .execute_unprepared(
            "UPDATE compaction_operation SET status='completed',outcome='applied' WHERE id='op'",
        )
        .await
        .unwrap();
    assert!(
        !store
            .compaction_repair_lifecycle(prepared, event, NOW / 1000)
            .await
            .unwrap()
    );
    assert!(row(&store, "op").await.is_some());
}

#[tokio::test]
async fn lifecycle_heartbeat_during_preparation_does_not_fence_equal_inputs() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "failed", NOW).await;
    state(&store, "op").await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,active_attempt_number) VALUES('item','turn','compaction:op','system_event','in_progress','{\"details\":{\"status\":\"started\"}}',0)").await.unwrap();
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    let before = row(&store, "op").await.unwrap();
    for sql in [
        "UPDATE turn SET updated_at=datetime('now','+1 second') WHERE id='turn'",
        "UPDATE thread SET preview='heartbeat',updated_at=datetime('now','+1 second') WHERE id='thread'",
        "UPDATE turn_item SET last_heartbeat_at=CURRENT_TIMESTAMP,updated_at=datetime('now','+1 second') WHERE id='item'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_eq!(row(&store, "op").await.unwrap(), before);
    assert!(
        store
            .compaction_repair_lifecycle(prepared, event, NOW / 1000)
            .await
            .unwrap()
    );
    assert!(row(&store, "op").await.is_none());
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_item WHERE item_id='compaction:op' AND json_extract(payload,'$.details.status')='failed'").await, 1);
}

#[tokio::test]
async fn lifecycle_claim_token_fences_retry_with_unchanged_source_generation() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "failed", NOW).await;
    state(&store, "op").await;
    let candidate = row(&store, "op").await.unwrap();
    let clock = || NOW;
    let (first, second) = tokio::join!(
        store.compaction_claim_lifecycle(&candidate, &clock),
        store.compaction_claim_lifecycle(&candidate, &clock),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_ne!(first.is_some(), second.is_some());
    let first = first.or(second).unwrap();
    let prepared = store
        .compaction_prepare_lifecycle(&first, NOW)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    let before = row(&store, "op").await.unwrap();
    let _next = claim(&store, "op", NOW + 5_000).await;
    let next = row(&store, "op").await.unwrap();
    assert_eq!(next.generation, before.generation);
    assert_ne!(next.claim_token, before.claim_token);
    assert_eq!(next.attempts, 2);
    assert_eq!(next.retry_not_before, NOW + 15_000);
    assert!(
        !store
            .compaction_repair_lifecycle(prepared, event, NOW / 1000)
            .await
            .unwrap()
    );
    assert_eq!(row(&store, "op").await.unwrap(), next);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 0);
}

#[tokio::test]
async fn lifecycle_publication_projection_ack_and_post_trigger_generation_are_atomic() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "failed", NOW).await;
    let initial = state(&store, "op").await;
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    let before = row(&store, "op").await.unwrap();
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("CREATE TRIGGER reject_lifecycle_projection BEFORE INSERT ON turn_item WHEN NEW.item_id='compaction:op' BEGIN SELECT RAISE(ABORT,'projection rollback'); END").await.unwrap();
    assert!(
        store
            .compaction_repair_lifecycle(prepared, event, NOW / 1000)
            .await
            .is_err()
    );
    assert_eq!(row(&store, "op").await.unwrap(), before);
    assert_eq!(
        runner::Entity::find_by_id("op")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .generation,
        initial.generation as i64
    );
    for table in [
        "turn_event",
        "turn_event_projection_state",
        "turn_event_delivery",
    ] {
        assert_eq!(
            scalar(&store, &format!("SELECT count(*) n FROM {table}")).await,
            0
        );
    }
    db.execute_unprepared("DROP TRIGGER reject_lifecycle_projection")
        .await
        .unwrap();
    assert!(apply(&store, "op", NOW + 5_000).await);
    assert!(
        row(&store, "op").await.is_none(),
        "own runner/item triggers must not strand post-repair debt"
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn_event_projection_state WHERE status='projected'"
        )
        .await,
        1
    );
    // Retried old handler must not wipe any later generation.
    db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
        .await
        .unwrap();
    let new = row(&store, "op").await.unwrap();
    assert!(new.generation > before.generation);
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW + 5_000)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(row(&store, "op").await.unwrap(), new);
}

#[tokio::test]
async fn lifecycle_event_driven_publication_overtakes_worker_with_fresh_atomic_ack() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "failed", NOW).await;
    state(&store, "op").await;
    store.compaction_reconcile_runner_state("op").await.unwrap();
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared).unwrap();
    let generation = prepared.state().unwrap().generation;
    store
        .compaction_materialize_lifecycle("op", generation, event.clone(), NOW / 1000)
        .await
        .unwrap();
    assert!(row(&store, "op").await.is_none());
    assert!(
        !store
            .compaction_repair_lifecycle(prepared, Some(event), NOW / 1000)
            .await
            .unwrap()
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
}

#[tokio::test]
async fn lifecycle_physical_plain_compressed_delete_replay_and_storage_switch() {
    for compressed_before_tracking in [false, true] {
        let (_file, store, writer) = fixture(compressed_before_tracking).await;
        operation(&store, "op", "failed", NOW).await;
        state(&store, "op").await;
        let db = store.with_maintenance_access().database_connection();
        // A physical metadata-only item can be installed before cutover.
        db.execute_unprepared("INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,active_attempt_number,created_at,updated_at) VALUES ('item','turn','compaction:op','system_event','in_progress','{\"details\":{\"status\":\"started\"}}',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        if compressed_before_tracking {
            enable(&store, "payload").await;
            writer
                .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
                .await
                .unwrap();
            // Old terminal history is trusted; post-install physical UPDATE
            // supplies the current obligation without a historical scan.
            db.execute_unprepared("UPDATE _turn_item_zstd SET payload=payload WHERE id='item'")
                .await
                .unwrap();
        }
        assert!(apply(&store, "op", NOW).await);
        assert!(row(&store, "op").await.is_none());
        if !compressed_before_tracking {
            enable(&store, "payload").await;
        }
        assert_eq!(scalar(&store,"SELECT count(*) n FROM sqlite_master WHERE type='trigger' AND name LIKE 'compaction_lifecycle_item_%' AND tbl_name='_turn_item_zstd'").await,4);
        // View replacement (enabling another column) must preserve all
        // physical triggers before the next write.
        enable(&store, "active_attempt_status").await;
        assert_eq!(scalar(&store,"SELECT count(*) n FROM sqlite_master WHERE type='trigger' AND name LIKE 'compaction_lifecycle_item_%' AND tbl_name='_turn_item_zstd'").await,4);
        let event_count = scalar(&store, "SELECT count(*) n FROM turn_event").await;
        let mut compressor = pioneer_sqlite::zstd::ColumnValueCompressor::new(3, None).unwrap();
        let blob = compressor
            .compress(b"{\"details\":{\"status\":\"started\"}}")
            .unwrap();
        // No trigger may attempt json_extract on this compressed BLOB.
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE _turn_item_zstd SET payload=?,_payload_dict=-1 WHERE item_id='compaction:op'",
            [blob.into()],
        ))
        .await
        .unwrap();
        assert!(row(&store, "op").await.is_some());
        assert!(apply(&store, "op", NOW).await);
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event").await,
            event_count,
            "replay repairs projections without duplicate append"
        );
        assert_eq!(scalar(&store,"SELECT count(*) n FROM turn_item WHERE json_extract(payload,'$.details.status')='failed'").await,1);
        db.execute_unprepared("DELETE FROM _turn_item_zstd WHERE item_id='compaction:op'")
            .await
            .unwrap();
        assert!(row(&store, "op").await.is_some());
        assert!(apply(&store, "op", NOW).await);
        assert_eq!(
            scalar(
                &store,
                "SELECT count(*) n FROM turn_item WHERE item_id='compaction:op'"
            )
            .await,
            1
        );
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event").await,
            event_count
        );
        let before = scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await;
        assert!(row(&store, "op").await.is_none());
        // Replay/direct view DML reaches the same physical tracker.
        db.execute_unprepared("UPDATE turn_item SET payload='{\"details\":{\"status\":\"started\"}}' WHERE item_id='compaction:op'").await.unwrap();
        assert!(apply(&store, "op", NOW).await);
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
            before
        );
    }
}

#[tokio::test]
async fn lifecycle_late_runner_context_turn_and_source_deletion() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "failed", NOW).await;
    assert!(apply(&store, "op", NOW).await);
    assert!(row(&store, "op").await.is_none());
    state(&store, "op").await;
    assert!(
        row(&store, "op").await.is_some(),
        "late runner is exact INSERT coverage"
    );
    let db = store.with_maintenance_access().database_connection();
    // Match the Gateway's actual runtime foreign_keys=OFF policy: missing
    // source rows must not be fabricated by recovery.
    db.execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM compaction_context WHERE owner='owner'")
        .await
        .unwrap();
    assert!(apply(&store, "op", NOW).await);
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM compaction_context").await,
        0
    );
    db.execute_unprepared("INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1)").await.unwrap();
    expand_all(&store, NOW).await;
    db.execute_unprepared("DELETE FROM turn WHERE id='turn'")
        .await
        .unwrap();
    assert!(apply(&store, "op", NOW).await);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn").await, 0);
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    expand_all(&store, NOW).await;
    assert!(apply(&store, "op", NOW).await);
    assert!(row(&store, "op").await.is_none());
    db.execute_unprepared("DELETE FROM compaction_runner_state WHERE operation_id='op'")
        .await
        .unwrap();
    assert!(row(&store, "op").await.is_some());
    assert!(apply(&store, "op", NOW).await);
    db.execute_unprepared("DELETE FROM compaction_operation WHERE id='op'")
        .await
        .unwrap();
    assert!(row(&store, "op").await.is_none());
    operation(&store, "op", "failed", NOW).await;
    let new = row(&store, "op").await.unwrap();
    assert!(
        new.generation > 1,
        "delete/reinsert cannot ABA the sequence"
    );
}

#[tokio::test]
async fn lifecycle_running_missing_binding_retries_without_creating_sources() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "running", NOW).await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("DELETE FROM turn WHERE id='turn'")
        .await
        .unwrap();
    let claimed = claim(&store, "op", NOW).await;
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW)
            .await
            .is_err()
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn").await, 0);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='running'"
        )
        .await,
        1
    );
    assert_eq!(row(&store, "op").await.unwrap().due_at, NOW + 5_000);
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    expand_all(&store, NOW).await;
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(apply(&store, "op", NOW + 5_000).await);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='failed'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn lifecycle_scope_and_seed_pages_restart_safe_without_terminal_audit() {
    let (file, store, writer) = fixture(true).await;
    for i in 0..19 {
        operation(&store, &format!("active-{i:02}"), "running", NOW + 60_000).await;
    }
    for i in 0..19 {
        let id = format!("terminal-{i:02}");
        operation(&store, &id, "failed", NOW).await;
        state(&store, &id).await;
    }
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_pending"
        )
        .await,
        0,
        "cutover performs no source backfill"
    );
    assert_eq!(
        store
            .compaction_expand_lifecycle_scope(true, &|| NOW)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_pending"
        )
        .await,
        3
    );
    assert_eq!(scalar(&store,"SELECT count(*) n FROM compaction_lifecycle_pending WHERE operation_id LIKE 'terminal-%'").await,0);
    let seed = scope::Entity::find_by_id(("seed".to_owned(), String::new(), String::new()))
        .one(&store.with_maintenance_access().database_connection())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seed.cursor_id.as_deref(), Some("active-02"));
    assert_eq!(seed.upper_id.as_deref(), Some("active-18"));
    drop(store);
    drop(writer);
    let (store, _) = open(&file.0).await;
    expand_all(&store, NOW).await;
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_pending WHERE operation_id LIKE 'active-%'"
        )
        .await,
        19
    );
    assert_eq!(scalar(&store,"SELECT count(*) n FROM compaction_lifecycle_pending WHERE operation_id LIKE 'terminal-%'").await,0);
    assert_eq!(
        sequence::Entity::find_by_id(1)
            .one(&store.with_maintenance_access().database_connection())
            .await
            .unwrap()
            .unwrap()
            .seed_complete,
        1
    );
    assert_eq!(
        store
            .compaction_expand_lifecycle_scope(true, &|| NOW)
            .await
            .unwrap(),
        0
    );
    let mut seen = std::collections::HashSet::new();
    for _ in 0..8 {
        let batch = store.compaction_due_lifecycle(NOW).await.unwrap();
        assert!(batch.len() <= 8);
        if batch.is_empty() {
            break;
        }
        for row in batch {
            assert!(seen.insert(row.operation_id.clone()));
            assert!(apply(&store, &row.operation_id, NOW).await);
        }
    }
    assert_eq!(seen.len(), 19);
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    // One Turn change creates one coalesced scope; no foreground fanout.
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("UPDATE turn SET status='interrupted' WHERE id='turn'")
        .await
        .unwrap();
    let before = scalar(
        &store,
        "SELECT generation n FROM compaction_lifecycle_sequence",
    )
    .await;
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_scope WHERE kind='turn'"
        )
        .await,
        1
    );
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .compaction_expand_lifecycle_scope(false, &|| NOW)
            .await
            .unwrap(),
        4
    );
    let old = scope::Entity::find_by_id(("turn".to_owned(), String::new(), "turn".to_owned()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    db.execute_unprepared(
        "UPDATE turn SET status='cancelled',updated_at=updated_at WHERE id='turn'",
    )
    .await
    .unwrap();
    let fresh = scope::Entity::find_by_id(("turn".to_owned(), String::new(), "turn".to_owned()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(fresh.generation > old.generation && fresh.generation > before);
    assert!(fresh.cursor_id.is_none() && fresh.upper_id.is_none());
    expand_all(&store, NOW).await;
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_pending WHERE eligible_at=0"
        )
        .await,
        38
    );
}

#[tokio::test]
async fn lifecycle_seed_page_rollback_keeps_claim_and_restart_progress() {
    let (file, store, writer) = fixture(true).await;
    for id in ["a", "b", "c", "d"] {
        operation(&store, id, "running", NOW + 60_000).await;
    }
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("CREATE TRIGGER reject_seed_page BEFORE INSERT ON compaction_lifecycle_pending BEGIN SELECT RAISE(ABORT,'seed page rollback'); END").await.unwrap();
    assert!(
        store
            .compaction_expand_lifecycle_scope(true, &|| NOW)
            .await
            .is_err()
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_pending"
        )
        .await,
        0
    );
    let interrupted = scope::Entity::find_by_id(("seed".to_owned(), String::new(), String::new()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(interrupted.claim_token.is_some());
    assert_eq!(interrupted.attempts, 1);
    assert_eq!(interrupted.due_at, NOW + 5_000);
    assert!(interrupted.cursor_id.is_none() && interrupted.upper_id.is_none());
    assert_eq!(
        scalar(
            &store,
            "SELECT seed_complete n FROM compaction_lifecycle_sequence"
        )
        .await,
        0
    );
    db.execute_unprepared("DROP TRIGGER reject_seed_page")
        .await
        .unwrap();
    drop(db);
    drop(store);
    drop(writer);
    let (store, _) = open(&file.0).await;
    assert_eq!(
        store
            .compaction_expand_lifecycle_scope(true, &|| NOW)
            .await
            .unwrap(),
        0
    );
    expand_all(&store, NOW + 5_000).await;
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_pending"
        )
        .await,
        4
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT seed_complete n FROM compaction_lifecycle_sequence"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn lifecycle_stop_rebind_covers_old_and_new_scope_and_turn_binding_fences_commit() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "running", NOW + 60_000).await;
    expand_all(&store, NOW).await;
    assert!(apply(&store, "op", NOW).await);
    let db = store.with_maintenance_access().database_connection();
    // Direct physical DML must cover replay without Gateway hooks.
    db.execute_unprepared(
        "INSERT INTO compaction_execution_stop(owner,turn_id) VALUES('owner','turn')",
    )
    .await
    .unwrap();
    expand_all(&store, NOW).await;
    let claimed = claim(&store, "op", NOW).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW)
        .await
        .unwrap()
        .unwrap();
    db.execute_unprepared("INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('other-owner','ws','thread',1)").await.unwrap();
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin) VALUES('other-turn','thread','completed','conversation','user')").await.unwrap();
    db.execute_unprepared("UPDATE compaction_execution_stop SET owner='other-owner',turn_id='other-turn' WHERE owner='owner' AND turn_id='turn'").await.unwrap();
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_lifecycle_scope WHERE kind='owner_turn'"
        )
        .await,
        2
    );
    assert!(
        !store
            .compaction_repair_lifecycle(prepared, None, NOW / 1000)
            .await
            .unwrap()
    );
    expand_all(&store, NOW).await;
    assert_eq!(
        row(&store, "op").await.unwrap().retry_not_before,
        NOW + 5_000
    );
    assert!(apply(&store, "op", NOW + 5_000).await);
    assert_eq!(row(&store, "op").await.unwrap().due_at, NOW + 60_000);
    db.execute_unprepared("UPDATE turn SET status='interrupted' WHERE id='turn'")
        .await
        .unwrap();
    expand_all(&store, NOW + 5_000).await;
    let claimed = claim(&store, "op", NOW + 5_000).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW + 5_000)
        .await
        .unwrap()
        .unwrap();
    db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class) VALUES('rebound','ws','','agent','m','p','active','user','workspace')").await.unwrap();
    db.execute_unprepared("UPDATE turn SET thread_id='rebound' WHERE id='turn'")
        .await
        .unwrap();
    assert!(
        !store
            .compaction_repair_lifecycle(prepared, None, NOW / 1000)
            .await
            .unwrap()
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE status='running'"
        )
        .await,
        1
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 0);
    // Binding changes coalesce, and a missing matching context never gets
    // fabricated by recovery. Its next claim remains durable retry work.
    expand_all(&store, NOW + 10_000).await;
    let claimed = claim(&store, "op", NOW + 10_000).await;
    assert!(
        store
            .compaction_prepare_lifecycle(&claimed, NOW + 10_000)
            .await
            .is_err()
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_context WHERE thread_id='rebound'"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn lifecycle_claim_failure_exact_deferral_poison_progress_and_generation_exhaustion() {
    let (_file, store, _) = fixture(false).await;
    for id in ["a-poison", "b-good"] {
        operation(&store, id, "failed", NOW).await;
        state(&store, id).await;
    }
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("CREATE TRIGGER reject_claim BEFORE UPDATE OF claim_token ON compaction_lifecycle_pending WHEN OLD.operation_id='a-poison' AND NEW.claim_token IS NOT NULL BEGIN SELECT RAISE(ABORT,'poison reservation'); END").await.unwrap();
    let due = store.compaction_due_lifecycle(NOW).await.unwrap();
    assert_eq!(due.len(), 2);
    for row in due {
        if row.operation_id == "a-poison" {
            assert!(
                store
                    .compaction_claim_lifecycle(&row, &|| NOW)
                    .await
                    .is_err()
            );
        } else {
            assert!(apply(&store, &row.operation_id, NOW).await);
        }
    }
    let delayed = row(&store, "a-poison").await.unwrap();
    assert_eq!(delayed.due_at, NOW + 5_000);
    assert_eq!(delayed.attempts, 1);
    assert!(delayed.claim_token.is_none());
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    let stale = delayed.clone();
    db.execute_unprepared("UPDATE compaction_operation SET outcome='changed' WHERE id='a-poison'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_claim_lifecycle(&stale, &|| NOW + 5_000)
            .await
            .unwrap()
            .is_none()
    );
    let fresh = row(&store, "a-poison").await.unwrap();
    assert_eq!(
        fresh.attempts, 1,
        "stale deferral cannot mutate a new generation"
    );
    db.execute_unprepared("CREATE TRIGGER reject_deferral BEFORE UPDATE OF attempts ON compaction_lifecycle_pending WHEN OLD.operation_id='a-poison' BEGIN SELECT RAISE(ABORT,'bookkeeping unavailable'); END").await.unwrap();
    let error = store
        .compaction_claim_lifecycle(&fresh, &|| NOW + 5_000)
        .await
        .unwrap_err();
    assert!(
        error
            .downcast_ref::<CompactionLifecycleStorageError>()
            .is_some()
    );
    assert_eq!(row(&store, "a-poison").await.unwrap(), fresh);
    db.execute_unprepared("DROP TRIGGER reject_deferral")
        .await
        .unwrap();
    db.execute_unprepared("DROP TRIGGER reject_claim")
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_lifecycle_sequence SET generation=9223372036854775807",
    )
    .await
    .unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE compaction_operation SET outcome='overflow' WHERE id='a-poison'"
        )
        .await
        .is_err()
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM compaction_operation WHERE outcome='changed'"
        )
        .await,
        1
    );
    assert_eq!(row(&store, "a-poison").await.unwrap(), fresh);
}

#[derive(Default)]
struct Routes {
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadEvent>>,
    writes: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    queued: tokio::sync::Notify,
}
impl pioneer_sqlite::SqliteReadObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        self.reads.lock().unwrap().push(event);
    }
}
impl pioneer_sqlite::SqliteWriteObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        if matches!(
            event,
            pioneer_sqlite::SqliteWriteEvent::Enqueued {
                class: SqliteWriteClass::Maintenance,
                ..
            }
        ) {
            self.queued.notify_one();
        }
        self.writes.lock().unwrap().push(event);
    }
}
async fn observed_store(path: &Path, routes: Arc<Routes>) -> CrudStore {
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options
        .max_connections(2)
        .min_connections(1)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    CrudStore::new(SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, routes.clone()),
        routes,
    ))
}
#[tokio::test]
async fn lifecycle_maintenance_routes_clock_after_writer_and_cancellation_preserve_claim() {
    use pioneer_sqlite::{SqliteReadClass, SqliteReadEvent, SqliteWriteEvent};
    let (file, store, writer) = fixture(false).await;
    operation(&store, "op", "running", NOW + 60_000).await;
    expand_all(&store, NOW).await;
    drop(store);
    drop(writer);
    let routes = Arc::new(Routes::default());
    let store = observed_store(&file.0, routes.clone()).await;
    let candidate = store.compaction_due_lifecycle(NOW).await.unwrap().remove(0);
    assert!(
        routes.writes.lock().unwrap().is_empty(),
        "discovery uses physical reader"
    );
    assert!(routes.reads.lock().unwrap().iter().any(|e| matches!(
        e,
        SqliteReadEvent::OperationFinished {
            class: SqliteReadClass::Maintenance,
            ..
        }
    )));
    assert!(routes.reads.lock().unwrap().iter().all(|e| !matches!(
        e,
        SqliteReadEvent::OperationFinished {
            class: SqliteReadClass::Interactive,
            ..
        }
    )));
    let hold = store
        .with_maintenance_access()
        .database_connection()
        .begin()
        .await
        .unwrap();
    routes.queued.notified().await; // consume the holder's enqueue
    let time = Arc::new(std::sync::atomic::AtomicI64::new(NOW));
    let worker = store.clone();
    let c = candidate.clone();
    let t = time.clone();
    let waiting = tokio::spawn(async move {
        worker
            .compaction_claim_lifecycle(&c, &|| t.load(std::sync::atomic::Ordering::SeqCst))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    assert!(
        store.get_turn("thread", "turn").await.unwrap().is_some(),
        "interactive reader remains available while writer waits"
    );
    time.store(NOW + 30_000, std::sync::atomic::Ordering::SeqCst);
    hold.rollback().await.unwrap();
    let claimed = waiting.await.unwrap().unwrap().unwrap();
    assert_eq!(
        row(&store, "op").await.unwrap().retry_not_before,
        NOW + 35_000
    );
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW + 30_000)
        .await
        .unwrap()
        .unwrap();
    let hold = store
        .with_maintenance_access()
        .database_connection()
        .begin()
        .await
        .unwrap();
    // Consume any queued notifications left by earlier successful work.
    while tokio::time::timeout(Duration::from_millis(1), routes.queued.notified())
        .await
        .is_ok()
    {}
    let worker = store.clone();
    let waiting = tokio::spawn(async move {
        worker
            .compaction_repair_lifecycle(prepared, None, NOW / 1000)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    hold.rollback().await.unwrap();
    let after = row(&store, "op").await.unwrap();
    assert_eq!(after.retry_not_before, NOW + 35_000);
    assert!(after.claim_token.is_some());
    assert!(
        store
            .compaction_due_lifecycle(NOW + 30_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(routes.writes.lock().unwrap().iter().any(|e|matches!(e,SqliteWriteEvent::Cancelled{class:SqliteWriteClass::Maintenance,queue,..} if queue.maintenance==0)));
    assert!(routes.writes.lock().unwrap().iter().all(|e|!matches!(e,SqliteWriteEvent::Acquired{class,..} if *class!=SqliteWriteClass::Maintenance)));
    assert!(
        store
            .database_connection()
            .reader_query_only_enabled()
            .await
            .unwrap()
    );
    // Caller disappearance/panic after claim leaves the same durable retry.
    let worker = store.clone();
    let crash = tokio::spawn(async move {
        let _ = worker;
        panic!("owned candidate interrupted after claim")
    });
    assert!(crash.await.unwrap_err().is_panic());
    assert_eq!(row(&store, "op").await.unwrap(), after);
    assert!(apply(&store, "op", NOW + 35_000).await);
    assert_eq!(row(&store, "op").await.unwrap().due_at, NOW + 60_000);
}

#[tokio::test]
async fn lifecycle_retry_policy_saturates_and_unchanged_source_values_do_not_refresh() {
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "failed", NOW).await;
    state(&store, "op").await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared(
        "UPDATE compaction_runner_state SET state='broken' WHERE operation_id='op'",
    )
    .await
    .unwrap();
    let mut now = NOW;
    for (index, expected) in [
        5_000, 10_000, 20_000, 40_000, 80_000, 160_000, 300_000, 300_000, 300_000, 300_000,
        300_000, 300_000, 300_000, 300_000, 300_000, 300_000, 300_000,
    ]
    .into_iter()
    .enumerate()
    {
        let claimed = claim(&store, "op", now).await;
        assert!(
            store
                .compaction_prepare_lifecycle(&claimed, now)
                .await
                .is_err()
        );
        let before = row(&store, "op").await.unwrap();
        assert_eq!(before.attempts, (index as i64 + 1).min(16));
        assert_eq!(before.retry_not_before, now + expected);
        db.execute_unprepared("UPDATE compaction_runner_state SET generation=generation,state=state WHERE operation_id='op'").await.unwrap();
        db.execute_unprepared(
            "UPDATE compaction_operation SET status=status,deadline_ms=deadline_ms WHERE id='op'",
        )
        .await
        .unwrap();
        assert_eq!(row(&store, "op").await.unwrap(), before);
        now = before.due_at;
    }
}

#[tokio::test]
async fn lifecycle_idle_discovery_and_scope_indexes_have_no_historical_antijoin() {
    let (_file, store, _) = fixture(false).await;
    expand_all(&store, NOW).await;
    let db = store.with_maintenance_access().database_connection();
    for index in 0..64 {
        operation(&store, &format!("old-{index}"), "failed", NOW).await;
    }
    for index in 0..64 {
        assert!(apply(&store, &format!("old-{index}"), NOW).await);
    }
    assert!(
        store
            .compaction_due_lifecycle(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .compaction_expand_lifecycle_scope(true, &|| NOW)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .compaction_expand_lifecycle_scope(false, &|| NOW)
            .await
            .unwrap(),
        0
    );
    // Plans run only after acceptance. This code verifies exact index shapes
    // as well as the due prefix, rather than timing a small fixture.
    for (name, columns) in [
        (
            "idx_compaction_lifecycle_execution",
            vec!["execution_turn", "id"],
        ),
        (
            "idx_compaction_lifecycle_owner_execution",
            vec!["owner", "execution_turn", "id"],
        ),
        ("idx_compaction_lifecycle_owner", vec!["owner", "id"]),
        ("idx_compaction_lifecycle_running", vec!["status", "id"]),
        (
            "idx_compaction_lifecycle_due",
            vec!["due_at", "generation", "operation_id"],
        ),
    ] {
        let rows = db
            .query_all_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                [name.into()],
            ))
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|r| r.try_get::<String>("", "name").unwrap())
                .collect::<Vec<_>>(),
            columns
        );
    }
    let plan=db.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "EXPLAIN QUERY PLAN SELECT operation_id,generation FROM compaction_lifecycle_pending WHERE due_at<=? ORDER BY due_at,generation,operation_id LIMIT 8",[NOW.into()])).await.unwrap();
    let details = plan
        .iter()
        .map(|r| r.try_get::<String>("", "detail").unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        details.contains("SEARCH") && details.contains("idx_compaction_lifecycle_due"),
        "{details}"
    );
    assert!(
        !details.contains("SCAN compaction_operation") && !details.contains("TEMP B-TREE"),
        "{details}"
    );
    let triggers=db.query_all_raw(Statement::from_string(DbBackend::Sqlite,"SELECT sql FROM sqlite_master WHERE type='trigger' AND name LIKE 'compaction_lifecycle_%'".to_owned())).await.unwrap();
    for trigger in triggers {
        let sql = trigger.try_get::<String>("", "sql").unwrap();
        assert!(
            !sql.contains("json_extract") && !sql.contains(".payload"),
            "tracking uses metadata only"
        );
    }
}

#[tokio::test]
async fn lifecycle_repeated_started_keeps_one_attempt_and_completed_finishes_it() {
    use pioneer_entity::{turn_item, turn_item_attempt};
    use sea_orm::{ColumnTrait, QueryFilter};
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "running", NOW + 60_000).await;
    let running = state(&store, "op").await;
    let started = Event::ItemStarted(pioneer_protocol::ItemStartedNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item: pioneer_protocol::TurnItem::SystemEvent {
            id: "compaction:op".into(),
            code: Some("agent_context_compaction".into()),
            message: "started".into(),
            level: pioneer_protocol::SystemEventLevel::Info,
            details: Some(serde_json::json!({"status":"started"})),
        },
    });
    store
        .compaction_materialize_lifecycle("op", running.generation, started.clone(), NOW / 1000)
        .await
        .unwrap();
    let db = store.with_maintenance_access().database_connection();
    let first = turn_item::Entity::find()
        .filter(turn_item::Column::ItemId.eq("compaction:op"))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let deliveries = scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await;
    assert!(deliveries > 0);
    store
        .compaction_materialize_lifecycle("op", running.generation, started, NOW / 1000 + 100)
        .await
        .unwrap();
    let repeated = turn_item::Entity::find_by_id(&first.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeated, first);
    assert_eq!(repeated.active_attempt_number, 1);
    assert!(repeated.active_attempt_id.is_some());
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_item_attempt").await,
        1
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
        deliveries
    );
    store
        .compaction_finish("op", "failed", "deadline")
        .await
        .unwrap();
    assert!(apply(&store, "op", NOW + 100_000).await);
    let finished = turn_item_attempt::Entity::find_by_id(first.active_attempt_id.unwrap())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    // SystemEvent completion uses the existing completed attempt semantics.
    assert_eq!(finished.status, "completed");
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_item_attempt").await,
        1
    );
    let deliveries = scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await;
    db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
        .await
        .unwrap();
    assert!(apply(&store, "op", NOW + 200_000).await);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 2);
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
        deliveries
    );
    assert_eq!(
        scalar(&store, "SELECT count(*) n FROM turn_item_attempt").await,
        1
    );
}

#[tokio::test]
async fn lifecycle_terminal_repair_uses_saved_event_time_and_matches_canonical_replay() {
    use pioneer_entity::{turn_event, turn_item};
    use sea_orm::{ColumnTrait, QueryFilter};
    for compressed in [false, true] {
        let (_file, store, _) = fixture(false).await;
        if compressed {
            enable(&store, "payload").await;
        }
        operation(&store, "op", "failed", NOW).await;
        state(&store, "op").await;
        let t1 = NOW;
        let t2 = NOW + 600_000;
        assert!(apply(&store, "op", t1).await);
        let db = store.with_maintenance_access().database_connection();
        let saved = turn_event::Entity::find().one(&db).await.unwrap().unwrap();
        assert_eq!(saved.created_at.timestamp(), t1 / 1000);
        let original = turn_item::Entity::find()
            .filter(turn_item::Column::ItemId.eq("compaction:op"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.created_at, saved.created_at);
        assert_eq!(original.updated_at, saved.created_at);
        let deliveries = scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await;
        let event: Event = serde_json::from_str(&saved.payload).unwrap();
        let generation = store
            .compaction_runner_state("op")
            .await
            .unwrap()
            .unwrap()
            .generation;
        store
            .compaction_materialize_lifecycle("op", generation, event, t2 / 1000)
            .await
            .unwrap();
        assert_eq!(
            turn_item::Entity::find_by_id(&original.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        // Damage both the item data and projection time, then repair at T2.
        db.execute_unprepared("UPDATE turn_item SET payload='{\"details\":{\"status\":\"started\"}}',updated_at='2090-01-01 00:00:00+00:00' WHERE item_id='compaction:op'").await.unwrap();
        assert!(apply(&store, "op", t2).await);
        assert_eq!(
            turn_item::Entity::find_by_id(&original.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        let table = if compressed {
            "_turn_item_zstd"
        } else {
            "turn_item"
        };
        db.execute_unprepared(&format!(
            "DELETE FROM {table} WHERE item_id='compaction:op'"
        ))
        .await
        .unwrap();
        assert!(apply(&store, "op", t2 + 60_000).await);
        let restored = turn_item::Entity::find()
            .filter(turn_item::Column::ItemId.eq("compaction:op"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.payload, original.payload);
        assert_eq!(restored.status, original.status);
        assert_eq!(restored.created_at, saved.created_at);
        assert_eq!(restored.updated_at, saved.created_at);
        assert_eq!(
            turn_event::Entity::find_by_id(&saved.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            saved
        );
        assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
            deliveries
        );
        // Rebuild through the ordinary event replay path, which uses the saved
        // event's time. Compare the actual projection, excluding random row ID.
        db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
            .await
            .unwrap();
        db.execute_unprepared("UPDATE turn_event_projection_state SET status='pending',claim_token=NULL,claim_expires_at=NULL WHERE turn_id='turn'; UPDATE turn_event_projection_stream_state SET projected_through_sequence=0 WHERE turn_id='turn'").await.unwrap();
        let replay = store
            .replay_due_turn_event_projections(t2 / 1000 + 120, 1)
            .await
            .unwrap();
        assert_eq!(replay.claimed, 1);
        assert_eq!(replay.projected, 1);
        let canonical = turn_item::Entity::find()
            .filter(turn_item::Column::ItemId.eq("compaction:op"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(canonical.payload, restored.payload);
        assert_eq!(canonical.status, restored.status);
        assert_eq!(canonical.created_at, restored.created_at);
        assert_eq!(canonical.updated_at, restored.updated_at);
        assert_eq!(
            scalar(&store, "SELECT count(*) n FROM turn_event_delivery").await,
            deliveries
        );
    }
}

#[tokio::test]
async fn lifecycle_migration_partial_ddl_rollback_retry_and_down_up_trust_terminal_history() {
    let (_file, store, writer) = fixture(true).await;
    operation(&store, "old-terminal", "failed", NOW).await;
    state(&store, "old-terminal").await;
    operation(&store, "old-running", "running", NOW).await;
    let db = store.with_maintenance_access().database_connection();
    // Collision after tables, seed, indexes and the first physical trigger.
    let collision = "compaction_lifecycle_compaction_operation_delete";
    db.execute_unprepared(&format!(
        "CREATE TRIGGER {collision} AFTER DELETE ON compaction_operation WHEN 0 BEGIN SELECT 1; END"
    ))
    .await
    .unwrap();
    assert!(
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .is_err()
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM sqlite_master WHERE (name LIKE 'compaction_lifecycle_%' OR name LIKE 'idx_compaction_lifecycle_%') AND name <> 'compaction_lifecycle_compaction_operation_delete'").await, 0);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM seaql_migrations WHERE version='m20261004_000004_compaction_lifecycle_pending'").await, 0);
    db.execute_unprepared(&format!("DROP TRIGGER {collision}"))
        .await
        .unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    assert_eq!(scalar(&store, "SELECT count(*) n FROM seaql_migrations WHERE version='m20261004_000004_compaction_lifecycle_pending'").await, 1);
    expand_all(&store, NOW).await;
    assert!(row(&store, "old-terminal").await.is_none());
    assert!(row(&store, "old-running").await.is_some());
    let migrations = Migrator::migrations();
    let target = migrations
        .iter()
        .position(|m| m.name() == TRACKING)
        .unwrap();
    let suffix = u32::try_from(migrations.len() - target).unwrap();
    let tx = db.begin().await.unwrap();
    Migrator::down(&*tx, Some(suffix)).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(scalar(&store, "SELECT count(*) n FROM sqlite_master WHERE name LIKE 'compaction_lifecycle_%' OR name LIKE 'idx_compaction_lifecycle_%'").await, 0);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM seaql_migrations WHERE version='m20261004_000004_compaction_lifecycle_pending'").await, 0);
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    expand_all(&store, NOW).await;
    assert!(row(&store, "old-terminal").await.is_none());
    assert!(row(&store, "old-running").await.is_some());
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 0);
}

#[tokio::test]
async fn lifecycle_terminal_repair_fences_canonical_event_snapshot_and_rolls_back() {
    use pioneer_entity::turn_event;
    let (_file, store, _) = fixture(false).await;
    operation(&store, "op", "failed", NOW).await;
    let running = state(&store, "op").await;
    assert!(apply(&store, "op", NOW).await);
    let db = store.with_maintenance_access().database_connection();
    let saved = turn_event::Entity::find().one(&db).await.unwrap().unwrap();
    // Reconcile will update runner inside the domain transaction. Simulate an
    // event replacement after projection preparation but before publication.
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_runner_state SET generation=?,state=? WHERE operation_id='op'",
        [
            (running.generation as i64).into(),
            serde_json::to_string(&running).unwrap().into(),
        ],
    ))
    .await
    .unwrap();
    db.execute_unprepared("DELETE FROM turn_item WHERE item_id='compaction:op'")
        .await
        .unwrap();
    let claimed = claim(&store, "op", NOW + 600_000).await;
    let prepared = store
        .compaction_prepare_lifecycle(&claimed, NOW + 600_000)
        .await
        .unwrap()
        .unwrap();
    let event = lifecycle_event(&prepared);
    let before = row(&store, "op").await.unwrap();
    let runner_before = runner::Entity::find_by_id("op")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    db.execute_unprepared("CREATE TRIGGER change_event_during_repair AFTER UPDATE ON compaction_runner_state BEGIN UPDATE turn_event SET created_at='2090-01-01 00:00:00+00:00'; END").await.unwrap();
    assert!(
        store
            .compaction_repair_lifecycle(prepared, event, NOW / 1000 + 600)
            .await
            .is_err()
    );
    assert_eq!(
        turn_event::Entity::find_by_id(&saved.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        saved
    );
    assert_eq!(
        runner::Entity::find_by_id("op")
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        runner_before
    );
    assert_eq!(row(&store, "op").await.unwrap(), before);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_item").await, 0);
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 1);
    db.execute_unprepared("DROP TRIGGER change_event_during_repair")
        .await
        .unwrap();
    assert!(apply(&store, "op", before.retry_not_before).await);
}
