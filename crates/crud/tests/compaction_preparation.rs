use migration::{Migrator, MigratorTrait};
use pioneer_crud::CrudStore;
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

async fn scalar(store: &CrudStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn fixture(compressed: bool) -> CrudStore {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    fixture_on(db, compressed).await
}

async fn fixture_on(db: sea_orm::DatabaseConnection, compressed: bool) -> CrudStore {
    let before = Migrator::migrations()
        .iter()
        .position(|m| m.name().contains("20260910_000001"))
        .unwrap();
    Migrator::up(&db, Some(before as u32)).await.unwrap();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','legacy',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('other','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('other-turn','other','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "WITH RECURSIVE nums(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM nums WHERE n<299) INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) SELECT 'input-'||n,'turn',n,'text','legacy','{\"type\":\"text\",\"text\":\"legacy\"}',CURRENT_TIMESTAMP FROM nums",
        "WITH RECURSIVE nums(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM nums WHERE n<300) INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) SELECT 'event-'||n,'thread','turn',n,'fixture','{}',CURRENT_TIMESTAMP FROM nums",
        "WITH RECURSIVE nums(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM nums WHERE n<300) INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot,created_at) SELECT 'context-'||n,'turn',n,'tool_result_v2','{}','{}',CURRENT_TIMESTAMP FROM nums",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES('unrelated','other-turn',0,'text','untouched','{}',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    if compressed {
        db.query_one_raw(Statement::from_string(DbBackend::Sqlite,
            "SELECT zstd_enable_transparent('{\"table\":\"turn_event\",\"column\":\"payload\",\"compression_level\":3,\"dict_chooser\":\"''[nodict]''\"}')"))
            .await.unwrap();
    }
    Migrator::up(&db, None).await.unwrap();
    CrudStore::new(db).with_maintenance_access()
}

async fn finish(store: &CrudStore) {
    for _ in 0..100 {
        if store
            .compaction_prepare_history_quantum("ws", "thread")
            .await
            .unwrap()
        {
            return;
        }
    }
    panic!("bounded fixture did not finish preparation");
}

#[tokio::test]
async fn legacy_history_is_prepared_in_bounded_resumable_pages_before_a_new_fence() {
    let store = fixture(false).await;
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_input_revision"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_history_preparation"
        )
        .await,
        0
    );
    let stale_fence = store.compaction_history_read_fence().await.unwrap();
    assert!(
        store
            .compaction_history_turn_page("ws", "thread", "", &stale_fence)
            .await
            .is_err()
    );
    for _ in 0..3 {
        assert!(
            !store
                .compaction_prepare_history_quantum("ws", "thread")
                .await
                .unwrap()
        );
    }
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_input_revision"
        )
        .await,
        128
    );
    assert!(
        !store
            .compaction_history_prepared("ws", "thread")
            .await
            .unwrap()
    );
    // A new service handle resumes durable state, rather than a local loop offset.
    let resumed = CrudStore::new(store.database_connection()).with_maintenance_access();
    let mut last = 128;
    loop {
        let ready = resumed
            .compaction_prepare_history_quantum("ws", "thread")
            .await
            .unwrap();
        let count=scalar(&resumed,"SELECT (SELECT count(*) FROM compaction_input_revision)+(SELECT count(*) FROM compaction_event_revision)+(SELECT count(*) FROM compaction_source_revision) AS n").await;
        assert!((0..=128).contains(&(count - last)));
        last = count;
        if ready {
            break;
        }
    }
    assert_eq!(last, 900);
    assert!(
        resumed
            .compaction_history_turn_page("ws", "thread", "", &stale_fence)
            .await
            .is_err()
    );
    let fence = resumed.compaction_history_read_fence().await.unwrap();
    let turns = resumed
        .compaction_history_turn_page("ws", "thread", "", &fence)
        .await
        .unwrap();
    assert_eq!(turns.len(), 1);
    assert_eq!(
        (
            turns[0].input_high_water,
            turns[0].event_high_water,
            turns[0].context_high_water
        ),
        (300, 300, 300)
    );
    assert_eq!(
        scalar(
            &resumed,
            "SELECT count(*) AS n FROM compaction_input_revision WHERE source_id='unrelated'"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &resumed,
            "SELECT version AS n FROM compaction_projection_epoch WHERE thread_id='thread'"
        )
        .await,
        1
    );
    let step = scalar(
        &resumed,
        "SELECT step AS n FROM compaction_history_preparation WHERE thread_id='thread'",
    )
    .await;
    finish(&resumed).await;
    assert_eq!(
        scalar(
            &resumed,
            "SELECT step AS n FROM compaction_history_preparation WHERE thread_id='thread'"
        )
        .await,
        step
    );
}

#[tokio::test]
async fn preparation_preserves_concurrent_edits_deletes_and_compressed_sources() {
    let store = fixture(true).await;
    for _ in 0..3 {
        store
            .compaction_prepare_history_quantum("ws", "thread")
            .await
            .unwrap();
    }
    let db = store.database_connection();
    db.execute_unprepared("UPDATE turn_input SET payload='{\"edited\":true}' WHERE id='input-250'")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM turn_input WHERE id='input-251'")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,payload,created_at) VALUES('new','turn',300,'text','{}',CURRENT_TIMESTAMP)").await.unwrap();
    finish(&store).await;
    assert_eq!(
        scalar(
            &store,
            "SELECT revision AS n FROM compaction_input_revision WHERE source_id='input-250'"
        )
        .await,
        2
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT present AS n FROM compaction_input_revision WHERE source_id='input-251'"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_input_revision WHERE source_id='new'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_event_revision WHERE present=1"
        )
        .await,
        300
    );
    assert_eq!(scalar(&store,"SELECT count(*) AS n FROM turn_input WHERE id='input-250' AND payload='{\"edited\":true}'").await,1);
}

#[tokio::test]
async fn preparation_cursor_and_insertions_roll_back_together_and_workers_converge() {
    let store = fixture(false).await;
    for _ in 0..2 {
        store
            .compaction_prepare_history_quantum("ws", "thread")
            .await
            .unwrap();
    }
    let db = store.database_connection();
    db.execute_unprepared("CREATE TRIGGER reject_preparation BEFORE UPDATE OF step ON compaction_history_preparation BEGIN SELECT RAISE(ABORT,'injected failure'); END").await.unwrap();
    assert!(
        store
            .compaction_prepare_history_quantum("ws", "thread")
            .await
            .is_err()
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_input_revision"
        )
        .await,
        0
    );
    db.execute_unprepared("DROP TRIGGER reject_preparation")
        .await
        .unwrap();
    let ((), ()) = tokio::join!(finish(&store), finish(&store));
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_input_revision"
        )
        .await,
        300
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(DISTINCT capture_order) AS n FROM compaction_event_revision"
        )
        .await,
        300
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT version AS n FROM compaction_projection_epoch WHERE thread_id='thread'"
        )
        .await,
        1
    );
    assert!(
        store
            .compaction_prepare_history_quantum("wrong-workspace", "thread")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancelled_preparation_reopens_from_disk_with_a_physical_read_only_pool() {
    use pioneer_sqlite::{SqliteDatabase, sqlite_read_only_connection_url};
    use sea_orm::{ConnectOptions, TransactionTrait};
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let path = std::env::temp_dir().join(format!(
        "pioneer-legacy-preparation-{}.db",
        pioneer_protocol::generate_id(21)
    ));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let mut options = ConnectOptions::new(url.clone());
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    let setup = fixture_on(writer.clone(), false).await;
    let mut options = ConnectOptions::new(sqlite_read_only_connection_url(&path));
    options
        .max_connections(1)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
    let reader = Database::connect(options).await.unwrap();
    assert_eq!(
        reader
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA query_only"
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "query_only")
            .unwrap(),
        1
    );
    let database = SqliteDatabase::new(reader, writer);
    let store = CrudStore::new(database.clone()).with_maintenance_access();
    for _ in 0..3 {
        store
            .compaction_prepare_history_quantum("ws", "thread")
            .await
            .unwrap();
    }
    let step = scalar(
        &store,
        "SELECT step AS n FROM compaction_history_preparation WHERE thread_id='thread'",
    )
    .await;
    let output = pioneer_protocol::ItemDeltaNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item_id: "persisted-shell".into(),
        delta: "before cancellation".into(),
        stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
        payload: None,
        markdown: None,
        markdown_version: None,
    };
    store
        .record_tool_output("physical-output", &output)
        .await
        .unwrap();
    let reference: pioneer_compaction::frozen::FrozenMessageRef = serde_json::from_value(serde_json::json!({
        "logical_turn_id":"turn", "source_thread":"thread", "context_thread":null, "unit_id":"u",
        "sources":[{"scope":"event:turn","id":"event-1","version":"event-revision:1"}],
        "inherited":false,"complete":true,"protected_input":false,"wire_sha256":"a".repeat(64),
        "replay_source":null,"tool_call_id":null,"tool_name":null
    })).unwrap();
    for id in ["storage-a", "storage-b"] {
        let descriptor = pioneer_compaction::frozen::FrozenHistoryRef {
            format: 1,
            manifest_id: id.into(),
            messages: 1,
            identity_sha256: "b".repeat(64),
        };
        store
            .compaction_begin_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap();
        store
            .compaction_append_frozen_history(
                "ws",
                "thread",
                id,
                0,
                std::slice::from_ref(&reference),
            )
            .await
            .unwrap();
        assert!(
            store
                .compaction_finish_frozen_history("ws", "thread", &descriptor)
                .await
                .unwrap()
        );
    }
    let held = database.begin().await.unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            CrudStore::new(database.clone()).record_tool_output("cancelled-output", &output)
        )
        .await
        .is_err()
    );

    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            store.compaction_prepare_history_quantum("ws", "thread")
        )
        .await
        .is_err()
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            store.compact_frozen_storage_quantum()
        )
        .await
        .is_err()
    );
    held.rollback().await.unwrap();
    let rows = store
        .tool_output_page("ws", "thread", "turn", "persisted-shell", 0)
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "cancelled queued write must not persist later"
    );
    store
        .record_tool_output("after-cancel", &output)
        .await
        .unwrap();

    assert_eq!(
        scalar(
            &store,
            "SELECT step AS n FROM compaction_history_preparation WHERE thread_id='thread'"
        )
        .await,
        step
    );
    assert_eq!(
        scalar(&store, "SELECT count(*) AS n FROM compaction_frozen_layout").await,
        0
    );
    for _ in 0..4 {
        assert!(store.compact_frozen_storage_quantum().await.unwrap());
    }
    drop(store);
    drop(database);
    drop(setup);
    // Reopen the file: no in-memory cursor or worker survives this boundary.
    let reopened = CrudStore::new(Database::connect(url).await.unwrap()).with_maintenance_access();
    let rows = reopened
        .tool_output_page("ws", "thread", "turn", "persisted-shell", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|(_, row)| row.text == "before cancellation")
    );
    for _ in 0..30 {
        if !reopened.compact_frozen_storage_quantum().await.unwrap() {
            break;
        }
    }
    assert_eq!(
        scalar(
            &reopened,
            "SELECT count(*) AS n FROM compaction_frozen_message_data"
        )
        .await,
        1
    );
    for id in ["storage-a", "storage-b"] {
        assert_eq!(
            reopened
                .compaction_frozen_history_page("ws", "thread", id, 0)
                .await
                .unwrap(),
            vec![reference.clone()]
        );
    }
    finish(&reopened).await;
    assert_eq!(
        scalar(
            &reopened,
            "SELECT count(*) AS n FROM compaction_source_revision"
        )
        .await,
        300
    );
    drop(reopened);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn tool_output_chunks_are_scoped_ordered_and_idempotent() {
    let store = fixture(false).await;
    let mut n = pioneer_protocol::ItemDeltaNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item_id: "command".into(),
        delta: "same text\n".into(),
        stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
        payload: None,
        markdown: None,
        markdown_version: None,
    };
    store.record_tool_output("chunk-a", &n).await.unwrap();
    store.record_tool_output("chunk-a", &n).await.unwrap();
    store.record_tool_output("chunk-b", &n).await.unwrap();
    n.stream = Some(pioneer_protocol::ItemDeltaStream::Stderr);
    n.delta = "failure detail".into();
    store.record_tool_output("chunk-c", &n).await.unwrap();
    assert!(store.record_tool_output("chunk-a", &n).await.is_err());
    n.thread_id = "other".into();
    assert!(store.record_tool_output("bad", &n).await.is_err());
    let reopened = CrudStore::new(store.database_connection());
    let rows = reopened
        .tool_output_page("ws", "thread", "turn", "command", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].1.text, "same text\n");
    assert_eq!(rows[1].1.text, "same text\n");
    assert_eq!(rows[2].1.text, "failure detail");
    assert!(
        reopened
            .tool_output_page("foreign", "thread", "turn", "command", 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        reopened
            .tool_output_page("ws", "thread", "turn", "command", rows[2].0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn tool_output_large_unicode_chunks_survive_retry_without_snapshot_copies() {
    let store = fixture(false).await;
    let original = "вывод🦀\n".repeat(30000);
    let n = pioneer_protocol::ItemDeltaNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item_id: "large".into(),
        delta: original.clone(),
        stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
        payload: Some(serde_json::json!({"truncated":false})),
        markdown: None,
        markdown_version: None,
    };
    store.record_tool_output("large", &n).await.unwrap();
    store.record_tool_output("large", &n).await.unwrap();
    let rows = store
        .tool_output_page("ws", "thread", "turn", "large", 0)
        .await
        .unwrap();
    assert!(rows.len() > 1);
    let mut conflicting = n.clone();
    conflicting.delta.truncate(195000);
    assert!(
        store
            .record_tool_output("large", &conflicting)
            .await
            .is_err()
    );
    assert!(rows.iter().all(|(_, row)| row.text.len() <= 128 * 1024));
    assert_eq!(
        rows.iter()
            .map(|(_, row)| row.text.as_str())
            .collect::<String>(),
        original
    );
    assert_eq!(
        rows.iter()
            .filter(|(_, row)| row.metadata.is_some())
            .count(),
        1
    );
    assert!(
        store
            .tool_output_page("ws", "thread", "turn", "large", rows.last().unwrap().0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn history_check_retry_survives_disk_reopen_and_cancelled_writer_admission() {
    use pioneer_crud::compaction::{HistoryCheckDiagnostic as D, HistoryCheckOutcome as O};
    use pioneer_sqlite::{SqliteDatabase, sqlite_read_only_connection_url};
    use sea_orm::{ConnectOptions, EntityTrait, TransactionTrait};
    let path = std::env::temp_dir().join(format!(
        "pioneer-check-retry-{}.db",
        pioneer_protocol::generate_id(21)
    ));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let mut options = ConnectOptions::new(url.clone());
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    let setup = fixture_on(writer.clone(), false).await;
    let mut options = ConnectOptions::new(sqlite_read_only_connection_url(&path));
    options
        .max_connections(1)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
    let reader = Database::connect(options).await.unwrap();
    let database = SqliteDatabase::new(reader, writer);
    let store = CrudStore::new(database.clone()).with_maintenance_access();
    store
        .compaction_enqueue_native_history_check("ws", "thread", "turn", "{}")
        .await
        .unwrap();
    let page = store.compaction_due_history_checks(1000).await.unwrap();
    assert_eq!(page.len(), 1);
    let held = database.begin().await.unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            store.compaction_claim_history_check("turn", page[0].revision, 1000)
        )
        .await
        .is_err()
    );
    // Interactive readers still run while the maintenance write is queued.
    assert_eq!(
        scalar(
            &CrudStore::new(database.clone()),
            "SELECT count(*) AS n FROM turn"
        )
        .await,
        2
    );
    held.rollback().await.unwrap();
    assert_eq!(
        scalar(
            &store,
            "SELECT revision AS n FROM compaction_history_check WHERE turn_id='turn'"
        )
        .await,
        0
    );
    let claim = store
        .compaction_claim_history_check("turn", 0, 1000)
        .await
        .unwrap()
        .unwrap();
    store
        .compaction_begin_history_attempt("turn", claim.revision, "{}", "safe-digest", 1000)
        .await
        .unwrap();
    store
        .compaction_record_history_result(
            "turn",
            claim.revision,
            0,
            O::Retryable,
            &D::new("history_capture", "database_error", "Temporary failure"),
            1000,
        )
        .await
        .unwrap();
    drop(store);
    drop(database);
    drop(setup);
    let reopened = CrudStore::new(Database::connect(url).await.unwrap()).with_maintenance_access();
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
    let row = pioneer_entity::compaction_history_check::Entity::find_by_id("turn")
        .one(&reopened.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert!(row.diagnostic.unwrap().contains("database_error"));
    drop(reopened);
    let _ = std::fs::remove_file(path);
}
