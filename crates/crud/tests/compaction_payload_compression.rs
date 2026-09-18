use migration::Migrator;
use pioneer_crud::CrudStore;
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use std::path::{Path, PathBuf};

struct TestFile(PathBuf);
impl Drop for TestFile {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

async fn open(path: &Path) -> (CrudStore, SqliteWriteExecutor) {
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options.max_connections(1).min_connections(1);
    let writer_connection = Database::connect(options).await.unwrap();
    let writer = SqliteWriteExecutor::new(writer_connection.clone());
    SqliteDatabase::from_executor(writer_connection, writer.clone())
        .maintenance()
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
    options.max_connections(1).min_connections(1);
    let reader = Database::connect(options).await.unwrap();
    // Connection initialization only. All fixture and migration work goes through
    // the scoped database and its serialized maintenance writer.
    reader
        .execute_unprepared("PRAGMA query_only=ON")
        .await
        .unwrap();
    let db = SqliteDatabase::from_executor(reader, writer.clone());
    assert!(db.reader_query_only_enabled().await.unwrap());
    (CrudStore::new(db).with_maintenance_access(), writer)
}

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

async fn fixture() -> (TestFile, CrudStore, SqliteWriteExecutor) {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let file = TestFile(std::env::temp_dir().join(format!(
        "pioneer-compaction-zstd-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let (store, writer) = open(&file.0).await;
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('event','thread','turn',1,'fixture','{\"text\":\"original\"}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES ('item','turn','item','command_execution','completed',0,'{\"text\":\"original\"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('input','turn',0,'text','original','{\"text\":\"original\"}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('context','turn',1,'assistant_round','{\"text\":\"original\"}','{}',CURRENT_TIMESTAMP)",
    ] {
        store
            .database_connection()
            .execute_unprepared(sql)
            .await
            .unwrap();
    }
    (file, store, writer)
}

async fn enable(store: &CrudStore, table: &str) {
    let config = serde_json::json!({"table":table,"column":"payload","compression_level":3,"dict_chooser":"'[nodict]'"}).to_string();
    store
        .database_connection()
        .query_one_write_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT zstd_enable_transparent(?)",
            [config.into()],
        ))
        .await
        .unwrap();
}

async fn compress(store: &CrudStore, table: &str) {
    let payload: String = store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            format!("SELECT payload FROM _{table}_zstd WHERE _payload_dict IS NULL LIMIT 1"),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "payload")
        .unwrap();
    // CPU work takes place after releasing the reader and before reserving the
    // writer. The same original-payload CAS is used by Gateway maintenance.
    let compressed =
        pioneer_sqlite::zstd::compress_column_value(payload.as_bytes(), 3, None).unwrap();
    assert_eq!(store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        format!("UPDATE _{table}_zstd SET payload=?,_payload_dict=-1 WHERE _payload_dict IS NULL AND payload=?"),
        [compressed.into(), payload.into()])).await.unwrap().rows_affected(), 1);
    assert_eq!(scalar(store, &format!("SELECT COUNT(*) n FROM _{table}_zstd WHERE typeof(payload)='blob' AND _payload_dict=-1")).await, 1);
}

#[tokio::test]
async fn compaction_revisions_survive_physical_compression() {
    let (_file, store, _writer) = fixture().await;
    let sources = [
        (
            "turn_event",
            "event",
            "event",
            "event-revision",
            "sequence=sequence+1",
        ),
        (
            "turn_item",
            "item",
            "item",
            "item-revision",
            "status='failed'",
        ),
        (
            "turn_input",
            "input",
            "input",
            "input-revision",
            "input_index=input_index+1",
        ),
        (
            "turn_llm_context",
            "source",
            "context",
            "revision",
            "sequence=sequence+1",
        ),
    ];
    for (table, revision, scope, prefix, metadata_edit) in sources {
        enable(&store, table).await;
        let source = pioneer_compaction::SourceRef {
            scope: format!("{scope}:turn"),
            id: scope.into(),
            version: format!("{prefix}:1"),
        };
        let before = store
            .compaction_reference_fragment("ws", "thread", &source, 0)
            .await
            .unwrap()
            .unwrap();
        let epoch = store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap();
        let fence = store.compaction_history_read_fence().await.unwrap();
        compress(&store, table).await;
        let after = store
            .compaction_reference_fragment("ws", "thread", &source, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.text, after.text);
        assert_eq!(before.reference, after.reference);
        assert_eq!(
            store
                .compaction_projection_version("ws", "thread")
                .await
                .unwrap(),
            epoch
        );
        let after_fence = store.compaction_history_read_fence().await.unwrap();
        assert_eq!(
            (
                after_fence.turn_order,
                after_fence.turn_id,
                after_fence.input_order,
                after_fence.event_order,
                after_fence.context_order
            ),
            (
                fence.turn_order,
                fence.turn_id,
                fence.input_order,
                fence.event_order,
                fence.context_order
            )
        );
        assert!(
            store
                .compaction_sources_current("ws", "thread", &[source.clone()])
                .await
                .unwrap()
        );
        // A metadata edit on a compressed payload is still a logical edit.
        store
            .database_connection()
            .execute_unprepared(&format!("UPDATE {table} SET {metadata_edit}"))
            .await
            .unwrap();
        assert!(
            store
                .compaction_reference_fragment("ws", "thread", &source, 0)
                .await
                .is_err()
        );
        assert!(
            store
                .compaction_projection_version("ws", "thread")
                .await
                .unwrap()
                > epoch
        );
        assert_eq!(scalar(&store, &format!("SELECT revision n FROM compaction_{revision}_revision WHERE source_id='{scope}'")).await, 2);
        store
            .database_connection()
            .execute_unprepared(&format!(
                "UPDATE {table} SET payload='{{\"text\":\"edited\"}}'"
            ))
            .await
            .unwrap();
        let edited_revision = scalar(
            &store,
            &format!(
                "SELECT revision n FROM compaction_{revision}_revision WHERE source_id='{scope}'"
            ),
        )
        .await;
        assert!(edited_revision > 2);
        compress(&store, table).await;
        assert_eq!(scalar(&store, &format!("SELECT revision n FROM compaction_{revision}_revision WHERE source_id='{scope}'")).await, edited_revision);
    }
    assert!(
        store
            .database_connection()
            .reader_query_only_enabled()
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn whole_and_batched_payload_loading_preserves_unicode_large_values_and_order() {
    let (_file, store, _writer) = fixture().await;
    let large = serde_json::json!({"text": "🧪漢字".repeat(90_000)}).to_string();
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event SET payload=? WHERE id='event'",
            [large.clone().into()],
        ))
        .await
        .unwrap();
    enable(&store, "turn_event").await;
    compress(&store, "turn_event").await;
    let event = pioneer_compaction::SourceRef {
        scope: "event:turn".into(),
        id: "event".into(),
        version: "event-revision:2".into(),
    };
    assert_eq!(
        store
            .compaction_reference_payload("ws", "thread", &event)
            .await
            .unwrap()
            .as_deref(),
        Some(large.as_str())
    );

    let first = "a".repeat(130_000);
    let second = "б".repeat(65_000);
    let third = "c".repeat(130_000);
    for (index, (id, payload)) in [("input", &first), ("input-2", &second), ("input-3", &third)]
        .into_iter()
        .enumerate()
    {
        if index == 0 {
            store
                .database_connection()
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "UPDATE turn_input SET payload=? WHERE id=?",
                    [payload.clone().into(), id.into()],
                ))
                .await
                .unwrap();
        } else {
            store.database_connection().execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES(?, 'turn', ?, 'text', '', ?, CURRENT_TIMESTAMP)",
                [id.into(), (index as i64).into(), payload.clone().into()],
            )).await.unwrap();
        }
    }
    let refs = [
        pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: "input".into(),
            version: "input-revision:2".into(),
        },
        pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: "input-2".into(),
            version: "input-revision:1".into(),
        },
        pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: "input-3".into(),
            version: "input-revision:1".into(),
        },
    ];
    let (consumed, payloads) = store
        .compaction_reference_payload_batch("ws", "thread", &refs)
        .await
        .unwrap();
    assert_eq!(consumed, 2);
    assert_eq!(payloads, [first, second]);
    let (consumed, payloads) = store
        .compaction_reference_payload_batch("ws", "thread", &refs[2..])
        .await
        .unwrap();
    assert_eq!(consumed, 1);
    assert_eq!(payloads, [third]);
    store
        .database_connection()
        .execute_unprepared("UPDATE turn_input SET payload='changed' WHERE id='input-2'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_payload_batch("ws", "thread", &refs[..2])
            .await
            .is_err(),
        "a stale member cannot disappear from a successful batch"
    );
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_input WHERE id='input-3'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_payload_batch("ws", "thread", &refs[2..])
            .await
            .is_err(),
        "a tombstoned member cannot disappear from a successful batch"
    );
}

/// Manual CRUD micro-scenario used beside the end-to-end Gateway benchmark.
/// Calls and returned bytes are measured; the SQL-query figure is explicitly
/// an implementation-derived estimate. Elapsed time is diagnostic only.
#[tokio::test]
#[ignore = "manual history preparation benchmark; run only in the dedicated benchmark stage"]
async fn benchmark_history_payload_loading_many_small_and_one_large_compressed_result() {
    let (_file, store, _writer) = fixture().await;
    for index in 1..=1_000_i64 {
        store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES(?, 'turn', ?, 'text', '', ?, CURRENT_TIMESTAMP)",
            [format!("bench-{index}").into(), index.into(), serde_json::json!({"type":"text","text":format!("message-{index}")}).to_string().into()],
        )).await.unwrap();
    }
    let large = serde_json::json!({"result":"z".repeat(4 * 1024 * 1024)}).to_string();
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event SET payload=? WHERE id='event'",
            [large.clone().into()],
        ))
        .await
        .unwrap();
    enable(&store, "turn_event").await;
    compress(&store, "turn_event").await;

    let mut refs = (1..=1_000_i64)
        .map(|index| pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: format!("bench-{index}"),
            version: "input-revision:1".into(),
        })
        .collect::<Vec<_>>();
    refs.push(pioneer_compaction::SourceRef {
        scope: "event:turn".into(),
        id: "event".into(),
        version: "event-revision:2".into(),
    });
    let started = std::time::Instant::now();
    let mut offset = 0usize;
    let mut batches = 0usize;
    let mut retained_bytes = 0usize;
    while offset < refs.len() {
        let (consumed, payloads) = store
            .compaction_reference_payload_batch("ws", "thread", &refs[offset..])
            .await
            .unwrap();
        retained_bytes = retained_bytes.max(payloads.iter().map(String::len).sum());
        offset += consumed;
        batches += 1;
    }
    eprintln!(
        "history_payload_crud_microbenchmark rows={} measured_payload_calls={} estimated_reader_queries={} measured_max_returned_bytes={} elapsed_ms={}",
        refs.len(),
        batches,
        batches * 2 - 1,
        retained_bytes,
        started.elapsed().as_millis()
    );
}

#[tokio::test]
async fn stale_compaction_references_remain_stale_after_compression_and_restart() {
    let (file, store, writer) = fixture().await;
    let reference = pioneer_compaction::SourceRef {
        scope: "event:turn".into(),
        id: "event".into(),
        version: "event-revision:1".into(),
    };
    store
        .compaction_reference_fragment("ws", "thread", &reference, 0)
        .await
        .unwrap()
        .unwrap();
    // A real edit invalidates a published reference. Compression and restart
    // must preserve that distinction rather than accepting the stale history.
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload='{\"text\":\"edited\"}' WHERE id='event'",
        )
        .await
        .unwrap();
    enable(&store, "turn_event").await;
    compress(&store, "turn_event").await;
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &reference, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale source revision")
    );
    assert!(
        store
            .compaction_reference_payload("ws", "thread", &reference)
            .await
            .unwrap()
            .is_none(),
        "a stale exact revision must not return a payload"
    );
    assert!(
        store
            .compaction_reference_payload_batch("ws", "thread", std::slice::from_ref(&reference))
            .await
            .is_err(),
        "a stale batch must not return a partial success"
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT revision n FROM compaction_event_revision WHERE source_id='event'"
        )
        .await,
        2
    );
    drop(store);
    drop(writer);
    let (store, writer) = open(&file.0).await;
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &reference, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale source revision")
    );
    assert!(
        store
            .compaction_reference_payload("ws", "thread", &reference)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_reference_payload_batch("ws", "thread", std::slice::from_ref(&reference))
            .await
            .is_err()
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT revision n FROM compaction_event_revision WHERE source_id='event'"
        )
        .await,
        2
    );
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload='{\"text\":\"edited again\"}' WHERE id='event'",
        )
        .await
        .unwrap();
    compress(&store, "turn_event").await;
    assert_eq!(
        scalar(
            &store,
            "SELECT revision n FROM compaction_event_revision WHERE source_id='event'"
        )
        .await,
        3
    );
    let current = pioneer_compaction::SourceRef {
        scope: "event:turn".into(),
        id: "event".into(),
        version: "event-revision:3".into(),
    };
    assert!(
        store
            .compaction_reference_payload("ws", "thread", &current)
            .await
            .unwrap()
            .is_some()
    );
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='event'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_payload("ws", "thread", &current)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_reference_payload_batch("ws", "thread", std::slice::from_ref(&current))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn whole_and_batch_reads_reject_noncanonical_revision_aliases() {
    let (_file, store, _writer) = fixture().await;
    for (scope, id, version) in [
        ("input:turn", "input", "input-revision:01"),
        ("context:turn", "context", "revision:+1"),
        ("event:turn", "event", "event-revision:01"),
        ("item:turn", "item", "item-revision:+1"),
    ] {
        let reference = pioneer_compaction::SourceRef {
            scope: scope.into(),
            id: id.into(),
            version: version.into(),
        };
        assert!(
            store
                .compaction_reference_payload("ws", "thread", &reference)
                .await
                .is_err(),
            "whole read accepted noncanonical version {version}"
        );
        assert!(
            store
                .compaction_reference_payload_batch(
                    "ws",
                    "thread",
                    std::slice::from_ref(&reference),
                )
                .await
                .is_err(),
            "batch read accepted noncanonical version {version}"
        );
    }
}
