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
    let maintenance =
        SqliteDatabase::from_executor(writer_connection, writer.clone()).maintenance();
    maintenance
        .execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    maintenance
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

#[tokio::test]
async fn portable_tool_storage_survives_staging_restart_and_preserves_retry_and_null() {
    let (file, store, _writer) = fixture().await;
    let output = pioneer_protocol::ItemDeltaNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item_id: "portable".into(),
        delta: "точные bytes 🦀\n".repeat(6000),
        stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
        payload: Some(serde_json::json!({"metadata":"separate dictionary ".repeat(400)})),
        markdown: None,
        markdown_version: None,
    };
    store.record_tool_output("portable", &output).await.unwrap();
    let before = store
        .tool_output_page("ws", "thread", "turn", "portable", 0)
        .await
        .unwrap();
    let mut interrupted = pioneer_crud::PortablePayloadProgress::default();
    for _ in 0..80 {
        interrupted.quantum(&store).await.unwrap();
        if scalar(&store, "SELECT count(*) n FROM portable_payload_block").await > 0 {
            break;
        }
    }
    drop(interrupted);
    // Reopening discards in-memory preparation. Incomplete blocks are retired,
    // never selected as a logical payload, and reclaimed in bounded portions.
    let (reopened, _writer) = open(&file.0).await;
    let mut restarted = pioneer_crud::PortablePayloadProgress::default();
    for _ in 0..200 {
        restarted.quantum(&reopened).await.unwrap();
    }
    assert_eq!(
        scalar(
            &reopened,
            "SELECT count(*) n FROM portable_payload WHERE retired=1"
        )
        .await,
        0
    );
    assert!(
        scalar(
            &reopened,
            "SELECT count(*) n FROM tool_output_chunk WHERE text_payload_id IS NOT NULL AND text=''"
        )
        .await
            > 0
    );
    assert!(scalar(&reopened,"SELECT count(*) n FROM tool_output_chunk WHERE metadata_payload_id IS NOT NULL AND metadata=''").await>0);
    reopened
        .record_tool_output("portable", &output)
        .await
        .unwrap();
    let after = reopened
        .tool_output_page("ws", "thread", "turn", "portable", 0)
        .await
        .unwrap();
    assert_eq!(
        before.iter().map(|r| r.0).collect::<Vec<_>>(),
        after.iter().map(|r| r.0).collect::<Vec<_>>()
    );
    assert_eq!(
        after.iter().map(|r| r.1.text.as_str()).collect::<String>(),
        output.delta
    );
    assert_eq!(after[0].1.metadata, before[0].1.metadata);
    // New values admitted after the old pass reached EOF cannot be skipped.
    let mut empty = output.clone();
    empty.delta.clear();
    empty.payload = None;
    reopened.record_tool_output("later", &empty).await.unwrap();
    for _ in 0..80 {
        restarted.quantum(&reopened).await.unwrap();
    }
    let rows = reopened
        .tool_output_page("ws", "thread", "turn", "portable", after.last().unwrap().0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.text, "");
    assert_eq!(rows[0].1.metadata, None);
    assert_eq!(scalar(&reopened,"SELECT p.raw n FROM tool_output_chunk c JOIN portable_payload p ON p.id=c.text_payload_id WHERE c.id='later'").await,1);
    reopened
        .database_connection()
        .execute_unprepared("DELETE FROM tool_output_chunk WHERE item_id='portable'")
        .await
        .unwrap();
    for _ in 0..80 {
        restarted.quantum(&reopened).await.unwrap();
    }
    assert_eq!(
        scalar(&reopened, "SELECT count(*) n FROM portable_payload").await,
        0
    );
    assert_eq!(
        scalar(&reopened, "SELECT count(*) n FROM portable_payload_block").await,
        0
    );
    assert!(
        reopened
            .database_connection()
            .reader_query_only_enabled()
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn portable_hot_append_stream_does_not_starve_updates_below_cursor() {
    let (_file, store, _writer) = fixture().await;
    let output = pioneer_protocol::ItemDeltaNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item_id: "rescan".into(),
        delta: "legacy".into(),
        stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
        payload: None,
        markdown: None,
        markdown_version: None,
    };
    for id in ["a", "m"] {
        store.record_tool_output(id, &output).await.unwrap();
    }
    let mut progress = pioneer_crud::PortablePayloadProgress::default();
    for _ in 0..64 {
        progress.quantum(&store).await.unwrap();
        if scalar(
            &store,
            "SELECT count(*) n FROM tool_output_chunk WHERE id='a' AND text_payload_id IS NOT NULL",
        )
        .await
            == 1
        {
            break;
        }
    }
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM tool_output_chunk WHERE id='a' AND text_payload_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM tool_output_chunk WHERE id='m' AND text_payload_id IS NULL"
        )
        .await,
        1
    );
    // A legacy source mutation invalidates completed backing below the pass's
    // current cursor. Higher keys arrive faster than this worker can encode.
    let replacement = "replacement 界🙂";
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE tool_output_chunk SET text=?1 WHERE id='a'",
            [replacement.into()],
        ))
        .await
        .unwrap();
    let mut revisited = false;
    for index in 0..128 {
        store
            .record_tool_output(&format!("z{index:04}"), &output)
            .await
            .unwrap();
        progress.quantum(&store).await.unwrap();
        if scalar(
            &store,
            "SELECT count(*) n FROM tool_output_chunk WHERE id='a' AND text_payload_id IS NOT NULL",
        )
        .await
            == 1
        {
            revisited = true;
            break;
        }
    }
    assert!(
        revisited,
        "a never-ending higher-key stream cannot starve lower-key updates"
    );
    let rows = store
        .tool_output_page("ws", "thread", "turn", "rescan", 0)
        .await
        .unwrap();
    assert_eq!(
        rows.iter().find(|row| row.1.id == "a").unwrap().1.text,
        replacement
    );
}

#[tokio::test]
async fn portable_cli_mapping_preserves_sent_bytes_control_updates_and_upsert() {
    let (_file, store, _writer) = fixture().await;
    let at = chrono::Utc::now().fixed_offset();
    let history = serde_json::to_string(
        &serde_json::json!([{"role":"user","content":"контекст🦀".repeat(12000)}]),
    )
    .unwrap();
    let mut original = format!(
        "  {}\n",
        serde_json::json!({"pioneerContextBasis":{"historyJson":history,"executionThreadId":"thread","pendingTurn":{"turnId":"turn","messageRevision":0,"messageDeleted":false}},"input":"original sent bytes"})
    );
    let mut binding = pioneer_crud::NewCliRuntimeTurnBinding {
        turn_id: "turn".into(),
        thread_id: "thread".into(),
        continuation_thread_id: "thread".into(),
        workspace_id: "ws".into(),
        runtime_id: "codex".into(),
        runtime_kind: "codex".into(),
        native_thread_id: "native".into(),
        native_turn_id: None,
        request_id: Some("portable-request".into()),
        status: "starting".into(),
        model: None,
        cwd: None,
        sandbox_json: None,
        approval_policy: None,
        input_mapping_json: original.clone(),
        created_at: at,
        updated_at: at,
    };
    store
        .upsert_cli_runtime_turn_binding(binding.clone())
        .await
        .unwrap();
    let mut progress = pioneer_crud::PortablePayloadProgress::default();
    let mut staged = false;
    for _ in 0..100 {
        progress.quantum(&store).await.unwrap();
        if scalar(&store, "SELECT count(*) n FROM portable_payload_block b JOIN portable_payload p ON p.id=b.payload_id WHERE p.family='cli.mapping' AND p.retired=1").await > 0 {
            staged = true;
            break;
        }
    }
    assert!(staged, "replace a real staged source before publication");
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_cli_runtime_binding WHERE turn_id='turn' AND input_mapping_json_payload_id IS NULL").await, 1);
    // Interleave an authoritative logical UPSERT with the old CPU preparation.
    // Even insignificant JSON whitespace is part of the exact sent bytes.
    original.push_str(" \n");
    binding.input_mapping_json = original.clone();
    store
        .upsert_cli_runtime_turn_binding(binding.clone())
        .await
        .unwrap();
    for _ in 0..180 {
        if let Err(error) = progress.quantum(&store).await {
            // A CAS miss is a diagnosed maintenance refusal. Only the old
            // staged source may be discarded; the replacement must converge.
            assert_eq!(
                error.to_string(),
                "portable source changed before publication"
            );
        }
    }
    assert_eq!(scalar(&store,"SELECT count(*) n FROM turn_cli_runtime_binding WHERE input_mapping_json_payload_id IS NOT NULL AND octet_length(input_mapping_json)<256").await,1);
    assert_eq!(
        store
            .get_cli_runtime_turn_binding_by_request("portable-request")
            .await
            .unwrap()
            .unwrap()
            .input_mapping_json,
        original
    );
    assert_eq!(
        store
            .set_cli_runtime_turn_mcp_metadata("turn", None)
            .await
            .unwrap()
            .input_mapping_json,
        original
    );
    assert_eq!(scalar(&store,"SELECT coalesce(json_extract(input_mapping_json,'$.pioneerContextBasis.historyJson'),'[]')<>'[]' n FROM turn_cli_runtime_binding WHERE turn_id='turn'").await,1);
    binding.input_mapping_json =
        r#"{ "pioneerContextBasis": {"historyJson":"[]","executionThreadId":"thread","pendingTurn":{"turnId":"turn","messageRevision":0,"messageDeleted":false}}, "input":"replacement" }"#.into();
    let replacement = binding.input_mapping_json.clone();
    assert_eq!(
        store
            .upsert_cli_runtime_turn_binding(binding)
            .await
            .unwrap()
            .input_mapping_json,
        replacement
    );
    for _ in 0..100 {
        progress.quantum(&store).await.unwrap();
    }
    assert_eq!(
        store
            .get_cli_runtime_turn_binding("turn")
            .await
            .unwrap()
            .unwrap()
            .input_mapping_json,
        replacement
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM portable_payload WHERE retired=1"
        )
        .await,
        0
    );
}

async fn task_snapshot_input(
    store: &CrudStore,
    history_json: String,
) -> pioneer_crud::NewTaskRunConversationSnapshot {
    for sql in [
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('task','ws','thread','thread','thread','turn','agent','running','fixture','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('run','task','run',1,1,'succeeded','agent')",
    ] {
        store
            .database_connection()
            .execute_unprepared(sql)
            .await
            .unwrap();
    }
    pioneer_crud::NewTaskRunConversationSnapshot {
        run_id: "run".into(),
        task_id: "task".into(),
        workspace_id: "ws".into(),
        conversation_thread_id: "thread".into(),
        source_turn_id: None,
        history_json,
        created_at: chrono::Utc::now().fixed_offset(),
    }
}

async fn backfill_task_snapshot(store: &CrudStore) {
    let mut progress = pioneer_crud::PortablePayloadProgress::default();
    for _ in 0..240 {
        progress.quantum(store).await.unwrap();
    }
    assert_eq!(
        scalar(
            store,
            "SELECT count(*) n FROM task_run_conversation_snapshot s JOIN portable_payload p ON p.id=s.history_json_payload_id WHERE s.run_id='run' AND p.raw=0 AND p.retired=0 AND octet_length(s.history_json)<256"
        )
        .await,
        1,
        "exercise published compressed storage, never the plain/raw fallback"
    );
}

#[tokio::test]
async fn portable_task_snapshot_keeps_immutable_winner_and_basis_revision_after_restart() {
    async fn assert_fragments(
        store: &CrudStore,
        reference: &pioneer_compaction::SourceRef,
        history: &str,
    ) {
        let mut rebuilt = String::new();
        let mut offset = 0;
        loop {
            let fragment = store
                .compaction_reference_fragment("ws", "thread", &reference, offset)
                .await
                .unwrap()
                .unwrap();
            let characters = fragment.text.chars().count() as u64;
            assert!(characters > 0 && characters <= 16_384);
            assert_eq!(&fragment.reference, reference);
            assert_eq!(
                fragment.text,
                history
                    .chars()
                    .skip(offset as usize)
                    .take(16_384)
                    .collect::<String>()
            );
            rebuilt.push_str(&fragment.text);
            offset += characters;
            if let Some(next) = fragment.next_character {
                assert_eq!(next, offset);
            } else {
                break;
            }
        }
        assert_eq!(rebuilt, history);
        assert_eq!(offset, history.chars().count() as u64);
    }
    let (file, store, writer) = fixture().await;
    // Preserve formatting, escaping, combining marks and multibyte scalars;
    // returning a parsed/reserialized value would violate this contract.
    let history = format!(
        "  [\n {{\"role\": \"user\", \"content\": {}}}\n]\n",
        serde_json::to_string(&"контекст🦀e\u{301}\"\n".repeat(16_000)).unwrap()
    );
    let mut input = task_snapshot_input(&store, history.clone()).await;
    let inserted = store
        .insert_task_run_conversation_snapshot_if_absent(input.clone())
        .await
        .unwrap();
    assert_eq!(inserted.history_json, history);
    let revision = scalar(
        &store,
        "SELECT revision n FROM compaction_task_basis_revision WHERE run_id='run'",
    )
    .await;
    assert_eq!(revision, 1);
    let reference = pioneer_compaction::SourceRef {
        scope: "task-basis:run".into(),
        id: "run".into(),
        version: format!("task-basis-revision:{revision}"),
    };
    let epoch = store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    // The same public reader must handle legacy TEXT before cutover. These
    // sequential reads cross physical UTF-8/character boundaries, then seek
    // backwards and to EOF with a populated continuation cache.
    assert_fragments(&store, &reference, &history).await;
    for offset in [16_383, 0, history.chars().count() as u64] {
        let fragment = store
            .compaction_reference_fragment("ws", "thread", &reference, offset)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fragment.text,
            history
                .chars()
                .skip(offset as usize)
                .take(16_384)
                .collect::<String>()
        );
        let end = offset + fragment.text.chars().count() as u64;
        assert_eq!(
            fragment.next_character,
            (end < history.chars().count() as u64).then_some(end)
        );
    }
    backfill_task_snapshot(&store).await;
    assert_eq!(
        scalar(&store, "SELECT p.original_bytes n FROM portable_payload p JOIN task_run_conversation_snapshot s ON s.history_json_payload_id=p.id WHERE s.run_id='run'").await,
        history.len() as i64
    );
    assert_eq!(
        scalar(&store, "SELECT p.original_chars n FROM portable_payload p JOIN task_run_conversation_snapshot s ON s.history_json_payload_id=p.id WHERE s.run_id='run'").await,
        history.chars().count() as i64
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT revision n FROM compaction_task_basis_revision WHERE run_id='run'"
        )
        .await,
        revision
    );
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        epoch
    );

    drop(store);
    drop(writer);
    let (store, _writer) = open(&file.0).await;
    assert_eq!(
        store
            .get_task_run_conversation_snapshot("run")
            .await
            .unwrap()
            .unwrap(),
        inserted
    );
    input.history_json = "[ {\"role\":\"assistant\",\"content\":\"losing retry\"} ]".into();
    input.source_turn_id = Some("turn".into());
    input.created_at += chrono::Duration::seconds(1);
    assert_eq!(
        store
            .insert_task_run_conversation_snapshot_if_absent(input)
            .await
            .unwrap(),
        inserted,
        "a retry must return the exact original record after physical cutover"
    );
    assert_eq!(
        store
            .compaction_reference_payload("ws", "thread", &reference)
            .await
            .unwrap()
            .as_deref(),
        Some(history.as_str())
    );

    assert_fragments(&store, &reference, &history).await;
    assert_eq!(
        scalar(
            &store,
            "SELECT revision n FROM compaction_task_basis_revision WHERE run_id='run'"
        )
        .await,
        revision
    );
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        epoch
    );
}

#[tokio::test]
async fn portable_task_snapshot_fragment_does_not_decode_distant_corrupt_block() {
    let (_file, store, _writer) = fixture().await;
    let history = format!(
        "[{{\"role\":\"user\",\"content\":{}}}]",
        serde_json::to_string(&"Ж🦀e\u{301}".repeat(60_000)).unwrap()
    );
    let input = task_snapshot_input(&store, history.clone()).await;
    store
        .insert_task_run_conversation_snapshot_if_absent(input)
        .await
        .unwrap();
    backfill_task_snapshot(&store).await;
    assert!(
        scalar(&store, "SELECT count(*) n FROM portable_payload_block b JOIN task_run_conversation_snapshot s ON s.history_json_payload_id=b.payload_id WHERE s.run_id='run'").await >= 4,
        "the damaged block must be independent of the first fragment"
    );
    let reference = pioneer_compaction::SourceRef {
        scope: "task-basis:run".into(),
        id: "run".into(),
        version: "task-basis-revision:1".into(),
    };
    let distant_offset = scalar(&store, "SELECT b.char_offset n FROM portable_payload_block b JOIN task_run_conversation_snapshot s ON s.history_json_payload_id=b.payload_id WHERE s.run_id='run' AND b.ordinal=3").await as u64;
    assert!(distant_offset >= 16_384);
    let expected_first = history.chars().take(16_384).collect::<String>();
    let expected_distant = history
        .chars()
        .skip(distant_offset as usize)
        .take(16_384)
        .collect::<String>();
    // A successful control at the same scope/version/offset makes a later
    // failure attributable to corruption, rather than an invalid fixture.
    let intact = store
        .compaction_reference_fragment("ws", "thread", &reference, distant_offset)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(intact.text, expected_distant);
    // Content checksums alone do not authenticate a block's position. A bad
    // offset must fail before valid bytes from the wrong block are returned.
    store.database_connection().execute_unprepared(
        "UPDATE portable_payload_block SET char_offset=1 WHERE ordinal=3 AND payload_id=(SELECT history_json_payload_id FROM task_run_conversation_snapshot WHERE run_id='run')"
    ).await.unwrap();
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &reference, 1)
            .await
            .is_err()
    );
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "UPDATE portable_payload_block SET char_offset=?1 WHERE ordinal=3 AND payload_id=(SELECT history_json_payload_id FROM task_run_conversation_snapshot WHERE run_id='run')",
        [i64::try_from(distant_offset).unwrap().into()])).await.unwrap();
    let changed = store.database_connection().execute_unprepared(
        "UPDATE portable_payload_block SET data=zeroblob(78) WHERE ordinal=3 AND payload_id=(SELECT history_json_payload_id FROM task_run_conversation_snapshot WHERE run_id='run')"
    ).await.unwrap();
    assert_eq!(changed.rows_affected(), 1);
    // The fault respects the physical schema's envelope-size CHECK. A whole
    // decode followed by substr would fail here on the unrelated fourth block.
    let first = store
        .compaction_reference_fragment("ws", "thread", &reference, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.text, expected_first);
    assert_eq!(first.next_character, Some(16_384));
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &reference, distant_offset)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn portable_cli_projection_keeps_sqlite_json_predicate_across_block_boundaries() {
    let (_file, store, _writer) = fixture().await;
    store.database_connection().execute_unprepared("INSERT INTO turn_cli_runtime_binding(turn_id,thread_id,continuation_thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,input_mapping_json,status,frozen_manifest_id) VALUES('turn','thread','thread','ws','codex','codex','native','{}','starting',NULL)").await.unwrap();
    let cases = [
        r#""pioneerContextBasis":{"historyJson":"[]"}"#,
        r#""pioneerContextBasis":{"historyJson":null}"#,
        r#""pioneerContextBasis":{"historyJson":0}"#,
        r#""pioneerContextBasis":{"historyJson":[]}"#,
        r#""pioneerContextBasis":{"historyJson":[ ]}"#,
        r#""pioneerContextBasis":{"historyJson":[1]}"#,
        r#""pioneerContextBasis":{"historyJson":"\u005b\u005d"}"#,
        r#""\u0070ioneerContextBasis":{"\u0068istoryJson":"[]"}"#,
        r#""pioneerContextBasis":{"historyJson":"[]","historyJson":"hold"}"#,
        r#""pioneerContextBasis":{"historyJson":"hold","historyJson":"[]"}"#,
        r#""pioneerContextBasis":null,"pioneerContextBasis":{"historyJson":"hold"}"#,
        r#""pioneerContextBasis":{"historyJson":"[]",}"#,
    ];
    let predicate = "SELECT CASE WHEN json_valid(input_mapping_json) THEN coalesce(json_extract(input_mapping_json,'$.pioneerContextBasis.historyJson'),'[]')<>'[]' ELSE 1 END n FROM turn_cli_runtime_binding WHERE turn_id='turn'";
    for body in cases {
        // Force the selected key/escape near the streaming fragment boundary.
        let source = format!("{{\"padding\":\"{}\",{body}}}", "x".repeat(65_500));
        store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "UPDATE turn_cli_runtime_binding SET input_mapping_json=?1,frozen_manifest_id=NULL WHERE turn_id='turn'",[source.clone().into()])).await.unwrap();
        // The existing SQLite predicate is the independent compatibility oracle.
        let expected = scalar(&store, predicate).await;
        let mut progress = pioneer_crud::PortablePayloadProgress::default();
        for _ in 0..100 {
            progress.quantum(&store).await.unwrap();
        }
        assert_eq!(scalar(&store,"SELECT p.raw n FROM portable_payload p JOIN turn_cli_runtime_binding b ON b.input_mapping_json_payload_id=p.id WHERE b.turn_id='turn'").await,0);
        assert_eq!(scalar(&store, predicate).await, expected);
        assert_eq!(
            store
                .get_cli_runtime_turn_binding("turn")
                .await
                .unwrap()
                .unwrap()
                .input_mapping_json,
            source
        );
    }
}

#[tokio::test]
async fn portable_active_staging_cannot_be_reclaimed_by_another_progress_owner() {
    let (_file, store, _writer) = fixture().await;
    let payload = serde_json::json!({"metadata":"independent staged blocks 🦀".repeat(50_000)});
    let output = pioneer_protocol::ItemDeltaNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item_id: "staging".into(),
        delta: String::new(),
        stream: None,
        payload: Some(payload.clone()),
        markdown: None,
        markdown_version: None,
    };
    store.record_tool_output("staging", &output).await.unwrap();
    let mut first = pioneer_crud::PortablePayloadProgress::default();
    let mut staged = false;
    for _ in 0..160 {
        first.quantum(&store).await.unwrap();
        if scalar(&store,"SELECT count(*) n FROM portable_payload p WHERE p.family='tool.metadata' AND p.retired=1 AND (SELECT count(*) FROM portable_payload_block b WHERE b.payload_id=p.id)>=16").await>0 {
            staged=true;break;
        }
    }
    assert!(
        staged,
        "interrupt only after a full encoded batch with more source bytes pending"
    );
    let mut other = pioneer_crud::PortablePayloadProgress::default();
    other.quantum(&store).await.unwrap();
    other.quantum(&store).await.unwrap();
    drop(other);
    assert_eq!(scalar(&store,"SELECT count(*) n FROM portable_payload p WHERE p.family='tool.metadata' AND p.retired=1 AND (SELECT count(*) FROM portable_payload_block b WHERE b.payload_id=p.id)>=16").await,1);
    for _ in 0..180 {
        first.quantum(&store).await.unwrap();
    }
    let rows = store
        .tool_output_page("ws", "thread", "turn", "staging", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.metadata,
        Some(serde_json::to_string(&payload).unwrap())
    );
    assert_eq!(scalar(&store,"SELECT count(*) n FROM portable_payload_block b LEFT JOIN portable_payload p ON p.id=b.payload_id WHERE p.id IS NULL").await,0);
}
