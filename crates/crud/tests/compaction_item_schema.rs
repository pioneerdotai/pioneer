use migration::{Migrator, MigratorTrait};
use pioneer_crud::CrudStore;
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use std::path::{Path, PathBuf};

const MIGRATION: &str = "m20260913_000001_remove_compaction_item_capture_order";

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

async fn fixture(compressed: bool) -> (TestFile, CrudStore, SqliteWriteExecutor) {
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let file = TestFile(std::env::temp_dir().join(format!(
        "pioneer-item-schema-{}.sqlite",
        uuid::Uuid::new_v4()
    )));
    let (store, writer) = open(&file.0).await;
    let before = Migrator::migrations()
        .iter()
        .position(|m| m.name() == MIGRATION)
        .unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, Some(before as u32))
        .await
        .unwrap();
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        r#"INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES ('item','turn','item','command_execution','completed',0,'{"storage":{"kind":"shell"},"output":"retained result"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"#,
        "UPDATE compaction_item_revision SET revision=7,capture_order=41 WHERE source_id='item'",
        "INSERT INTO compaction_item_revision(source_id,turn_id,revision,present,capture_order) VALUES ('deleted','turn',9,0,42)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    if compressed {
        compress(&store).await;
    }
    (file, store, writer)
}

async fn compress(store: &CrudStore) {
    let config = serde_json::json!({"table":"turn_item","column":"payload","compression_level":3,"dict_chooser":"'[nodict]'"});
    store
        .database_connection()
        .query_one_write_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT zstd_enable_transparent(?)",
            [config.to_string().into()],
        ))
        .await
        .unwrap();
}

async fn count(store: &CrudStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn assert_clean(store: &CrudStore) {
    assert_eq!(count(store, "SELECT count(*) n FROM pragma_table_info('compaction_item_revision') WHERE name='capture_order'").await, 0);
    assert_eq!(count(store, "SELECT count(*) n FROM sqlite_master WHERE name='compaction_item_revision_capture_order'").await, 0);
    assert_eq!(count(store, "SELECT count(*) n FROM sqlite_master WHERE type='trigger' AND name IN ('compaction_item_insert','compaction_item_update','compaction_item_delete') AND sql LIKE '%capture_order%'").await, 0);
    assert_eq!(count(store, "SELECT count(*) n FROM sqlite_master WHERE type='trigger' AND name IN ('compaction_item_insert','compaction_item_update','compaction_item_delete')").await, 3);
    assert_eq!(count(store, "SELECT count(*) n FROM compaction_item_revision WHERE (source_id='item' AND turn_id='turn' AND revision=7 AND present=1) OR (source_id='deleted' AND turn_id='turn' AND revision=9 AND present=0)").await, 2);
    assert_eq!(
        count(
            store,
            &format!("SELECT count(*) n FROM seaql_migrations WHERE version='{MIGRATION}'")
        )
        .await,
        1
    );
    assert!(
        store
            .database_connection()
            .reader_query_only_enabled()
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn item_capture_cleanup_preserves_revisions_and_restarts_on_plain_and_compressed_storage() {
    for compressed in [false, true] {
        let (file, store, writer) = fixture(compressed).await;
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
        assert_clean(&store).await;
        // Reopen the durable database and execute the body again, rather than
        // only taking the already-applied migration marker fast path.
        store
            .database_connection()
            .execute_unprepared(&format!(
                "DELETE FROM seaql_migrations WHERE version='{MIGRATION}'"
            ))
            .await
            .unwrap();
        drop(store);
        drop(writer);
        let (store, writer) = open(&file.0).await;
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
        assert_clean(&store).await;
        let original = store
            .compaction_tool_result_fragment("ws", "thread", "turn", "item", None, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.reference.version, "item-revision:7");
        assert!(original.text.contains("retained result"));
        // Also exercise enabling transparent compression after the new migration.
        if !compressed {
            compress(&store).await;
        }
        let db = store.database_connection();
        db.execute_unprepared(
            "UPDATE turn_item SET payload=json_set(payload,'$.output','changed') WHERE id='item'",
        )
        .await
        .unwrap();
        assert!(
            store
                .compaction_reference_fragment("ws", "thread", &original.reference, 0)
                .await
                .is_err()
        );
        assert_eq!(count(&store, "SELECT count(*) n FROM compaction_item_revision WHERE source_id='item' AND revision=8 AND present=1").await, 1);
        db.execute_unprepared("DELETE FROM turn_item WHERE id='item'")
            .await
            .unwrap();
        assert_eq!(count(&store, "SELECT count(*) n FROM compaction_item_revision WHERE source_id='item' AND revision=9 AND present=0").await, 1);
        db.execute_unprepared("INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES ('item','turn','item','command_execution','completed',0,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        assert_eq!(count(&store, "SELECT count(*) n FROM compaction_item_revision WHERE source_id='item' AND revision=10 AND present=1").await, 1);
        assert_eq!(
            store
                .compaction_history_turn_page(
                    "ws",
                    "thread",
                    "",
                    &store.compaction_history_read_fence().await.unwrap()
                )
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn item_capture_cleanup_rolls_back_schema_and_triggers_when_marker_commit_fails() {
    for compressed in [false, true] {
        let (_file, store, writer) = fixture(compressed).await;
        let db = store.database_connection();
        db.execute_unprepared(&format!("CREATE TRIGGER reject_capture_cleanup BEFORE INSERT ON seaql_migrations WHEN NEW.version='{MIGRATION}' BEGIN SELECT RAISE(ABORT,'fixture marker failure'); END")).await.unwrap();
        assert!(
            writer
                .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
                .await
                .is_err()
        );
        assert_eq!(count(&store, "SELECT count(*) n FROM pragma_table_info('compaction_item_revision') WHERE name='capture_order'").await, 1);
        assert_eq!(count(&store, "SELECT count(*) n FROM sqlite_master WHERE name='compaction_item_revision_capture_order'").await, 1);
        assert_eq!(count(&store, "SELECT count(*) n FROM sqlite_master WHERE type='trigger' AND name IN ('compaction_item_insert','compaction_item_update','compaction_item_delete') AND sql LIKE '%capture_order%'").await, 3);
        assert_eq!(
            count(
                &store,
                &format!("SELECT count(*) n FROM seaql_migrations WHERE version='{MIGRATION}'")
            )
            .await,
            0
        );
        assert_eq!(count(&store, "SELECT count(*) n FROM compaction_item_revision WHERE source_id='item' AND revision=7 AND present=1 AND capture_order=41").await, 1);
        db.execute_unprepared("DROP TRIGGER reject_capture_cleanup")
            .await
            .unwrap();
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
        assert_clean(&store).await;
    }
}
