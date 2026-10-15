use migration::Migrator;
use pioneer_entity::{turn_item, turn_item_attempt};
use pioneer_sqlite::{
    SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor, sqlite_connection_url,
    sqlite_read_only_connection_url,
};
use pioneer_tasks::{
    TaskRuntimeInvariantReport, TaskRuntimeInvariantScanner, TaskRuntimeInvariantViolationKind,
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DbBackend, EntityTrait, QueryOrder, Statement,
};
use serde_json::{Value, json};
use std::{fs, path::Path, process::Command};

const CHILD_ACTION: &str = "PIONEER_INVARIANT_TEST_ACTION";
const CHILD_DB: &str = "PIONEER_INVARIANT_TEST_DB";
const CHILD_RESULT: &str = "PIONEER_INVARIANT_TEST_RESULT";
const TURN_ID: &str = "turn_invariant";
const ITEM_ID: &str = "item_invariant";
const ATTEMPT_ID: &str = "attempt_invariant";
const OBSERVED_AT: i64 = 2_000_000_000;

#[test]
fn scan_sqlite_path_reads_compressed_views_without_changing_database() {
    check_fixture("prepare_compressed");
}

#[test]
fn scan_sqlite_path_reads_uncompressed_database_without_changing_database() {
    check_fixture("prepare_plain");
}

#[test]
fn scan_sqlite_path_does_not_create_missing_database() {
    let fixture = tempfile::tempdir().expect("temporary fixture directory");
    let database_dir = fixture.path().join("database");
    fs::create_dir(&database_dir).expect("create database directory");
    let db_path = database_dir.join("missing.db");
    let error = run_child("scan_missing", &db_path);
    assert!(
        error
            .as_str()
            .unwrap()
            .contains("failed to open sqlite database"),
        "unexpected error: {error}"
    );
    assert!(
        !db_path.exists(),
        "read-only scan must not create a database"
    );
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(!Path::new(&format!("{}{suffix}", db_path.display())).exists());
    }
}

fn check_fixture(prepare_action: &str) {
    let fixture = tempfile::tempdir().expect("temporary fixture directory");
    let database_dir = fixture.path().join("database");
    fs::create_dir(&database_dir).expect("create database directory");
    let db_path = database_dir.join("fixture.db");

    // exec starts a new process: SQLite's global auto-extension registration
    // from fixture preparation or neighboring tests cannot reach the scanner.
    let before = run_child(prepare_action, &db_path);
    let files_before = database_files(&database_dir);
    let report: TaskRuntimeInvariantReport =
        serde_json::from_value(run_child("scan", &db_path)).expect("scanner report");

    assert_eq!(report.db_path.as_deref(), db_path.to_str());
    assert_eq!(report.generated_at_unix, Some(OBSERVED_AT));
    assert_eq!(report.violation_count(), 1, "{report}");
    assert_eq!(report.error_count(), 1, "{report}");
    let violation = &report.violations[0];
    assert_eq!(violation.code, "stale_turn_item_attempt", "{report}");
    assert_eq!(violation.entity_id, ITEM_ID);
    assert!(
        matches!(&violation.kind,
            TaskRuntimeInvariantViolationKind::StaleTurnItemAttempt {
                turn_id, item_id, attempt_id, item_status, attempt_status,
                attempt_number: Some(1), ..
            } if turn_id == TURN_ID && item_id == ITEM_ID && attempt_id == ATTEMPT_ID
                && item_status == "completed" && attempt_status == "running"
        ),
        "{report}"
    );

    assert_eq!(
        database_files(&database_dir),
        files_before,
        "scan changed database files"
    );
    let after = run_child("inspect", &db_path);
    assert_eq!(after, before, "scan changed schema or logical rows");
    assert_eq!(
        database_files(&database_dir),
        files_before,
        "inspection changed database files"
    );
}

fn database_files(directory: &Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut files = fs::read_dir(directory)
        .expect("read fixture directory")
        .map(|entry| {
            let entry = entry.expect("fixture entry");
            (
                entry.file_name(),
                fs::read(entry.path()).expect("read database file"),
            )
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn run_child(action: &str, db_path: &Path) -> Value {
    // Keep result files outside the database directory, so the file comparison
    // also catches creation of journals or other SQLite sidecars.
    let result_path = db_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(format!("{action}.json"));
    let output = Command::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "invariant_sqlite_path_child",
            "--ignored",
            "--test-threads=1",
        ])
        .env(CHILD_ACTION, action)
        .env(CHILD_DB, db_path)
        .env(CHILD_RESULT, &result_path)
        .output()
        .expect("start isolated fixture/scanner process");
    assert!(
        output.status.success(),
        "child action {action} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    // A result file also proves the exact filter actually ran the child test.
    serde_json::from_slice(&fs::read(result_path).expect("child result file"))
        .expect("child result JSON")
}

#[tokio::test]
#[ignore = "subprocess helper; invoked by the parent regression tests"]
async fn invariant_sqlite_path_child() {
    let action = std::env::var(CHILD_ACTION).expect("child action");
    let db_path = std::path::PathBuf::from(std::env::var_os(CHILD_DB).expect("child database"));
    let result_path = std::env::var_os(CHILD_RESULT).expect("child result path");
    let result = match action.as_str() {
        "prepare_compressed" | "prepare_plain" => {
            prepare_fixture(&db_path, action == "prepare_compressed").await
        }
        "inspect" => {
            // Inspection has its own process and cannot initialize the scanner.
            pioneer_sqlite::zstd::register_auto_extension_once()
                .expect("register fixture inspection extension");
            let db = connect_fixture(&db_path, true).await;
            let snapshot = fixture_snapshot(&db).await;
            db.close().await.expect("close fixture inspection");
            snapshot
        }
        "scan" => {
            // Do not register an extension, open a pool, run migrations, or
            // initialize Gateway here: scan_sqlite_path must own initialization.
            let report = TaskRuntimeInvariantScanner::new()
                .scan_sqlite_path(&db_path, OBSERVED_AT)
                .await
                .expect("scan fixture in a fresh process without Gateway");
            serde_json::to_value(report).expect("serialize report")
        }
        "scan_missing" => {
            let error = TaskRuntimeInvariantScanner::new()
                .scan_sqlite_path(&db_path, OBSERVED_AT)
                .await
                .expect_err("opening a missing database must fail");
            json!(format!("{error:#}"))
        }
        other => panic!("unexpected child action: {other}"),
    };
    fs::write(result_path, serde_json::to_vec(&result).unwrap()).expect("write child result");
}

async fn connect_fixture(db_path: &Path, read_only: bool) -> SqliteDatabase {
    let url = if read_only {
        sqlite_read_only_connection_url(db_path)
    } else {
        sqlite_connection_url(db_path)
    };
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).sqlx_logging(false);
    let connection = Database::connect(options)
        .await
        .expect("connect local fixture");
    if read_only {
        SqliteDatabase::from(connection)
    } else {
        let writer = SqliteWriteExecutor::new(connection.clone());
        writer
            .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .expect("migrate local fixture through writer executor");
        SqliteDatabase::from_executor(connection, writer)
    }
}

fn payload() -> String {
    json!({"message": "compressed invariant fixture ".repeat(256)}).to_string()
}

async fn prepare_fixture(db_path: &Path, compressed: bool) -> Value {
    if compressed {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("register fixture preparation extension");
    }
    let db = connect_fixture(db_path, false).await;
    db.execute_unprepared("PRAGMA journal_mode = DELETE")
        .await
        .expect("use a self-contained fixture file");
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_item (id, turn_id, item_id, item_type, status, payload) VALUES (?, ?, ?, 'command_execution', 'completed', ?)",
        ["row_invariant".into(), TURN_ID.into(), ITEM_ID.into(), payload().into()],
    )).await.expect("insert terminal item");
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_item_attempt (id, turn_id, item_id, item_type, attempt_number, status, payload) VALUES (?, ?, ?, 'command_execution', 1, 'running', ?)",
        [ATTEMPT_ID.into(), TURN_ID.into(), ITEM_ID.into(), payload().into()],
    )).await.expect("insert stale running attempt");

    if compressed {
        for table in ["turn_item", "turn_item_attempt"] {
            // [nodict] makes every seeded payload eligible immediately, without
            // dictionary training thresholds that could leave rows uncompressed.
            let config = json!({"table": table, "column": "payload", "compression_level": 3, "dict_chooser": "'[nodict]'"});
            db.query_one_write_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT zstd_enable_transparent(?)",
                [config.to_string().into()],
            ))
            .await
            .expect("enable actual transparent compression views");
        }
        let maintenance = db
            .query_one_write_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT zstd_incremental_maintenance(60, 1) AS pending",
            ))
            .await
            .expect("compress fixture payloads")
            .expect("maintenance result");
        assert_eq!(
            maintenance.try_get::<i64>("", "pending").unwrap(),
            0,
            "fixture compression must complete before scanning"
        );
        for table in ["turn_item", "turn_item_attempt"] {
            let view = db
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT sql FROM sqlite_schema WHERE type = 'view' AND name = ?",
                    [table.into()],
                ))
                .await
                .expect("inspect fixture view")
                .unwrap();
            assert!(
                view.try_get::<String>("", "sql")
                    .unwrap()
                    .contains("zstd_decompress_col")
            );
        }
    }
    let snapshot = fixture_snapshot(&db).await;
    db.close()
        .await
        .expect("close fixture before scanner process starts");
    snapshot
}

async fn fixture_snapshot(db: &SqliteDatabase) -> Value {
    let schema = db
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name",
        ))
        .await
        .expect("read schema")
        .into_iter()
        .map(|row| {
            json!({
                "type": row.try_get::<String>("", "type").unwrap(),
                "name": row.try_get::<String>("", "name").unwrap(),
                "table": row.try_get::<String>("", "tbl_name").unwrap(),
                "sql": row.try_get::<Option<String>>("", "sql").unwrap(),
            })
        })
        .collect::<Vec<_>>();
    let items = turn_item::Entity::find()
        .order_by_asc(turn_item::Column::Id)
        .all(db)
        .await
        .expect("read logical items through view");
    let attempts = turn_item_attempt::Entity::find()
        .order_by_asc(turn_item_attempt::Column::Id)
        .all(db)
        .await
        .expect("read logical attempts through view");
    assert_eq!(items.len(), 1);
    assert_eq!(attempts.len(), 1);
    assert_eq!(items[0].payload, payload());
    assert_eq!(attempts[0].payload, payload());
    json!({"schema": schema, "items": items, "attempts": attempts})
}
