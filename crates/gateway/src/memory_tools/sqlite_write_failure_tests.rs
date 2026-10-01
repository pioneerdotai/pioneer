use super::{classify_memory_write_failure, sqlite_write_failure_code_is_transient};
use pioneer_memory::MemoryWriteFailure;
use pioneer_sqlite::{SqliteDatabase, sqlite_connection_url};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DbBackend, DbErr, RuntimeErr, SqlxError, Statement,
    TransactionTrait,
};

// Use the isolated-test constructor used by the SQLite crate's own fixtures.
// Each handle owns one physical connection and its normal writer executor.
// All statements/transactions below go through SqliteDatabase, not the pools.
async fn open_fixture_database(url: &str) -> SqliteDatabase {
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).sqlx_logging(false);
    let database = SqliteDatabase::from_single_connection(
        Database::connect(options)
            .await
            .expect("open isolated SQLite fixture"),
    );
    database
        .execute_unprepared("PRAGMA busy_timeout = 0")
        .await
        .unwrap();
    database
}

fn database_code(error: &DbErr) -> i32 {
    let DbErr::Exec(RuntimeErr::SqlxError(cause)) = error else {
        panic!("fixture must exercise the production DbErr/RuntimeErr path");
    };
    let SqlxError::Database(database) = cause.as_ref() else {
        panic!("fixture must produce an actual SQLx database error");
    };
    database
        .code()
        .expect("SQLite numeric code")
        .parse()
        .unwrap()
}

fn assert_classification(error: DbErr, primary_code: i32, expected: MemoryWriteFailure) {
    assert_eq!(database_code(&error) & 0xff, primary_code);
    // DbErr is Clone; both inputs retain the real driver's DatabaseError.
    assert_eq!(
        classify_memory_write_failure(anyhow::Error::new(error.clone())),
        expected
    );
    assert_eq!(
        classify_memory_write_failure(anyhow::Error::new(error).context("fixture write context")),
        expected,
    );
}

#[tokio::test]
async fn actual_sqlite_busy_is_transient_through_seaorm_and_anyhow_context() {
    let directory = tempfile::tempdir().unwrap();
    let url = sqlite_connection_url(&directory.path().join("busy.sqlite"));
    let owner = open_fixture_database(&url).await;
    let contender = open_fixture_database(&url).await;
    owner
        .execute_unprepared("CREATE TABLE fact (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();

    // Intentional fixture lock: only the necessary database calls occur while
    // this short transaction is open. No race, sleep, backoff or provider I/O.
    let transaction = owner.begin().await.unwrap();
    transaction
        .execute_unprepared("INSERT INTO fact VALUES (1)")
        .await
        .unwrap();
    let error = contender
        .execute_unprepared("INSERT INTO fact VALUES (2)")
        .await
        .unwrap_err();
    transaction.rollback().await.unwrap();
    assert_eq!(database_code(&error), 5); // SQLITE_BUSY
    assert_classification(error, 5, MemoryWriteFailure::StorageTransient);
    contender.close().await.unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn actual_sqlite_locked_sharedcache_is_transient_through_seaorm_and_context() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "{}&cache=shared",
        sqlite_connection_url(&directory.path().join("locked.sqlite"))
    );
    let owner = open_fixture_database(&url).await;
    let contender = open_fixture_database(&url).await;
    owner
        .execute_unprepared("CREATE TABLE fact (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let transaction = owner.begin().await.unwrap();
    transaction
        .execute_unprepared("CREATE TABLE pending_schema (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    // A schema write lock rejects compilation on the other shared-cache
    // connection. This fails in sqlite3_prepare_v3, before SQLx's step-time
    // unlock-notify loop, so no waiting task or timing-dependent race is needed.
    let error = contender
        .execute_unprepared("INSERT INTO fact VALUES (2)")
        .await
        .unwrap_err();
    transaction.rollback().await.unwrap();
    // The actual extended SQLITE_LOCKED_SHAREDCACHE retains primary LOCKED.
    assert_eq!(database_code(&error), 262);
    assert_classification(error, 6, MemoryWriteFailure::StorageTransient);
    contender.close().await.unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn actual_sqlite_busy_snapshot_preserves_extended_code_classification() {
    let directory = tempfile::tempdir().unwrap();
    let url = sqlite_connection_url(&directory.path().join("snapshot.sqlite"));
    let owner = open_fixture_database(&url).await;
    owner
        .execute_unprepared("PRAGMA journal_mode = WAL")
        .await
        .unwrap();
    owner
        .execute_unprepared("CREATE TABLE fact (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    owner
        .execute_unprepared("INSERT INTO fact VALUES (1)")
        .await
        .unwrap();
    let contender = open_fixture_database(&url).await;

    let transaction = owner.begin().await.unwrap();
    transaction
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT id FROM fact",
        ))
        .await
        .unwrap();
    contender
        .execute_unprepared("INSERT INTO fact VALUES (2)")
        .await
        .unwrap();
    let error = transaction
        .execute_unprepared("INSERT INTO fact VALUES (3)")
        .await
        .unwrap_err();
    transaction.rollback().await.unwrap();
    assert_eq!(database_code(&error), 517); // SQLITE_BUSY_SNAPSHOT
    assert_classification(error, 5, MemoryWriteFailure::StorageTransient);
    contender.close().await.unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn actual_sqlite_constraint_and_matching_text_do_not_authorize_retries() {
    let database = open_fixture_database("sqlite::memory:").await;
    database
        .execute_unprepared("CREATE TABLE fact (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    database
        .execute_unprepared("INSERT INTO fact VALUES (1)")
        .await
        .unwrap();
    let error = database
        .execute_unprepared("INSERT INTO fact VALUES (1)")
        .await
        .unwrap_err();
    assert_classification(error, 19, MemoryWriteFailure::Unclassified); // SQLITE_CONSTRAINT
    for text in ["SQLITE_BUSY", "database is locked"] {
        assert_eq!(
            classify_memory_write_failure(anyhow::anyhow!(text)),
            MemoryWriteFailure::Unclassified
        );
        assert_eq!(
            classify_memory_write_failure(
                anyhow::Error::new(DbErr::Custom(text.into())).context("write context")
            ),
            MemoryWriteFailure::Unclassified,
        );
    }
    database.close().await.unwrap();
}

#[test]
fn sqlite_extended_result_code_families_are_bounded_to_busy_and_locked() {
    // Include primary codes and extended codes that are not all reliably
    // emitted by a portable fixture (BUSY_RECOVERY, BUSY_TIMEOUT, LOCKED_VTAB).
    for code in [5, 261, 517, 773, 6, 262, 518] {
        assert!(sqlite_write_failure_code_is_transient(code), "{code}");
    }
    for code in [0, 1, 10, 19, 1555, 2067] {
        assert!(!sqlite_write_failure_code_is_transient(code), "{code}");
    }
}
