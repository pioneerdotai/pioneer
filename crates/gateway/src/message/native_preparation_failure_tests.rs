use super::{
    classify_native_preparation_error, confirm_owned_cancellation_lookup,
    native_preparation_sqlite_code_is_lock,
};
use pioneer_sqlite::{SqliteDatabase, sqlite_connection_url};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DbBackend, DbErr, RuntimeErr, SqlxError, Statement,
    TransactionTrait,
};

// Use the isolated-test constructor used by the SQLite crate's own fixtures.
// Each handle owns one physical connection and its normal writer executor.
// All statements/transactions below go through SqliteDatabase, not the pools.
pub(in crate::message) async fn open_fixture_database(url: &str) -> SqliteDatabase {
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

fn assert_classification(error: DbErr, primary_code: i32, expected: bool) {
    assert_eq!(database_code(&error) & 0xff, primary_code);
    // DbErr is Clone; both inputs retain the real driver's DatabaseError.
    assert_eq!(
        classify_native_preparation_error(anyhow::Error::new(error.clone())).is_retryable(),
        expected
    );
    assert_eq!(
        classify_native_preparation_error(
            anyhow::Error::new(error).context("fixture write context")
        )
        .is_retryable(),
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
    assert_classification(error, 5, true);
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
    assert_classification(error, 6, true);
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
    assert_classification(error, 5, true);
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
    assert_classification(error, 19, false); // SQLITE_CONSTRAINT
    for text in ["SQLITE_BUSY", "database is locked"] {
        assert_eq!(
            classify_native_preparation_error(anyhow::anyhow!(text)).is_retryable(),
            false
        );
        assert_eq!(
            classify_native_preparation_error(
                anyhow::Error::new(DbErr::Custom(text.into())).context("write context")
            )
            .is_retryable(),
            false,
        );
    }
    database.close().await.unwrap();
}

#[test]
fn sqlite_extended_result_code_families_are_bounded_to_busy_and_locked() {
    // Include primary codes and extended codes that are not all reliably
    // emitted by a portable fixture (BUSY_RECOVERY, BUSY_TIMEOUT, LOCKED_VTAB).
    for code in [5, 261, 517, 773, 6, 262, 518] {
        assert!(native_preparation_sqlite_code_is_lock(code), "{code}");
    }
    for code in [0, 1, 10, 19, 1555, 2067] {
        assert!(!native_preparation_sqlite_code_is_lock(code), "{code}");
    }
}

#[tokio::test]
async fn receipt_lookup_error_keeps_its_typed_storage_reason() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "{}&cache=shared",
        sqlite_connection_url(&directory.path().join("receipt.sqlite"))
    );
    let owner = open_fixture_database(&url).await;
    owner.execute_unprepared("CREATE TABLE native_cancellation_context (turn_id TEXT PRIMARY KEY, accepted_event_id TEXT, execution_owner_id TEXT); CREATE TABLE turn_execution(turn_id TEXT, owner_id TEXT)").await.unwrap();
    let contender = open_fixture_database(&url).await;
    let store = pioneer_crud::CrudStore::new(contender.clone());
    let tx = owner.begin().await.unwrap();
    tx.execute_unprepared("CREATE TABLE locked_schema(id INTEGER)")
        .await
        .unwrap();
    let result = store
        .native_cancellation_was_accepted_owned("turn", "owner")
        .await;
    tx.rollback().await.unwrap();
    let rejection = confirm_owned_cancellation_lookup(result).unwrap_err();
    assert!(rejection.is_retryable());
    assert_eq!(rejection.code(), "storage_temporarily_unavailable");
    assert!(confirm_owned_cancellation_lookup(Ok(true)).unwrap());
    assert!(!confirm_owned_cancellation_lookup(Ok(false)).unwrap());
    assert_eq!(
        confirm_owned_cancellation_lookup(Err(anyhow::anyhow!("domain conflict")))
            .unwrap_err()
            .code(),
        "native_preparation_rejected"
    );
}
