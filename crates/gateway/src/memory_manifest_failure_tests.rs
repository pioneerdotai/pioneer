use super::*;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DbBackend, DbErr, RuntimeErr, Statement,
    TransactionTrait,
};

fn statement(sql: &str) -> Statement {
    Statement::from_string(DbBackend::Sqlite, sql.to_owned())
}

async fn database(url: &str) -> pioneer_sqlite::SqliteDatabase {
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).sqlx_logging(false);
    let db = pioneer_sqlite::SqliteDatabase::from_single_connection(
        Database::connect(options).await.unwrap(),
    )
    .maintenance();
    db.execute_raw(statement("PRAGMA busy_timeout=0"))
        .await
        .unwrap();
    db
}

fn assert_sqlite_failure(error: DbErr, primary: i32, extended: i32) {
    // Real SQLx SQLite error extracted from the production SeaORM wrapper.
    let sqlx = match error {
        DbErr::Exec(RuntimeErr::SqlxError(error)) | DbErr::Query(RuntimeErr::SqlxError(error)) => {
            error
        }
        other => panic!("expected SQLx wrapper, got {other:?}"),
    };
    for wrapper in [
        DbErr::Conn as fn(RuntimeErr) -> DbErr,
        DbErr::Exec,
        DbErr::Query,
    ] {
        let error = wrapper(RuntimeErr::SqlxError(sqlx.clone()));
        let failure = classify_memory_manifest_failure(
            anyhow::Error::new(error).context("SQL /private/canary workspace secret"),
            MemoryManifestFailureStage::Active,
        );
        assert!(failure.retryable());
        assert_eq!(failure.sqlite_primary_code, Some(primary));
        assert_eq!(failure.sqlite_extended_code, Some(extended));
        assert!(!format!("{failure:?}").contains("canary"));
    }
}

#[tokio::test]
async fn manifest_real_sqlite_busy_and_busy_snapshot_keep_extended_codes() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("manifest.db").display()
    );
    let first = database(&url).await;
    first
        .execute_raw(statement("PRAGMA journal_mode=WAL"))
        .await
        .unwrap();
    first
        .execute_raw(statement("CREATE TABLE manifest_lock (value INTEGER)"))
        .await
        .unwrap();
    first
        .execute_raw(statement("INSERT INTO manifest_lock VALUES (1)"))
        .await
        .unwrap();
    let second = database(&url).await;
    let transaction = first.begin().await.unwrap();
    transaction
        .execute_raw(statement("UPDATE manifest_lock SET value=2"))
        .await
        .unwrap();
    let busy = second
        .execute_raw(statement("UPDATE manifest_lock SET value=3"))
        .await
        .unwrap_err();
    transaction.rollback().await.unwrap();
    assert_sqlite_failure(busy, 5, 5);

    let snapshot = first.begin().await.unwrap();
    snapshot
        .query_all_raw(statement("SELECT * FROM manifest_lock"))
        .await
        .unwrap();
    second
        .execute_raw(statement("UPDATE manifest_lock SET value=4"))
        .await
        .unwrap();
    let busy_snapshot = snapshot
        .execute_raw(statement("UPDATE manifest_lock SET value=5"))
        .await
        .unwrap_err();
    snapshot.rollback().await.unwrap();
    assert_sqlite_failure(busy_snapshot, 5, 517);
    second.close().await.unwrap();
    first.close().await.unwrap();
}

#[tokio::test]
async fn manifest_real_sqlite_locked_shared_cache_keeps_extended_code() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc&cache=shared",
        directory.path().join("locked.db").display()
    );
    let first = database(&url).await;
    first
        .execute_raw(statement("PRAGMA journal_mode=DELETE"))
        .await
        .unwrap();
    first
        .execute_raw(statement("CREATE TABLE manifest_lock (value INTEGER)"))
        .await
        .unwrap();
    first
        .execute_raw(statement("INSERT INTO manifest_lock VALUES (1)"))
        .await
        .unwrap();
    let second = database(&url).await;
    let transaction = first.begin().await.unwrap();
    // Fail during statement preparation under a schema lock, before SQLx's
    // step-time unlock-notify loop. This fixture never waits for the lock.
    transaction
        .execute_raw(statement("CREATE TABLE pending_schema (value INTEGER)"))
        .await
        .unwrap();
    let locked = second
        .execute_raw(statement("INSERT INTO manifest_lock VALUES (2)"))
        .await
        .unwrap_err();
    transaction.rollback().await.unwrap();
    assert_sqlite_failure(locked, 6, 262);
    second.close().await.unwrap();
    first.close().await.unwrap();
}

#[test]
fn manifest_typed_pool_timeout_survives_context_at_each_stage() {
    for stage in [
        MemoryManifestFailureStage::Runtime,
        MemoryManifestFailureStage::Authorization,
        MemoryManifestFailureStage::Active,
        MemoryManifestFailureStage::Candidates,
    ] {
        for error in [
            DbErr::ConnectionAcquire(sea_orm::ConnAcquireErr::Timeout),
            DbErr::Query(RuntimeErr::SqlxError(std::sync::Arc::new(
                sea_orm::SqlxError::PoolTimedOut,
            ))),
        ] {
            let failure = classify_memory_manifest_failure(
                anyhow::Error::new(error).context("secret SQL path thread"),
                stage,
            );
            assert!(failure.retryable());
            assert_eq!(failure.stage, stage);
            assert_eq!(failure.sqlite_primary_code, None);
        }
    }
}

#[tokio::test]
async fn manifest_cantopen_and_text_and_unscoped_json_are_not_transient() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rw",
        directory.path().join("missing.db").display()
    );
    let cantopen = Database::connect(url).await.unwrap_err();
    let failure =
        classify_memory_manifest_failure(cantopen.into(), MemoryManifestFailureStage::Active);
    assert!(!failure.retryable());
    assert_eq!(failure.sqlite_primary_code, Some(14));
    for error in [
        anyhow::anyhow!("SQLite BUSY LOCKED code: 5 connection pool timed out"),
        anyhow::anyhow!("Memvid backend failed: /private/path token=secret"),
        anyhow::Error::new(serde_json::from_str::<serde_json::Value>("{").unwrap_err()),
        anyhow::Error::new(pioneer_memory::MemoryWriteFailure::StorageTransient),
    ] {
        let failure = classify_memory_manifest_failure(
            error.context("load inventory"),
            MemoryManifestFailureStage::Candidates,
        );
        assert_eq!(failure.class, MemoryManifestFailureClass::Unclassified);
        assert!(!failure.retryable());
        assert_eq!(failure.sqlite_primary_code, None);
    }
    let failure = classify_memory_manifest_failure(
        anyhow::Error::new(pioneer_memory::MemoryWriteFailure::AuthorizationOrDomain)
            .context("denied"),
        MemoryManifestFailureStage::Authorization,
    );
    assert_eq!(
        failure.class,
        MemoryManifestFailureClass::AuthorizationOrDomain
    );
    assert!(!failure.retryable());
}

#[derive(Debug)]
struct OtherDatabase;
impl std::fmt::Display for OtherDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SQLite BUSY SQL secret")
    }
}
impl std::error::Error for OtherDatabase {}
impl sea_orm::sqlx::error::DatabaseError for OtherDatabase {
    fn message(&self) -> &str {
        "SQLite BUSY SQL secret"
    }
    fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
        Some("5".into())
    }
    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> sea_orm::sqlx::error::ErrorKind {
        sea_orm::sqlx::error::ErrorKind::Other
    }
}

#[test]
fn manifest_numeric_code_from_other_backend_is_not_sqlite() {
    let error = DbErr::Query(RuntimeErr::SqlxError(std::sync::Arc::new(
        sea_orm::SqlxError::Database(Box::new(OtherDatabase)),
    )));
    let failure =
        classify_memory_manifest_failure(error.into(), MemoryManifestFailureStage::Active);
    assert_eq!(failure.class, MemoryManifestFailureClass::Unclassified);
    assert!(!failure.retryable());
    assert_eq!(failure.sqlite_primary_code, None);
    assert_eq!(failure.sqlite_extended_code, None);
}

#[tokio::test]
async fn manifest_missing_processor_is_safe_unclassified_runtime_failure() {
    let provider = GatewayMemoryProvider::new(std::sync::Weak::new());
    let failure = provider
        .load_memory_manifest(
            MemoryTurnContext {
                workspace_id: "workspace-canary".into(),
                thread_id: "thread-canary".into(),
                conversation_thread_id: None,
                turn_id: "turn-canary".into(),
                mode: pioneer_protocol::ThreadMode::Agent,
                input_text: "memory-content-canary secret".into(),
                task_id: None,
                agent_id: None,
                principal_id: None,
            },
            MemoryManifestRequest {
                max_items: 8,
                max_item_chars: 100,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(failure.stage, MemoryManifestFailureStage::Runtime);
    assert_eq!(failure.class, MemoryManifestFailureClass::Unclassified);
    assert!(!failure.retryable());
    assert!(!format!("{failure:?}").contains("canary"));
}
