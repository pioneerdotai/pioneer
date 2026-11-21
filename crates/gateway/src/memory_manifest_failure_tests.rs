use super::*;
use crate::tests::postgres_failures::{lock_unavailable, rejected_snapshot, rejected_statement};
use sea_orm::{DbErr, RuntimeErr};

fn assert_postgres_failure(error: DbErr, expected: [u8; 5]) {
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
        let failure = classify_memory_manifest_failure(
            anyhow::Error::new(wrapper(RuntimeErr::SqlxError(sqlx.clone())))
                .context("SQL /private/canary workspace secret"),
            MemoryManifestFailureStage::Active,
        );
        assert!(failure.retryable());
        assert_eq!(failure.postgres_sqlstate, Some(expected));
        assert!(!format!("{failure:?}").contains("canary"));
    }
}

#[pioneer_postgres_test::postgres_test]
async fn manifest_real_postgres_row_lock_and_stale_snapshot_keep_sqlstate() -> anyhow::Result<()> {
    assert_postgres_failure(lock_unavailable(false).await?, *b"55P03");
    assert_postgres_failure(rejected_snapshot().await?, *b"40001");
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn manifest_real_postgres_table_lock_keeps_sqlstate() -> anyhow::Result<()> {
    assert_postgres_failure(lock_unavailable(true).await?, *b"55P03");
    Ok(())
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
            assert_eq!(failure.postgres_sqlstate, None);
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn manifest_constraint_text_and_unscoped_json_are_not_transient() -> anyhow::Result<()> {
    let error = rejected_statement().await?;
    let failure =
        classify_memory_manifest_failure(error.into(), MemoryManifestFailureStage::Active);
    assert!(!failure.retryable());
    assert_eq!(failure.postgres_sqlstate, Some(*b"23505"));
    for error in [
        anyhow::anyhow!("SQLSTATE 55P03 connection pool timed out"),
        anyhow::anyhow!("PostgreSQL backend failed: /private/path token=secret"),
        anyhow::Error::new(serde_json::from_str::<serde_json::Value>("{").unwrap_err()),
        anyhow::Error::new(pioneer_memory::MemoryWriteFailure::StorageTransient),
    ] {
        let failure = classify_memory_manifest_failure(
            error.context("load inventory"),
            MemoryManifestFailureStage::Candidates,
        );
        assert_eq!(failure.class, MemoryManifestFailureClass::Unclassified);
        assert!(!failure.retryable());
        assert_eq!(failure.postgres_sqlstate, None);
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
    Ok(())
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
        Some("55P03".into())
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
fn manifest_sqlstate_from_other_backend_is_not_postgres() {
    let error = DbErr::Query(RuntimeErr::SqlxError(std::sync::Arc::new(
        sea_orm::SqlxError::Database(Box::new(OtherDatabase)),
    )));
    let failure =
        classify_memory_manifest_failure(error.into(), MemoryManifestFailureStage::Active);
    assert_eq!(failure.class, MemoryManifestFailureClass::Unclassified);
    assert!(!failure.retryable());
    assert_eq!(failure.postgres_sqlstate, None);
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
