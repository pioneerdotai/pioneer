use super::{classify_native_preparation_error, confirm_owned_cancellation_lookup};
use crate::tests::postgres_failures::{lock_unavailable, rejected_snapshot, rejected_statement, sqlstate};
use sea_orm::DbErr;

fn assert_classification(error: DbErr, state: &str, expected: bool) {
    assert_eq!(sqlstate(&error), state);
    assert_eq!(classify_native_preparation_error(anyhow::Error::new(error.clone())).is_retryable(), expected);
    assert_eq!(classify_native_preparation_error(anyhow::Error::new(error).context("fixture write context")).is_retryable(), expected);
}

#[pioneer_postgres_test::postgres_test]
async fn actual_postgres_row_lock_unavailable_is_transient() -> anyhow::Result<()> {
    assert_classification(lock_unavailable(false).await?, "55P03", true);
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn actual_postgres_table_lock_unavailable_is_transient() -> anyhow::Result<()> {
    assert_classification(lock_unavailable(true).await?, "55P03", true);
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn actual_postgres_rejected_snapshot_is_transient() -> anyhow::Result<()> {
    assert_classification(rejected_snapshot().await?, "40001", true);
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn actual_postgres_constraint_and_matching_text_do_not_authorize_retries() -> anyhow::Result<()> {
    assert_classification(rejected_statement().await?, "23505", false);
    for text in ["55P03", "40001", "database is locked"] {
        assert_eq!(classify_native_preparation_error(anyhow::anyhow!(text)).is_retryable(), false);
        let wrapped = anyhow::Error::new(DbErr::Custom(text.into())).context("write context");
        assert_eq!(classify_native_preparation_error(wrapped).is_retryable(), false);
    }
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn receipt_lookup_error_keeps_its_typed_storage_reason() -> anyhow::Result<()> {
    use sea_orm::{ConnectionTrait, TransactionTrait};
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let owner = fixture.connect_with_limit(1).await?;
    let contender = fixture.connect_with_limit(1).await?;
    contender.execute_unprepared("SET lock_timeout = '100ms'").await?;
    let scoped = pioneer_sqlite::SqliteDatabase::from_single_connection(contender);
    let store = pioneer_crud::CrudStore::new(scoped.clone());
    let tx = owner.begin().await?;
    tx.execute_unprepared("LOCK TABLE native_cancellation_context IN ACCESS EXCLUSIVE MODE").await?;
    let result = store.native_cancellation_was_accepted_owned("turn", "owner").await;
    tx.rollback().await?;
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
    scoped.close().await?;
    owner.close().await?;
    Ok(())
}
