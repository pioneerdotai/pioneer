use super::classify_memory_write_failure;
use crate::tests::postgres_failures::{
    lock_unavailable, rejected_snapshot, rejected_statement, sqlstate,
};
use pioneer_memory::MemoryWriteFailure;
use sea_orm::DbErr;

fn assert_classification(error: DbErr, state: &str, expected: MemoryWriteFailure) {
    assert_eq!(sqlstate(&error), state);
    assert_eq!(
        classify_memory_write_failure(anyhow::Error::new(error.clone())),
        expected
    );
    assert_eq!(
        classify_memory_write_failure(anyhow::Error::new(error).context("fixture write context")),
        expected
    );
}

#[pioneer_postgres_test::postgres_test]
async fn actual_postgres_row_lock_unavailable_is_transient() -> anyhow::Result<()> {
    assert_classification(
        lock_unavailable(false).await?,
        "55P03",
        MemoryWriteFailure::StorageTransient,
    );
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn actual_postgres_table_lock_unavailable_is_transient() -> anyhow::Result<()> {
    assert_classification(
        lock_unavailable(true).await?,
        "55P03",
        MemoryWriteFailure::StorageTransient,
    );
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn actual_postgres_rejected_snapshot_is_transient() -> anyhow::Result<()> {
    assert_classification(
        rejected_snapshot().await?,
        "40001",
        MemoryWriteFailure::StorageTransient,
    );
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn actual_postgres_constraint_and_matching_text_do_not_authorize_retries()
-> anyhow::Result<()> {
    assert_classification(
        rejected_statement().await?,
        "23505",
        MemoryWriteFailure::Unclassified,
    );
    for text in ["55P03", "40001", "database is locked"] {
        assert_eq!(
            classify_memory_write_failure(anyhow::anyhow!(text)),
            MemoryWriteFailure::Unclassified
        );
        let wrapped = anyhow::Error::new(DbErr::Custom(text.into())).context("write context");
        assert_eq!(
            classify_memory_write_failure(wrapped),
            MemoryWriteFailure::Unclassified
        );
    }
    Ok(())
}
