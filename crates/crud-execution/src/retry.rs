use anyhow::Result;
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, is_retryable_database_conflict,
    retry_with_backoff,
};
use std::{future::Future, time::Duration};

pub(crate) async fn retry_write_on_conflict<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    retry_with_backoff(
        operation,
        is_retryable_database_conflict,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}
