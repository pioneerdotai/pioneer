use anyhow::{Context, Result, bail};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use std::future::Future;
use std::path::{Component, Path};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub mod zstd;

pub const DEFAULT_LOCK_RETRY_ATTEMPTS: usize = 5;
pub const DEFAULT_LOCK_RETRY_BASE_DELAY_MS: u64 = 20;
pub const DEFAULT_SQLITE_BUSY_TIMEOUT_MS: u64 = 5000;

#[derive(Clone)]
pub struct SqliteWriteCoordinator {
    lock: Arc<Mutex<()>>,
    retry_attempts: usize,
    retry_base_delay: Duration,
}

impl Default for SqliteWriteCoordinator {
    fn default() -> Self {
        Self::new(
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
    }
}

impl SqliteWriteCoordinator {
    pub fn new(retry_attempts: usize, retry_base_delay: Duration) -> Self {
        Self {
            lock: Arc::new(Mutex::new(())),
            retry_attempts,
            retry_base_delay,
        }
    }

    pub async fn run_serialized_with_retry<T, E, F, Fut, P>(
        &self,
        operation: F,
        is_retryable: P,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        P: Fn(&E) -> bool,
    {
        let _write_guard = self.lock.lock().await;
        retry_with_backoff(
            operation,
            is_retryable,
            self.retry_attempts,
            self.retry_base_delay,
        )
        .await
    }

    pub async fn try_run_serialized_with_retry<T, E, F, Fut, P>(
        &self,
        operation: F,
        is_retryable: P,
    ) -> Option<Result<T, E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        P: Fn(&E) -> bool,
    {
        let Ok(_write_guard) = self.lock.try_lock() else {
            return None;
        };
        Some(
            retry_with_backoff(
                operation,
                is_retryable,
                self.retry_attempts,
                self.retry_base_delay,
            )
            .await,
        )
    }
}

pub async fn retry_with_backoff<T, E, F, Fut, P>(
    mut operation: F,
    is_retryable: P,
    retry_attempts: usize,
    retry_base_delay: Duration,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: Fn(&E) -> bool,
{
    for attempt in 0..=retry_attempts {
        let operation: Pin<Box<Fut>> = Box::pin(operation());
        match operation.await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < retry_attempts && is_retryable(&error) => {
                let multiplier = 1u64 << attempt;
                let delay_ms = retry_base_delay
                    .as_millis()
                    .saturating_mul(multiplier as u128)
                    .min(u64::MAX as u128) as u64;
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("sqlite retry loop must return in all branches")
}

pub fn is_sqlite_lock_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    (normalized.contains("database") && normalized.contains("locked"))
        || (normalized.contains("database") && normalized.contains("busy"))
        || normalized.contains("sqlite_busy")
}

pub fn is_anyhow_sqlite_lock(error: &anyhow::Error) -> bool {
    is_sqlite_lock_message(format!("{error:#}").as_str())
}

pub fn is_sqlite_transient_open_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("sqlite_cantopen") || normalized.contains("unable to open database file")
}

pub fn is_anyhow_sqlite_transient_open(error: &anyhow::Error) -> bool {
    is_sqlite_transient_open_message(format!("{error:#}").as_str())
}

pub fn is_sqlite_pool_timeout_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("connection pool timed out")
        || normalized.contains("failed to acquire connection from pool")
}

pub fn is_anyhow_sqlite_pool_timeout(error: &anyhow::Error) -> bool {
    is_sqlite_pool_timeout_message(format!("{error:#}").as_str())
}

pub fn is_anyhow_sqlite_transient_access(error: &anyhow::Error) -> bool {
    is_anyhow_sqlite_transient_open(error) || is_anyhow_sqlite_pool_timeout(error)
}

pub async fn apply_sqlite_pragmas(connection: &DatabaseConnection) -> Result<()> {
    connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA journal_mode = WAL",
        ))
        .await
        .context("failed to set sqlite PRAGMA journal_mode = WAL")?;

    connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            format!("PRAGMA busy_timeout = {DEFAULT_SQLITE_BUSY_TIMEOUT_MS}"),
        ))
        .await
        .context("failed to set sqlite PRAGMA busy_timeout")?;

    connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA foreign_keys = OFF",
        ))
        .await
        .context("failed to set sqlite PRAGMA foreign_keys = OFF")?;

    Ok(())
}

pub fn sqlite_connection_url(path: &Path) -> String {
    let normalized_path = path.to_string_lossy().replace('\\', "/");
    let has_windows_drive_prefix = normalized_path
        .as_bytes()
        .get(1)
        .is_some_and(|value| *value == b':');
    let needs_leading_slash =
        (path.is_absolute() || has_windows_drive_prefix) && !normalized_path.starts_with('/');

    let path_part = if needs_leading_slash {
        format!("/{normalized_path}")
    } else {
        normalized_path
    };

    format!("sqlite://{path_part}?mode=rwc")
}

pub fn normalize_relative_database_file_name(value: &str, field_name: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{field_name} must not be empty");
    }

    let path = Path::new(trimmed);

    if path.is_absolute() {
        bail!("{field_name} must be relative");
    }

    if path.components().any(is_disallowed_component) {
        bail!("{field_name} must not contain parent or root components");
    }

    Ok(trimmed.to_owned())
}

fn is_disallowed_component(component: Component<'_>) -> bool {
    matches!(
        component,
        Component::ParentDir | Component::RootDir | Component::Prefix(_)
    )
}

#[cfg(test)]
mod tests {
    use super::{
        SqliteWriteCoordinator, is_sqlite_lock_message, is_sqlite_pool_timeout_message,
        is_sqlite_transient_open_message,
    };
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[test]
    fn detects_sqlite_transient_open_errors() {
        assert!(is_sqlite_transient_open_message(
            "Query Error: error returned from database: (code: 14) unable to open database file"
        ));
        assert!(is_sqlite_transient_open_message("SQLITE_CANTOPEN"));
    }

    #[test]
    fn transient_open_errors_do_not_match_lock_only_predicate() {
        assert!(!is_sqlite_lock_message(
            "Query Error: error returned from database: (code: 14) unable to open database file"
        ));
    }

    #[test]
    fn detects_sqlite_pool_acquire_timeouts() {
        assert!(is_sqlite_pool_timeout_message(
            "failed to query expired running attempts: Failed to acquire connection from pool: Connection pool timed out: Connection pool timed out",
        ));
    }

    #[test]
    fn pool_acquire_timeouts_do_not_match_lock_only_predicate() {
        assert!(!is_sqlite_lock_message(
            "Failed to acquire connection from pool: Connection pool timed out"
        ));
    }

    #[tokio::test]
    async fn low_priority_write_skips_while_foreground_write_is_active() {
        let coordinator = SqliteWriteCoordinator::default();
        let foreground_coordinator = coordinator.clone();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let foreground = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                foreground_coordinator
                    .run_serialized_with_retry(
                        || async {
                            entered.notify_one();
                            release.notified().await;
                            Ok::<_, ()>(())
                        },
                        |_| false,
                    )
                    .await
            }
        });

        entered.notified().await;
        let skipped = coordinator
            .try_run_serialized_with_retry(|| async { Ok::<_, ()>(()) }, |_| false)
            .await;
        assert!(skipped.is_none());

        release.notify_one();
        foreground
            .await
            .expect("foreground task should join")
            .expect("foreground write should succeed");

        let completed = coordinator
            .try_run_serialized_with_retry(|| async { Ok::<_, ()>(42) }, |_| false)
            .await;
        assert_eq!(completed, Some(Ok(42)));
    }
}
