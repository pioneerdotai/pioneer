use anyhow::{Context, Result, bail};
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use std::future::Future;
use std::path::{Component, Path};
use std::pin::Pin;
use std::time::Duration;

mod database;
mod reader;
mod writer;

pub use database::{SqliteDatabase, SqliteReadTransaction};
pub use reader::{
    DEFAULT_MAX_CONCURRENT_MAINTENANCE_READS, SqliteReadClass, SqliteReadEvent, SqliteReadObserver,
    SqliteReadOutcome,
};
pub use writer::{
    DEFAULT_MAX_CRITICAL_BURST, DEFAULT_MAX_MAINTENANCE_WAIT_MS, DEFAULT_MAX_NON_MAINTENANCE_BURST,
    SqliteQueryStream, SqliteTransaction, SqliteWriteClass, SqliteWriteEvent, SqliteWriteExecutor,
    SqliteWriteGrantReason, SqliteWriteObserver, SqliteWritePolicy, SqliteWriteQueueSnapshot,
};

pub mod zstd;

pub const DEFAULT_LOCK_RETRY_ATTEMPTS: usize = 5;
pub const DEFAULT_LOCK_RETRY_BASE_DELAY_MS: u64 = 20;
pub const DEFAULT_SQLITE_BUSY_TIMEOUT_MS: u64 = 5000;

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

pub async fn apply_sqlite_pragmas<C>(connection: &C) -> Result<()>
where
    C: ConnectionTrait + Sync,
{
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
    sqlite_connection_url_with_mode(path, "rwc")
}

pub fn sqlite_read_only_connection_url(path: &Path) -> String {
    sqlite_connection_url_with_mode(path, "ro")
}

fn sqlite_connection_url_with_mode(path: &Path, mode: &str) -> String {
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

    format!("sqlite://{path_part}?mode={mode}")
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
        is_sqlite_lock_message, is_sqlite_pool_timeout_message, is_sqlite_transient_open_message,
    };

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
}
