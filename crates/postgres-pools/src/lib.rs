use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

mod database;
mod query_scope;
mod reader;
mod statement_route;
pub use query_scope::DiagnosticStream;
pub use statement_route::MANAGED_SEARCH_PATH;
mod work_signal;
mod writer;
pub use work_signal::{WorkChange, WorkSignal, WorkSubscription, wait_for_unix_deadline};

pub use database::{PostgresDatabase, PostgresReadTransaction};
pub use reader::{
    DEFAULT_MAX_CONCURRENT_MAINTENANCE_READS, PostgresReadClass, PostgresReadEvent,
    PostgresReadObserver, PostgresReadOutcome,
};
pub use writer::{
    DEFAULT_MAX_CRITICAL_BURST, DEFAULT_MAX_MAINTENANCE_WAIT_MS, DEFAULT_MAX_NON_MAINTENANCE_BURST,
    PostgresQueryStream, PostgresTransaction, PostgresWriteClass, PostgresWriteEvent,
    PostgresWriteExecutor, PostgresWriteGrantReason, PostgresWriteObserver, PostgresWritePolicy,
    PostgresWriteQueueSnapshot,
};

/// Current admission state captured under its native owner lock. This is not
/// a milestone/histogram sample. Revision is local to that owner; time is the
/// shared process monotonic clock, milliseconds. Reader oldest remains absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PostgresAdmissionSnapshot {
    pub revision: u64,
    pub sampled_monotonic_ms: u64,
    pub consent_generation: u64,
    pub depth: usize,
    pub oldest_waited_ms: Option<u64>,
}
impl PostgresAdmissionSnapshot {
    fn capture(revision: u64, depth: usize, oldest: Option<std::time::Instant>) -> Self {
        let consent_generation = pioneer_observability::telemetry_consent_snapshot().1;
        let at = std::time::Instant::now();
        let sampled_monotonic_ms = pioneer_observability::work::clock_ms();
        Self {
            revision,
            sampled_monotonic_ms,
            consent_generation,
            depth,
            oldest_waited_ms: oldest.map(|head| {
                at.saturating_duration_since(head)
                    .as_millis()
                    .min(u64::MAX as u128) as u64
            }),
        }
    }
}

pub const DEFAULT_LOCK_RETRY_ATTEMPTS: usize = 5;
pub const DEFAULT_LOCK_RETRY_BASE_DELAY_MS: u64 = 20;

/// Run one observed database attempt, returning the original result.
pub async fn run_database_attempt<T, E, Fut>(operation: Fut) -> Result<T, E>
where
    Fut: Future<Output = Result<T, E>>,
{
    database_attempt_context(&pioneer_observability::work::capture(), 0)
        .scope_result(Box::pin(operation))
        .await
}

fn database_attempt_context(
    parent: &pioneer_observability::work::WorkCarrier,
    attempt: usize,
) -> pioneer_observability::work::WorkCarrier {
    parent
        .execution_child(
            pioneer_observability::work::WorkKind::Database,
            pioneer_observability::work::ExecutorRole::Foreign,
        )
        .with_attempt(attempt.saturating_add(1).min(u16::MAX as usize) as u16)
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
    let parent = pioneer_observability::work::capture();
    for attempt in 0..=retry_attempts {
        let carrier = database_attempt_context(&parent, attempt);
        let operation: Pin<Box<Fut>> = Box::pin(operation());
        match carrier.scope_result(operation).await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < retry_attempts && is_retryable(&error) => {
                let multiplier = 1u64 << attempt;
                let delay_ms = retry_base_delay
                    .as_millis()
                    .saturating_mul(multiplier as u128)
                    .min(u64::MAX as u128) as u64;
                pioneer_observability::live::retry(delay_ms);
                pioneer_observability::live::sleeping(
                    Duration::from_millis(delay_ms),
                    tokio::time::sleep(Duration::from_millis(delay_ms)),
                )
                .await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("database retry loop must return in all branches")
}

/// A transaction rejected by PostgreSQL must be retried from its beginning.
pub fn is_retryable_database_conflict(error: &anyhow::Error) -> bool {
    use sea_orm::{DbErr, RuntimeErr, sqlx};

    error.chain().any(|source| {
        let error = match source.downcast_ref::<DbErr>() {
            Some(DbErr::Exec(RuntimeErr::SqlxError(error)))
            | Some(DbErr::Query(RuntimeErr::SqlxError(error))) => Some(error.as_ref()),
            _ => source.downcast_ref::<sqlx::Error>(),
        };
        let Some(sqlx::Error::Database(error)) = error else {
            return false;
        };
        matches!(error.code().as_deref(), Some("40001" | "40P01"))
    })
}

/// Failures for which the operation can obtain a fresh PostgreSQL session
/// or repeat a transaction known to have rolled back.
pub fn is_anyhow_database_transient_access(error: &anyhow::Error) -> bool {
    use sea_orm::{ConnAcquireErr, DbErr, RuntimeErr, sqlx};

    error.chain().any(|source| {
        let error = match source.downcast_ref::<DbErr>() {
            Some(DbErr::ConnectionAcquire(ConnAcquireErr::Timeout)) => return true,
            Some(
                DbErr::Conn(RuntimeErr::SqlxError(error))
                | DbErr::Exec(RuntimeErr::SqlxError(error))
                | DbErr::Query(RuntimeErr::SqlxError(error)),
            ) => Some(error.as_ref()),
            _ => source.downcast_ref::<sqlx::Error>(),
        };
        match error {
            Some(sqlx::Error::PoolTimedOut) => true,
            Some(sqlx::Error::Database(error)) => matches!(
                error.code().as_deref(),
                Some("40001" | "40P01" | "55P03" | "08001" | "53300" | "57P03")
            ),
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_typed_storage_errors_authorize_retry() {
        let timeout = anyhow::Error::new(sea_orm::DbErr::ConnectionAcquire(
            sea_orm::ConnAcquireErr::Timeout,
        ))
        .context("failed to list active tasks");
        assert!(is_anyhow_database_transient_access(&timeout));
        assert!(!is_retryable_database_conflict(&timeout));
        for text in [
            "SQLSTATE 40001",
            "connection pool timed out",
            "database is locked",
        ] {
            let error = anyhow::anyhow!(text);
            assert!(!is_anyhow_database_transient_access(&error));
            assert!(!is_retryable_database_conflict(&error));
        }
    }
}

#[cfg(test)]
mod query_scope_tests;
