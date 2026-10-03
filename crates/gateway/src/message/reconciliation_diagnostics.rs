//! Reporting only: no database handles, retry decisions, or error strings live here.
use sea_orm::sqlx::error::DatabaseError;
use sea_orm::{ConnAcquireErr, RuntimeErr, SqlxError, SqlxSqliteError};
use std::time::{Duration, Instant};

const REPORT_WINDOW: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Operation {
    NativeFinalization,
    TaskRunOccurrence,
}
impl Operation {
    fn as_str(self) -> &'static str {
        match self {
            Self::NativeFinalization => "native_turn_finalization",
            Self::TaskRunOccurrence => "task_run_occurrence",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cause {
    Unclassified,
    MissingDependency,
    SqliteCantOpen,
    SqliteBusy,
    SqliteLocked,
    SqliteIo,
    SqliteFull,
    SqliteReadOnly,
    SqliteCorrupt,
    SqliteNotADatabase,
    SqliteOther,
    PoolTimeout,
    PoolClosed,
}
impl Cause {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unclassified => "unclassified",
            Self::MissingDependency => "missing_dependency",
            Self::SqliteCantOpen => "sqlite_cantopen",
            Self::SqliteBusy => "sqlite_busy",
            Self::SqliteLocked => "sqlite_locked",
            Self::SqliteIo => "sqlite_io",
            Self::SqliteFull => "sqlite_full",
            Self::SqliteReadOnly => "sqlite_readonly",
            Self::SqliteCorrupt => "sqlite_corrupt",
            Self::SqliteNotADatabase => "sqlite_notadb",
            Self::SqliteOther => "sqlite_other",
            Self::PoolTimeout => "pool_timeout",
            Self::PoolClosed => "pool_closed",
        }
    }
    fn confirmed_storage(self) -> bool {
        !matches!(
            self,
            Self::Unclassified | Self::SqliteOther | Self::MissingDependency
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Diagnostic {
    operation: Operation,
    cause: Cause,
    sqlite_primary_code: Option<i32>,
    sqlite_code: Option<i32>,
}
impl Diagnostic {
    fn new(operation: Operation, cause: Cause) -> Self {
        Self {
            operation,
            cause,
            sqlite_primary_code: None,
            sqlite_code: None,
        }
    }
    fn sqlite(operation: Operation, error: &SqlxSqliteError) -> Self {
        // Parse only the typed SQLite driver's numeric code, never Display/Debug.
        let code = error
            .code()
            .and_then(|code| code.parse::<i32>().ok())
            .filter(|code| *code > 0);
        let primary = code.map(|code| code & 0xff);
        let cause = match primary {
            Some(14) => Cause::SqliteCantOpen,
            Some(5) => Cause::SqliteBusy,
            Some(6) => Cause::SqliteLocked,
            Some(10) => Cause::SqliteIo,
            Some(13) => Cause::SqliteFull,
            Some(8) => Cause::SqliteReadOnly,
            Some(11) => Cause::SqliteCorrupt,
            Some(26) => Cause::SqliteNotADatabase,
            _ => Cause::SqliteOther,
        };
        Self {
            operation,
            cause,
            sqlite_primary_code: primary,
            sqlite_code: code,
        }
    }
    fn sqlx(operation: Operation, error: &SqlxError) -> Option<Self> {
        match error {
            SqlxError::Database(database) => database
                .try_downcast_ref::<SqlxSqliteError>()
                .map(|error| Self::sqlite(operation, error)),
            SqlxError::PoolTimedOut => Some(Self::new(operation, Cause::PoolTimeout)),
            SqlxError::PoolClosed => Some(Self::new(operation, Cause::PoolClosed)),
            _ => None,
        }
    }
    fn extract(operation: Operation, error: &anyhow::Error) -> Self {
        for source in error.chain() {
            // Pinned SeaORM RuntimeErr wraps an Arc without exposing it as source.
            if let Some(RuntimeErr::SqlxError(error)) = source.downcast_ref::<RuntimeErr>()
                && let Some(diagnostic) = Self::sqlx(operation, error.as_ref())
            {
                return diagnostic;
            }
            if let Some(error) = source.downcast_ref::<SqlxError>()
                && let Some(diagnostic) = Self::sqlx(operation, error)
            {
                return diagnostic;
            }
            if let Some(error) = source.downcast_ref::<SqlxSqliteError>() {
                return Self::sqlite(operation, error);
            }
            if let Some(error) = source.downcast_ref::<ConnAcquireErr>() {
                return Self::new(
                    operation,
                    match error {
                        ConnAcquireErr::Timeout => Cause::PoolTimeout,
                        ConnAcquireErr::ConnectionClosed => Cause::PoolClosed,
                    },
                );
            }
        }
        Self::new(operation, Cause::Unclassified)
    }
}

pub(super) fn is_storage_failure(error: &anyhow::Error) -> bool {
    Diagnostic::extract(Operation::TaskRunOccurrence, error)
        .cause
        .confirmed_storage()
}

#[derive(Clone, Copy, Debug)]
struct Episode {
    started: Instant,
    failures: u64,
    suppressed: u64,
    last: Diagnostic,
}

/// Owned by each worker outside its loop, including panic containment. Fixed
/// size: one episode and the last permitted storage cause. Identity is operation
/// + cause + primary code + full received code. A changed confirmed cause emits
/// immediately and replaces the remembered cause. Unknown/non-storage errors
/// and success retain it. No mutex: only the owning worker updates this state.
pub(super) struct Reporter {
    operation: Operation,
    last_storage_report: Option<(Diagnostic, Instant)>,
    episode: Option<Episode>,
    suppressed_since_report: u64,
}
#[derive(Clone, Copy, Debug)]
struct Snapshot {
    diagnostic: Diagnostic,
    failures: u64,
    suppressed: u64,
    suppressed_since_report: u64,
    elapsed: Duration,
}
impl Reporter {
    pub(super) fn new(operation: Operation) -> Self {
        Self {
            operation,
            last_storage_report: None,
            episode: None,
            suppressed_since_report: 0,
        }
    }
    /// Observe only the final result of the entire reconciliation after retries.
    /// Ok(0) recovers; partial work followed by Err does not.
    pub(super) fn observe<T>(&mut self, result: &anyhow::Result<T>, now: Instant) {
        match result {
            Err(error) => {
                let diagnostic = Diagnostic::extract(self.operation, error);
                if let Some(snapshot) = self.failure(diagnostic, now) {
                    snapshot.emit(false);
                }
            }
            Ok(_) => {
                if let Some(snapshot) = self.success(now) {
                    snapshot.emit(true);
                }
            }
        }
    }
    /// Only an observed empty pending set closes an occurrence episode. Idle
    /// backoff passes neither add failures nor recover. The empty observation
    /// is a point-in-time fact; later source writes can start a new episode.
    pub(super) fn observe_occurrences(
        &mut self,
        result: &anyhow::Result<super::tasks::TaskRunOccurrenceReconcileSummary>,
        now: Instant,
    ) {
        match result {
            Ok(summary) if summary.first_error.is_some() => {
                let diagnostic =
                    Diagnostic::extract(self.operation, summary.first_error.as_ref().unwrap());
                if let Some(snapshot) = self.failure(diagnostic, now) {
                    snapshot.emit(false);
                }
            }
            Ok(summary) if summary.unresolved > 0 => {
                let diagnostic = Diagnostic::new(self.operation, Cause::MissingDependency);
                if let Some(snapshot) = self.failure(diagnostic, now) {
                    snapshot.emit(false);
                }
            }
            Ok(summary) if summary.pending == Some(false) => self.observe(result, now),
            Ok(_) => {}
            Err(_) => self.observe(result, now),
        }
    }
    fn failure(&mut self, diagnostic: Diagnostic, now: Instant) -> Option<Snapshot> {
        let episode = self.episode.get_or_insert(Episode {
            started: now,
            failures: 0,
            suppressed: 0,
            last: diagnostic,
        });
        episode.failures = episode.failures.saturating_add(1);
        episode.last = diagnostic;
        let suppressed = diagnostic.cause.confirmed_storage()
            && self.last_storage_report.is_some_and(|(last, at)| {
                last == diagnostic && now.saturating_duration_since(at) < REPORT_WINDOW
            });
        if suppressed {
            episode.suppressed = episode.suppressed.saturating_add(1);
            self.suppressed_since_report = self.suppressed_since_report.saturating_add(1);
            return None;
        }
        if diagnostic.cause.confirmed_storage() {
            self.last_storage_report = Some((diagnostic, now));
        }
        let snapshot = Self::snapshot(*episode, self.suppressed_since_report, now);
        // Delta resets on emitted ERROR or recovery INFO. Episode totals reset
        // only on recovery, so ERROR summaries never consume that history.
        self.suppressed_since_report = 0;
        Some(snapshot)
    }
    fn success(&mut self, now: Instant) -> Option<Snapshot> {
        let episode = self.episode.take()?;
        let snapshot = Self::snapshot(episode, self.suppressed_since_report, now);
        self.suppressed_since_report = 0;
        Some(snapshot)
    }
    fn snapshot(episode: Episode, delta: u64, now: Instant) -> Snapshot {
        Snapshot {
            diagnostic: episode.last,
            failures: episode.failures,
            suppressed: episode.suppressed,
            suppressed_since_report: delta,
            elapsed: now.saturating_duration_since(episode.started),
        }
    }
}
impl Snapshot {
    fn emit(self, recovered: bool) {
        let diagnostic = self.diagnostic;
        // Allowlisted fields only; duration describes the observed episode,
        // not proven file/database unavailability.
        macro_rules! report {
            ($level:ident, $message:literal) => {
                tracing::$level!(
                    operation = diagnostic.operation.as_str(),
                    cause = diagnostic.cause.as_str(),
                    sqlite_primary_code = diagnostic.sqlite_primary_code,
                    sqlite_code = diagnostic.sqlite_code,
                    final_failures = self.failures,
                    suppressed_messages = self.suppressed,
                    suppressed_since_report = self.suppressed_since_report,
                    observed_episode_ms = self.elapsed.as_millis().min(u64::MAX as u128) as u64,
                    $message
                );
            };
        }
        if recovered {
            report!(info, "reconciliation recovered after observed failures");
        } else {
            match diagnostic.operation {
                Operation::NativeFinalization => {
                    report!(error, "native Turn finalization reconciler failed");
                }
                Operation::TaskRunOccurrence => {
                    report!(error, "task parent occurrence reconciler failed");
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/reconciliation_diagnostics.rs"]
mod tests;
