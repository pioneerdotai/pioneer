use super::*;
use sea_orm::{ConnectionTrait, DbErr};
use std::{borrow::Cow, sync::Arc};
use tracing_subscriber::prelude::*;

const OP: Operation = Operation::NativeFinalization;
const CANARY: &str = "/private/canary.db SELECT secret FROM users turn-canary task-canary thread-canary token-canary";

fn cantopen(code: i32) -> Diagnostic {
    Diagnostic {
        operation: OP,
        cause: Cause::SqliteCantOpen,
        sqlite_primary_code: Some(14),
        sqlite_code: Some(code),
    }
}
fn pool_error() -> anyhow::Error {
    DbErr::Query(RuntimeErr::SqlxError(Arc::new(SqlxError::PoolTimedOut))).into()
}
fn capture(f: impl FnOnce()) -> Vec<sentry::protocol::Event<'static>> {
    let subscriber =
        tracing_subscriber::registry().with(pioneer_observability::sentry_tracing_layer());
    sentry::test::with_captured_events(|| tracing::subscriber::with_default(subscriber, f))
}
fn fields<'a>(
    event: &'a sentry::protocol::Event<'_>,
) -> &'a std::collections::BTreeMap<String, serde_json::Value> {
    let sentry::protocol::Context::Other(fields) = &event.contexts["Rust Tracing Fields"] else {
        panic!("tracing fields missing");
    };
    fields
}

#[tokio::test]
async fn typed_sqlite_codes_survive_real_wrappers_and_context() {
    let (_directory, _manager, store, _) =
        crate::message::tests::setup_pooled_file_workspace_manager().await;
    let database = store.with_maintenance_access().database_connection();
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing").join("canary.db");
    // Use the project's writer executor, no ad hoc connection or writer permit.
    let error = database
        .execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            "ATTACH DATABASE ? AS diagnostic_fixture",
            [missing.to_string_lossy().as_ref().into()],
        ))
        .await
        .unwrap_err();
    let anyhow_error = anyhow::Error::new(error.clone())
        .context(CANARY)
        .context("outer context");
    let diagnostic = Diagnostic::extract(OP, &anyhow_error);
    assert_eq!(diagnostic, cantopen(14));
    let events = capture(|| {
        let mut reporter = Reporter::new(OP);
        let start = Instant::now();
        reporter.observe::<usize>(
            &Err(anyhow::Error::new(error.clone()).context(CANARY)),
            start,
        );
        reporter.observe::<usize>(
            &Err(anyhow::Error::new(error.clone()).context(CANARY)),
            start,
        );
        reporter.observe(&Ok(0usize), start);
        tracing::error!("safe neighbor canary checkpoint");
    });
    assert_eq!(events.len(), 2);
    assert_eq!(fields(&events[0])["sqlite_primary_code"], 14);
    assert_eq!(fields(&events[0])["sqlite_code"], 14);
    assert_eq!(fields(&events[0])["cause"], "sqlite_cantopen");
    let recovery = events[1]
        .breadcrumbs
        .iter()
        .find(|crumb| {
            crumb.message.as_deref() == Some("reconciliation recovered after observed failures")
        })
        .unwrap();
    assert_eq!(recovery.data["sqlite_primary_code"], 14);
    assert_eq!(recovery.data["sqlite_code"], 14);
    let serialized = serde_json::to_string(&events).unwrap();
    assert!(!serialized.contains(missing.to_string_lossy().as_ref()));
    for canary in CANARY.split_whitespace() {
        assert!(!serialized.contains(canary));
    }

    let runtime = match error {
        DbErr::Exec(runtime) | DbErr::Query(runtime) | DbErr::Conn(runtime) => runtime,
        other => panic!("expected SQLx runtime wrapper: {other:?}"),
    };
    for wrapper in [
        DbErr::Conn(runtime.clone()),
        DbErr::Exec(runtime.clone()),
        DbErr::Query(runtime.clone()),
    ] {
        assert_eq!(
            Diagnostic::extract(OP, &anyhow::Error::new(wrapper).context(CANARY)),
            cantopen(14)
        );
    }
    assert_eq!(Diagnostic::extract(OP, &runtime.into()), cantopen(14));
    database
        .execute_unprepared(
            "CREATE TABLE diagnostic_code_fixture (id INTEGER PRIMARY KEY, value TEXT UNIQUE)",
        )
        .await
        .unwrap();
    database
        .execute_unprepared("INSERT INTO diagnostic_code_fixture VALUES (1, 'canary')")
        .await
        .unwrap();
    let extended = database
        .execute_unprepared("INSERT INTO diagnostic_code_fixture VALUES (2, 'canary')")
        .await
        .unwrap_err();
    let extended = Diagnostic::extract(OP, &extended.into());
    assert_eq!(extended.sqlite_primary_code, Some(19));
    assert_eq!(extended.sqlite_code, Some(2067));
    assert_eq!(extended.cause, Cause::SqliteOther);
    assert!(!extended.cause.confirmed_storage());
}

#[derive(Debug)]
struct OtherBackend;
impl std::fmt::Display for OtherBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(CANARY)
    }
}
impl std::error::Error for OtherBackend {}
impl DatabaseError for OtherBackend {
    fn message(&self) -> &str {
        CANARY
    }
    fn code(&self) -> Option<Cow<'_, str>> {
        Some("14".into())
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
fn untyped_lost_and_other_backend_causes_stay_unclassified() {
    for error in [
        anyhow::anyhow!("SQLite code 14 unable to open database file {CANARY}"),
        anyhow::Error::new(DbErr::Custom("SQLITE_CANTOPEN".into())),
        anyhow::Error::new(DbErr::Query(RuntimeErr::Internal("code 14".into()))),
        anyhow::Error::new(DbErr::Exec(RuntimeErr::SqlxError(Arc::new(
            SqlxError::Database(Box::new(OtherBackend)),
        ))))
        .context(CANARY),
    ] {
        assert_eq!(
            Diagnostic::extract(OP, &error),
            Diagnostic::new(OP, Cause::Unclassified)
        );
    }
}

#[test]
fn typed_pool_sources_and_non_storage_errors() {
    for error in [
        anyhow::Error::new(DbErr::ConnectionAcquire(ConnAcquireErr::Timeout)),
        anyhow::Error::new(SqlxError::PoolTimedOut),
        anyhow::Error::new(RuntimeErr::SqlxError(Arc::new(SqlxError::PoolTimedOut))),
        pool_error(),
    ] {
        assert_eq!(
            Diagnostic::extract(OP, &error.context(CANARY)),
            Diagnostic::new(OP, Cause::PoolTimeout)
        );
    }
    let start = Instant::now();
    let mut reporter = Reporter::new(OP);
    let other = Diagnostic {
        operation: OP,
        cause: Cause::SqliteOther,
        sqlite_primary_code: Some(19),
        sqlite_code: Some(2067),
    };
    for _ in 0..2 {
        assert!(reporter.failure(other, start).is_some());
    }
    assert_eq!(reporter.success(start).unwrap().suppressed, 0);
}

#[test]
fn first_boundary_and_continuous_failure_summaries() {
    let start = Instant::now();
    let mut reporter = Reporter::new(OP);
    let first = reporter.failure(cantopen(14), start).unwrap();
    assert_eq!((first.failures, first.suppressed), (1, 0));
    for seconds in [1, 2, 599] {
        assert!(
            reporter
                .failure(cantopen(14), start + Duration::from_secs(seconds))
                .is_none()
        );
    }
    let next = reporter
        .failure(cantopen(14), start + REPORT_WINDOW)
        .unwrap();
    assert_eq!(
        (next.failures, next.suppressed, next.suppressed_since_report),
        (5, 3, 3)
    );
    assert!(
        reporter
            .failure(cantopen(14), start + REPORT_WINDOW + Duration::from_secs(1))
            .is_none()
    );
    let next = reporter
        .failure(cantopen(14), start + REPORT_WINDOW * 2)
        .unwrap();
    assert_eq!(
        (next.failures, next.suppressed, next.suppressed_since_report),
        (7, 4, 1)
    );
    let recovery = reporter.success(start + REPORT_WINDOW * 2).unwrap();
    assert_eq!(
        (
            recovery.failures,
            recovery.suppressed,
            recovery.suppressed_since_report
        ),
        (7, 4, 0)
    );
    assert_eq!(recovery.elapsed, REPORT_WINDOW * 2);
    assert!(reporter.success(start + REPORT_WINDOW * 2).is_none());
}

#[test]
fn recovery_preserves_window_and_new_episode_counts() {
    let start = Instant::now();
    let mut reporter = Reporter::new(OP);
    assert!(reporter.failure(cantopen(14), start).is_some());
    assert!(reporter.success(start + Duration::from_secs(1)).is_some());
    assert!(
        reporter
            .failure(cantopen(14), start + Duration::from_secs(2))
            .is_none()
    );
    let recovery = reporter.success(start + Duration::from_secs(3)).unwrap();
    assert_eq!(
        (
            recovery.failures,
            recovery.suppressed,
            recovery.suppressed_since_report
        ),
        (1, 1, 1)
    );
    assert_eq!(recovery.elapsed, Duration::from_secs(1));
    assert!(
        reporter
            .failure(cantopen(14), start + REPORT_WINDOW)
            .is_some()
    );
}

#[test]
fn cause_changes_unknown_errors_and_operations_are_independent() {
    let start = Instant::now();
    let mut reporter = Reporter::new(OP);
    let mut other = Reporter::new(Operation::TaskRunOccurrence);
    assert!(reporter.failure(cantopen(14), start).is_some());
    other.observe::<usize>(&Err(pool_error()), start);
    other.observe(&Ok(0usize), start);
    assert!(reporter.episode.is_some());
    for _ in 0..2 {
        assert!(
            reporter
                .failure(Diagnostic::new(OP, Cause::Unclassified), start)
                .is_some()
        );
        assert!(reporter.episode.is_some());
    }
    assert!(reporter.failure(cantopen(14), start).is_none());
    assert!(reporter.failure(cantopen(270), start).is_some());
    assert!(reporter.failure(cantopen(526), start).is_some());
    assert!(
        reporter
            .failure(Diagnostic::new(OP, Cause::PoolTimeout), start)
            .is_some()
    );
    assert_eq!(reporter.success(start).unwrap().failures, 7);
}

#[test]
fn counters_saturate_without_consuming_episode_history() {
    let start = Instant::now();
    let mut reporter = Reporter::new(OP);
    reporter.failure(cantopen(14), start).unwrap();
    let episode = reporter.episode.as_mut().unwrap();
    episode.failures = u64::MAX;
    episode.suppressed = u64::MAX;
    reporter.suppressed_since_report = u64::MAX;
    assert!(reporter.failure(cantopen(14), start).is_none());
    let next = reporter
        .failure(cantopen(14), start + REPORT_WINDOW)
        .unwrap();
    assert_eq!(
        (next.failures, next.suppressed, next.suppressed_since_report),
        (u64::MAX, u64::MAX, u64::MAX)
    );
    let recovery = reporter.success(start + REPORT_WINDOW).unwrap();
    assert_eq!(
        (
            recovery.failures,
            recovery.suppressed,
            recovery.suppressed_since_report
        ),
        (u64::MAX, u64::MAX, 0)
    );
    assert!(reporter.episode.is_none());
}

#[test]
fn production_mapper_captures_periodic_errors_recovery_and_safe_canaries() {
    let start = Instant::now();
    let mut reporter = Reporter::new(OP);
    let error = pool_error().context(CANARY);
    let events = capture(|| {
        reporter.observe::<usize>(&Err(error), start);
        reporter.observe::<usize>(
            &Err(pool_error().context(CANARY)),
            start + Duration::from_secs(1),
        );
        reporter.observe::<usize>(&Err(pool_error().context(CANARY)), start + REPORT_WINDOW);
        reporter.observe(&Ok(0usize), start + REPORT_WINDOW + Duration::from_secs(1));
        reporter.observe(&Ok(0usize), start + REPORT_WINDOW + Duration::from_secs(2));
        reporter.observe::<usize>(
            &Err(anyhow::anyhow!(CANARY)),
            start + REPORT_WINDOW + Duration::from_secs(3),
        );
        tracing::error!("neighbor operation failed");
    });
    assert_eq!(events.len(), 4);
    assert_eq!(fields(&events[0])["cause"], "pool_timeout");
    assert_eq!(fields(&events[1])["final_failures"], 3);
    assert_eq!(fields(&events[1])["suppressed_messages"], 1);
    assert_eq!(fields(&events[2])["cause"], "unclassified");
    let recovery: Vec<_> = events[3]
        .breadcrumbs
        .iter()
        .filter(|crumb| {
            crumb.message.as_deref() == Some("reconciliation recovered after observed failures")
        })
        .collect();
    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery[0].data["final_failures"], 3);
    assert_eq!(recovery[0].data["suppressed_messages"], 1);
    let serialized = serde_json::to_string(&events).unwrap();
    for canary in CANARY.split_whitespace() {
        assert!(!serialized.contains(canary));
    }
}
