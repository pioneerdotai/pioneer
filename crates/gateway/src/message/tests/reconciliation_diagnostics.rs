use super::*;
use sea_orm::{ConnectionTrait, DbErr};
use std::{borrow::Cow, sync::Arc};
use tracing_subscriber::prelude::*;

const OP: Operation = Operation::NativeFinalization;
const CANARY: &str = "/private/canary.db SELECT secret FROM users turn-canary task-canary thread-canary token-canary";

fn connection_failure(code: &[u8; 5]) -> Diagnostic {
    Diagnostic {
        sqlstate: Some(*code),
        ..Diagnostic::new(OP, Cause::PostgresConnection)
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

#[pioneer_postgres_test::postgres_test]
async fn typed_postgres_errors_survive_real_wrappers_and_context() -> anyhow::Result<()> {
    use crate::tests::postgres_failures::{lock_unavailable, rejected_statement};
    let error = lock_unavailable(false).await?;
    let expected = Diagnostic {
        sqlstate: Some(*b"55P03"),
        ..Diagnostic::new(OP, Cause::PostgresContention)
    };
    let anyhow_error = anyhow::Error::new(error.clone())
        .context(CANARY)
        .context("outer context");
    let diagnostic = Diagnostic::extract(OP, &anyhow_error);
    assert_eq!(diagnostic, expected);
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
    assert_eq!(fields(&events[0])["cause"], "postgres_contention");
    assert_eq!(fields(&events[0])["sqlstate"], "55P03");
    let recovery = events[1]
        .breadcrumbs
        .iter()
        .find(|crumb| {
            crumb.message.as_deref() == Some("reconciliation recovered after observed failures")
        })
        .unwrap();
    assert_eq!(recovery.data["cause"], "postgres_contention");
    let serialized = serde_json::to_string(&events).unwrap();
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
            expected
        );
    }
    assert_eq!(Diagnostic::extract(OP, &runtime.into()), expected);
    let constraint = Diagnostic::extract(OP, &rejected_statement().await?.into());
    assert_eq!(constraint.cause, Cause::Unclassified);
    assert!(!constraint.cause.confirmed_storage());
    Ok(())
}

#[tokio::test]
async fn postgres_contention_is_classified_from_the_driver_error() -> anyhow::Result<()> {
    use sea_orm::TransactionTrait;

    pioneer_postgres_test::with_database(async |fixture| {
        let writer = fixture.connection();
        writer
            .execute_unprepared(
                "CREATE TABLE diagnostic_lock_fixture (id int PRIMARY KEY);
             INSERT INTO diagnostic_lock_fixture VALUES (1)",
            )
            .await?;
        let lock = writer.begin().await?;
        lock.execute_unprepared("SELECT id FROM diagnostic_lock_fixture WHERE id = 1 FOR UPDATE")
            .await?;
        let contender = fixture.connect().await?;
        let error = contender
            .execute_unprepared(
                "SELECT id FROM diagnostic_lock_fixture WHERE id = 1 FOR UPDATE NOWAIT",
            )
            .await
            .expect_err("another PostgreSQL session holds the row lock");
        lock.rollback().await?;

        let error = anyhow::Error::new(error).context(CANARY);
        let diagnostic = Diagnostic::extract(OP, &error);
        assert_eq!(diagnostic.cause, Cause::PostgresContention);
        assert!(is_storage_failure(&error));
        assert_eq!(diagnostic.sqlstate, Some(*b"55P03"));
        assert!(!is_storage_failure(&anyhow::anyhow!("SQLSTATE 55P03")));
        Ok(())
    })
    .await
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
        sqlstate: Some(*b"23505"),
        ..Diagnostic::new(OP, Cause::Unclassified)
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
    let first = reporter
        .failure(connection_failure(b"08001"), start)
        .unwrap();
    assert_eq!((first.failures, first.suppressed), (1, 0));
    for seconds in [1, 2, 599] {
        assert!(
            reporter
                .failure(
                    connection_failure(b"08001"),
                    start + Duration::from_secs(seconds)
                )
                .is_none()
        );
    }
    let next = reporter
        .failure(connection_failure(b"08001"), start + REPORT_WINDOW)
        .unwrap();
    assert_eq!(
        (next.failures, next.suppressed, next.suppressed_since_report),
        (5, 3, 3)
    );
    assert!(
        reporter
            .failure(
                connection_failure(b"08001"),
                start + REPORT_WINDOW + Duration::from_secs(1)
            )
            .is_none()
    );
    let next = reporter
        .failure(connection_failure(b"08001"), start + REPORT_WINDOW * 2)
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
    assert!(
        reporter
            .failure(connection_failure(b"08001"), start)
            .is_some()
    );
    assert!(reporter.success(start + Duration::from_secs(1)).is_some());
    assert!(
        reporter
            .failure(connection_failure(b"08001"), start + Duration::from_secs(2))
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
            .failure(connection_failure(b"08001"), start + REPORT_WINDOW)
            .is_some()
    );
}

#[test]
fn cause_changes_unknown_errors_and_operations_are_independent() {
    let start = Instant::now();
    let mut reporter = Reporter::new(OP);
    let mut other = Reporter::new(Operation::TaskRunOccurrence);
    assert!(
        reporter
            .failure(connection_failure(b"08001"), start)
            .is_some()
    );
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
    assert!(
        reporter
            .failure(connection_failure(b"08001"), start)
            .is_none()
    );
    assert!(
        reporter
            .failure(connection_failure(b"08006"), start)
            .is_some()
    );
    assert!(
        reporter
            .failure(connection_failure(b"57P01"), start)
            .is_some()
    );
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
    reporter
        .failure(connection_failure(b"08001"), start)
        .unwrap();
    let episode = reporter.episode.as_mut().unwrap();
    episode.failures = u64::MAX;
    episode.suppressed = u64::MAX;
    reporter.suppressed_since_report = u64::MAX;
    assert!(
        reporter
            .failure(connection_failure(b"08001"), start)
            .is_none()
    );
    let next = reporter
        .failure(connection_failure(b"08001"), start + REPORT_WINDOW)
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

#[test]
fn occurrence_repair_phase_and_reason_survive_storage_source_classification() {
    let error = pool_error()
        .context(pioneer_crud::TaskRunOccurrenceRepairReason::TerminalEventPreparation)
        .context(pioneer_crud::TaskRunOccurrenceRepairPhase::Preparation)
        .context(CANARY);
    let diagnostic = Diagnostic::extract(Operation::TaskRunOccurrence, &error);
    assert_eq!(diagnostic.cause, Cause::PoolTimeout);
    assert_eq!(diagnostic.phase, Some("repair_preparation"));
    assert_eq!(diagnostic.reason, Some("terminal_event_preparation"));
    let events = capture(|| {
        Reporter::new(Operation::TaskRunOccurrence).observe::<usize>(&Err(error), Instant::now())
    });
    assert_eq!(fields(&events[0])["phase"], "repair_preparation");
    assert_eq!(fields(&events[0])["reason"], "terminal_event_preparation");
    assert!(!serde_json::to_string(&events).unwrap().contains("canary"));
}

#[test]
fn finalization_operation_survives_typed_pool_cause() {
    let error = pool_error()
        .context(pioneer_crud::NativeFinalizationPhase::WriterBegin)
        .context(CANARY);
    let diagnostic = Diagnostic::extract(Operation::NativeFinalization, &error);
    assert_eq!(diagnostic.cause, Cause::PoolTimeout);
    assert_eq!(diagnostic.phase, Some("finalization_writer_begin"));
    let events = capture(|| Reporter::new(OP).observe::<usize>(&Err(error), Instant::now()));
    assert_eq!(fields(&events[0])["phase"], "finalization_writer_begin");
    assert!(!serde_json::to_string(&events).unwrap().contains("canary"));
}

#[test]
fn partial_finalization_failure_retains_storage_diagnostics_and_does_not_recover() {
    let events = capture(|| {
        let mut reporter = Reporter::new(OP);
        reporter.observe::<usize>(
            &Err(pool_error().context(pioneer_crud::NativeFinalizationPhase::WriterApply)),
            Instant::now(),
        );
        let mut summary = pioneer_crud::TurnFinalizationReconciliationSummary::default();
        summary.scanned = 1;
        summary.failures.push(Arc::new(
            pool_error()
                .context(pioneer_crud::NativeFinalizationPhase::WriterApply)
                .context(CANARY),
        ));
        reporter.observe_finalizations(&Ok(summary), Instant::now());
        let mut eof = pioneer_crud::TurnFinalizationReconciliationSummary::default();
        eof.pass_complete = true;
        eof.pass_failed = true;
        reporter.observe_finalizations(&Ok(eof), Instant::now());
        assert_eq!(reporter.episode.as_ref().unwrap().failures, 2);
    });
    assert_eq!(
        events.len(),
        1,
        "same storage failure uses existing rate policy"
    );
    // No successful observation closed the episode despite the outer Ok summary.
    assert!(
        events[0]
            .breadcrumbs
            .iter()
            .all(|crumb| crumb.message.as_deref()
                != Some("reconciliation recovered after observed failures"))
    );
    assert!(!serde_json::to_string(&events).unwrap().contains("canary"));
}
