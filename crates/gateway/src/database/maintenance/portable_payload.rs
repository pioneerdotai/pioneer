//! The same cancellation/maintenance lifecycle as the existing storage jobs.
use pioneer_crud::{CrudStore, PortablePayloadProgress};
use pioneer_observability::{DatabaseOperationSite, DatabaseWorkload, portable_payload as observe};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub(crate) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let mut progress = PortablePayloadProgress::default();
    let database = store.database_connection();
    let mut wake = database.work_signal("maintenance.portable").subscribe();
    let mut begin = true;
    let mut retry_pending = false;
    loop {
        if begin {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                result = wake.begin_pass(&database) => if result.is_err() {
                    tokio::select! {
                        _ = cancellation.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(60)) => {},
                    }
                    continue;
                },
            }
            begin = false;
        }
        let started = tokio::time::Instant::now();
        let result = tokio::select! {
            biased;
            _=cancellation.cancelled()=>return,
            result=crate::database::attribution::scope_database_workload_result(
                DatabaseWorkload::PortablePayloadMaintenance,
                DatabaseOperationSite::PortablePayloadQuantum,
                progress.quantum(&store),
            )=>result,
        };
        let completed = tokio::time::Instant::now();
        let backoff = result.is_err();
        let pause = match result {
            Ok(true) => Duration::ZERO,
            Ok(false) if retry_pending => {
                retry_pending = false;
                Duration::from_secs(60)
            }
            Ok(false) => {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = wake.wait(None) => {},
                }
                begin = true;
                continue;
            }
            Err(_) => {
                retry_pending = true;
                tracing::warn!(
                    "portable payload maintenance quantum failed; original data retained"
                );
                Duration::from_secs(60)
            }
        };
        // Charge elapsed quantum time (including awaits) at a 10% target.
        // Backoff and pacing overlap; there are no banked burst credits.
        // The quantum/work guard and all DB capacity are gone before waiting.
        let pause = pause.max(pacing_remaining(started, completed));
        let cancelled = observe::pause(pause, backoff, async {
            tokio::select! {
                _=cancellation.cancelled()=>true,
                _=tokio::time::sleep(pause)=>false,
            }
        })
        .await;
        if cancelled {
            return;
        }
    }
}

/// One completed portion owes nine portions of rest. Any elapsed time after
/// completion counts toward that rest; idle EOF bypasses this entirely.
fn pacing_remaining(started: tokio::time::Instant, completed: tokio::time::Instant) -> Duration {
    let rest = completed.duration_since(started).saturating_mul(9);
    rest.saturating_sub(tokio::time::Instant::now().duration_since(completed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pacing_accounts_for_elapsed_rest_without_banking_credit() {
        for millis in [1, 10, 100, 1000] {
            let started = tokio::time::Instant::now();
            tokio::time::advance(Duration::from_millis(millis)).await;
            let completed = tokio::time::Instant::now();
            assert_eq!(
                pacing_remaining(started, completed),
                Duration::from_millis(millis * 9)
            );
            tokio::time::advance(Duration::from_millis(millis * 3)).await;
            assert_eq!(
                pacing_remaining(started, completed),
                Duration::from_millis(millis * 6)
            );
            tokio::time::advance(Duration::from_secs(60)).await;
            assert_eq!(pacing_remaining(started, completed), Duration::ZERO);
        }
    }
}

#[cfg(test)]
mod worker_tests {
    use super::*;
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadEvent, SqliteReadObserver, SqliteWriteExecutor,
    };
    use sea_orm::ConnectionTrait;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    #[derive(Default)]
    struct Reads {
        finished: AtomicUsize,
        enqueued: AtomicUsize,
    }
    impl SqliteReadObserver for Reads {
        fn observe(&self, event: SqliteReadEvent) {
            match event {
                SqliteReadEvent::OperationFinished { .. } => {
                    self.finished.fetch_add(1, SeqCst);
                }
                SqliteReadEvent::AdmissionEnqueued { .. } => {
                    self.enqueued.fetch_add(1, SeqCst);
                }
                _ => {}
            }
        }
    }

    async fn empty_fixture() -> (tempfile::TempDir, SqliteDatabase, Arc<Reads>) {
        use migration::MigratorTrait;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("portable-idle.sqlite");
        let writer = sea_orm::Database::connect(pioneer_sqlite::sqlite_connection_url(&path))
            .await
            .unwrap();
        writer
            .execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        migration::Migrator::up(&writer, None).await.unwrap();
        let mut options =
            sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
        options.map_sqlx_sqlite_opts(|o| o.pragma("query_only", "ON"));
        let reader = sea_orm::Database::connect(options).await.unwrap();
        let reads = Arc::new(Reads::default());
        let db = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::new(writer),
            reads.clone(),
        );
        (directory, db, reads)
    }

    // Keep the test task runnable while real SQL futures settle, so paused
    // Tokio time cannot auto-advance to a worker timer. Every wait is bounded.
    async fn drive_worker_until(
        worker: &mut std::pin::Pin<Box<impl std::future::Future<Output = ()>>>,
        advance: Duration,
        ready: impl Fn() -> bool,
    ) {
        for _ in 0..10000 {
            assert!(futures_util::poll!(&mut *worker).is_pending());
            if ready() {
                return;
            }
            if !advance.is_zero() {
                tokio::time::advance(advance).await;
            }
            tokio::task::yield_now().await;
        }
        panic!("worker did not reach the observed DB boundary");
    }

    #[tokio::test(start_paused = true)]
    async fn worker_pays_active_debt_retains_throttle_signal_and_cancels_during_rest() {
        use futures_util::StreamExt;
        use sea_orm::{DbBackend, Statement, StreamTrait};
        let (_directory, db, reads) = empty_fixture().await;
        let maintenance = db.maintenance();
        let signal = db.work_signal("maintenance.portable");
        let cancellation = CancellationToken::new();
        // A partly consumed real stream holds the sole maintenance admission.
        // It is a fixture gate, not a worker hook or a replacement DB future.
        let stream_query = Statement::from_string(
            DbBackend::Sqlite,
            "SELECT 1 AS value UNION ALL SELECT 2 AS value",
        );
        let mut held = maintenance.stream_raw(stream_query.clone()).await.unwrap();
        assert!(held.next().await.unwrap().is_ok());
        let before_worker_admission = reads.enqueued.load(SeqCst);
        let before_worker_finish = reads.finished.load(SeqCst);
        let mut worker = Box::pin(run(Arc::new(CrudStore::new(db)), cancellation.clone()));
        let started = tokio::time::Instant::now();
        assert!(futures_util::poll!(&mut worker).is_pending());
        assert_eq!(reads.enqueued.load(SeqCst), before_worker_admission + 1);
        assert_eq!(reads.finished.load(SeqCst), before_worker_finish);
        tokio::time::advance(Duration::from_millis(100)).await;
        drop(held);
        let stream_finishes = reads.finished.load(SeqCst);
        // This empty startup quantum reads cleanup and the first family upper.
        // No clock advances while its real queries complete and it enters rest.
        drive_worker_until(&mut worker, Duration::ZERO, || {
            reads.finished.load(SeqCst) >= stream_finishes + 2
        })
        .await;
        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_millis(100)
        );
        let during_rest = reads.enqueued.load(SeqCst);
        assert_eq!(during_rest, before_worker_admission + 2);
        tokio::time::advance(Duration::from_millis(400)).await;
        // A relevant domain signal during the actual worker throttle must not
        // waive its debt, and must survive to another finite pass after EOF.
        signal.notify();
        assert!(futures_util::poll!(&mut worker).is_pending());
        assert_eq!(reads.enqueued.load(SeqCst), during_rest);
        tokio::time::advance(Duration::from_millis(499)).await;
        assert!(futures_util::poll!(&mut worker).is_pending());
        assert_eq!(
            reads.enqueued.load(SeqCst),
            during_rest,
            "next work before 9x rest"
        );
        // Allow one millisecond of timer resolution after the 900-ms debt.
        tokio::time::advance(Duration::from_millis(2)).await;
        drive_worker_until(&mut worker, Duration::ZERO, || {
            reads.enqueued.load(SeqCst) > during_rest
        })
        .await;
        // Six reads per empty pass. Reaching twelve worker reads proves the
        // signal sent during rest was not acknowledged/lost within that pass.
        drive_worker_until(&mut worker, Duration::from_millis(1), || {
            reads.finished.load(SeqCst) >= stream_finishes + 12
        })
        .await;
        // All twelve reads are complete. Flush the last portion's rest and
        // EOF; prolonged idle must not bank credit for the later active pass.
        for _ in 0..2 {
            tokio::time::advance(Duration::from_secs(3600)).await;
            assert!(futures_util::poll!(&mut worker).is_pending());
            assert_eq!(reads.finished.load(SeqCst), stream_finishes + 12);
        }

        // Start a fresh pass with another known active await, then interrupt
        // its real throttle before the 1.8 seconds of rest have elapsed.
        let mut held = maintenance.stream_raw(stream_query).await.unwrap();
        assert!(held.next().await.unwrap().is_ok());
        let before_restart = reads.enqueued.load(SeqCst);
        signal.notify();
        assert!(futures_util::poll!(&mut worker).is_pending());
        assert_eq!(reads.enqueued.load(SeqCst), before_restart + 1);
        tokio::time::advance(Duration::from_millis(200)).await;
        drop(held);
        let before_restart_finish = reads.finished.load(SeqCst);
        drive_worker_until(&mut worker, Duration::ZERO, || {
            reads.finished.load(SeqCst) >= before_restart_finish + 2
        })
        .await;
        assert_eq!(reads.enqueued.load(SeqCst), before_restart + 2);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(futures_util::poll!(&mut worker).is_pending());
        assert_eq!(reads.enqueued.load(SeqCst), before_restart + 2);
        let shutdown_at = tokio::time::Instant::now();
        cancellation.cancel();
        assert!(futures_util::poll!(&mut worker).is_ready());
        assert_eq!(tokio::time::Instant::now(), shutdown_at);
        assert_eq!(reads.enqueued.load(SeqCst), before_restart + 2);
    }

    #[tokio::test(start_paused = true)]
    async fn empty_worker_parks_until_signal_and_cancels_without_idle_polling() {
        let (_directory, db, reads) = empty_fixture().await;
        let signal = db.work_signal("maintenance.portable");
        let cancellation = CancellationToken::new();
        let mut worker = Box::pin(run(Arc::new(CrudStore::new(db)), cancellation.clone()));
        // Drive actual DB futures while virtual time pays active pacing. The
        // empty pass has one cleanup frontier and five family frontier reads.
        let mut invalidated_during_pass = false;
        for expected in [12, 18] {
            for _ in 0..10000 {
                assert!(futures_util::poll!(&mut worker).is_pending());
                let count = reads.finished.load(SeqCst);
                if !invalidated_during_pass && count > 0 {
                    signal.notify();
                    invalidated_during_pass = true;
                }
                if count == expected {
                    break;
                }
                tokio::time::advance(Duration::from_secs(1)).await;
                tokio::task::yield_now().await;
            }
            assert_eq!(reads.finished.load(SeqCst), expected);
            // Finish pacing and the global EOF transition; no periodic SQL.
            for _ in 0..10 {
                tokio::time::advance(Duration::from_secs(3600)).await;
                assert!(futures_util::poll!(&mut worker).is_pending());
            }
            assert_eq!(reads.finished.load(SeqCst), expected);
            signal.notify();
        }
        cancellation.cancel();
        worker.await;
        assert!(invalidated_during_pass);
        assert_eq!(reads.finished.load(SeqCst), 18);
    }
    #[tokio::test(start_paused = true)]
    async fn database_error_keeps_backoff_and_shutdown_interrupts_it() {
        let raw = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        let reads = Arc::new(Reads::default());
        let db = SqliteDatabase::from_executor_with_read_observer(
            raw.clone(),
            SqliteWriteExecutor::new(raw),
            reads.clone(),
        );
        let cancellation = CancellationToken::new();
        let mut worker = Box::pin(run(Arc::new(CrudStore::new(db)), cancellation.clone()));
        for _ in 0..10000 {
            assert!(futures_util::poll!(&mut worker).is_pending());
            if reads.finished.load(SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(reads.finished.load(SeqCst), 1);
        // Drain the completed error into its wait without advancing the clock.
        for _ in 0..10 {
            assert!(futures_util::poll!(&mut worker).is_pending());
        }
        tokio::time::advance(Duration::from_secs(59)).await;
        assert!(futures_util::poll!(&mut worker).is_pending());
        assert_eq!(reads.finished.load(SeqCst), 1);
        cancellation.cancel();
        worker.await;
        assert_eq!(reads.finished.load(SeqCst), 1);
    }
}
