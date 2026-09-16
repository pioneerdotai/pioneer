use pioneer_crud::CrudStore;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub(super) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let mut after = 0;
    let mut scanned = 0_u64;
    let mut deleted = 0_u64;
    let mut last_log = Instant::now();
    loop {
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = tokio::time::timeout(Duration::from_secs(5),
                crate::database::attribution::scope_database_workload_result(
                    pioneer_observability::DatabaseWorkload::NativeEventCleanup,
                    store.cleanup_native_events_quantum(after),
                )) => result,
        };
        let pause = match result {
            Ok(Ok(outcome)) => {
                scanned += outcome.rows_scanned;
                deleted += outcome.rows_deleted;
                let complete = outcome.last_rowid.is_none();
                if complete || last_log.elapsed() >= Duration::from_secs(30) {
                    tracing::info!(
                        rows_scanned = scanned,
                        rows_deleted = deleted,
                        pass_complete = complete,
                        "native event cleanup progress"
                    );
                    last_log = Instant::now();
                }
                // The source is the queue. Restarting this scan is idempotent;
                // no durable cursor or backfill table is needed for deletion.
                after = outcome.last_rowid.unwrap_or(0);
                if complete {
                    scanned = 0;
                    deleted = 0;
                    Duration::from_secs(60)
                } else {
                    started
                        .elapsed()
                        .saturating_mul(9)
                        .max(Duration::from_millis(25))
                }
            }
            error => {
                tracing::warn!(
                    reason = if error.is_err() {
                        "quantum_timeout"
                    } else {
                        "database_error"
                    },
                    "native event cleanup deferred"
                );
                Duration::from_secs(60)
            }
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(pause) => {},
        }
    }
}
