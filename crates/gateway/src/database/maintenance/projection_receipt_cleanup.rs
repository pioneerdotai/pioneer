use pioneer_crud::{CrudStore, ProjectionReceiptCleanupOutcome};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const MIN_QUANTUM_PAUSE: Duration = Duration::from_millis(25);
const IDLE_PAUSE: Duration = Duration::from_secs(60);
const QUANTUM_TIMEOUT: Duration = Duration::from_secs(5);

enum QuantumError {
    StreamFailed(ProjectionReceiptCleanupOutcome),
    Database,
}

pub(super) async fn run(crud_store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = crud_store.with_maintenance_access();
    let mut after_turn_id = None::<String>;
    let mut scanned = 0_u64;
    let mut deleted = 0_u64;
    let mut deferred = 0_u64;
    let mut source_bytes = 0_u64;
    let mut failed = 0_u64;
    let mut last_progress_log = Instant::now();
    loop {
        let started = Instant::now();
        // Dropping this future cancels reader/writer admission and rolls back
        // an unfinished transaction. No detached maintenance tasks.
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = tokio::time::timeout(QUANTUM_TIMEOUT,
                crate::database::attribution::scope_database_workload_result(
                    pioneer_observability::DatabaseWorkload::ProjectionReceiptCleanup,
                    async {
                        match store.cleanup_projection_receipts_quantum(after_turn_id.as_deref()).await {
                            Ok(outcome) if outcome.failed => Err(QuantumError::StreamFailed(outcome)),
                            Ok(outcome) => Ok(outcome),
                            Err(_) => Err(QuantumError::Database),
                        }
                    },
                )) => result,
        };
        let pause = match result {
            Ok(Ok(outcome)) if !outcome.backfill_ready => IDLE_PAUSE,
            Ok(Ok(outcome)) | Ok(Err(QuantumError::StreamFailed(outcome))) => {
                if let Some(turn_id) = outcome.last_turn_id {
                    after_turn_id = Some(turn_id);
                    scanned += 1;
                    deleted += outcome.rows_deleted;
                    source_bytes += outcome.source_bytes;
                    deferred += u64::from(outcome.deferred);
                    failed += u64::from(outcome.failed);
                    if last_progress_log.elapsed() >= Duration::from_secs(30) {
                        tracing::info!(
                            streams_scanned = scanned,
                            rows_deleted = deleted,
                            deferred_streams = deferred,
                            failed_streams = failed,
                            source_bytes,
                            "projection receipt cleanup progress"
                        );
                        last_progress_log = Instant::now();
                    }
                    quantum_pause(started.elapsed())
                } else {
                    tracing::info!(
                        streams_scanned = scanned,
                        rows_deleted = deleted,
                        deferred_streams = deferred,
                        failed_streams = failed,
                        source_bytes,
                        "projection receipt cleanup pass completed"
                    );
                    if failed > 0 {
                        tracing::warn!(
                            failed_streams = failed,
                            "projection receipt cleanup streams failed; receipts preserved for retry"
                        );
                    }
                    after_turn_id = None;
                    let pause = if deleted > 0 {
                        quantum_pause(started.elapsed())
                    } else {
                        IDLE_PAUSE
                    };
                    scanned = 0;
                    deleted = 0;
                    deferred = 0;
                    source_bytes = 0;
                    failed = 0;
                    last_progress_log = Instant::now();
                    pause
                }
            }
            Ok(Err(QuantumError::Database)) => {
                tracing::warn!(
                    reason = "database_error",
                    "projection receipt cleanup quantum deferred"
                );
                IDLE_PAUSE
            }
            Err(_) => {
                tracing::warn!(
                    reason = "quantum_timeout",
                    "projection receipt cleanup quantum deferred"
                );
                IDLE_PAUSE
            }
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(pause) => {},
        }
    }
}

fn quantum_pause(elapsed: Duration) -> Duration {
    // Target at most 10% duty cycle, with all DB reservations released while
    // sleeping. Queue contention therefore slows cleanup rather than boosting it.
    elapsed.saturating_mul(9).max(MIN_QUANTUM_PAUSE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_quanta_increase_the_pause_without_holding_database_access() {
        assert_eq!(quantum_pause(Duration::ZERO), MIN_QUANTUM_PAUSE);
        assert_eq!(
            quantum_pause(Duration::from_millis(100)),
            Duration::from_millis(900)
        );
    }

    #[tokio::test]
    async fn cancelled_worker_does_not_touch_an_unmigrated_database() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        tokio::time::timeout(
            Duration::from_secs(1),
            run(Arc::new(CrudStore::new(db)), cancellation),
        )
        .await
        .expect("cancelled worker must return immediately");
    }
}
