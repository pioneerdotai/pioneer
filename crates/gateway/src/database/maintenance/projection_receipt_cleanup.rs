use pioneer_crud::{CrudStore, ProjectionReceiptCleanupOutcome};
use pioneer_sqlite::background::{Plan, Ready};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const MIN_QUANTUM_PAUSE: Duration = Duration::from_millis(25);
const IDLE_PAUSE: Duration = Duration::from_secs(60);

enum QuantumError {
    StreamFailed(ProjectionReceiptCleanupOutcome),
    Database,
}

pub(super) async fn run(crud_store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = crud_store.with_maintenance_access();
    let work = store.background_work("storage.receipts");
    let Some(mut owner) = work.acquire() else {
        return;
    };
    let mut after_turn_id = None::<String>;
    let mut pass_changed = false;
    let mut scanned = 0_u64;
    let mut deleted = 0_u64;
    let mut deferred = 0_u64;
    let mut source_bytes = 0_u64;
    let mut failed = 0_u64;
    let mut last_progress_log = Instant::now();
    loop {
        let ready = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            ready = owner.ready() => ready,
        };
        if ready == Ready::Closed {
            return;
        }
        if ready == Ready::SettlementRequired {
            let settle_observation = owner.observe();
            after_turn_id = None;
            pass_changed = false;
            if store
                .database_connection()
                .settle_background_change(&owner)
                .await
                .is_err()
            {
                owner.retry_after(settle_observation, IDLE_PAUSE);
            }
            continue;
        }
        let observed = owner.observe();
        let started = Instant::now();
        // Do not put a local timeout around dispatched SQLite work. Dropping
        // the SQLx future does not interrupt sqlite3_step on its worker thread,
        // so a timed-out quantum can keep consuming reader capacity while the
        // maintenance loop starts another one.
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = owner.scope(crate::database::attribution::scope_database_workload_result(
                pioneer_observability::DatabaseWorkload::ProjectionReceiptCleanup, pioneer_observability::DatabaseOperationSite::ProjectionReceiptCleanupQuantum,
                async {
                    match store.cleanup_projection_receipts_quantum(after_turn_id.as_deref()).await {
                        Ok(outcome) if outcome.failed => Err(QuantumError::StreamFailed(outcome)),
                        Ok(outcome) => Ok(outcome),
                        Err(_) => Err(QuantumError::Database),
                    }
                },
            )) => result,
        };
        let plan = match result {
            Ok(outcome) if !outcome.backfill_ready => Plan::Dormant,
            Ok(outcome) | Err(QuantumError::StreamFailed(outcome)) => {
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
                    Plan::MonotonicDeadline(
                        tokio::time::Instant::now() + quantum_pause(started.elapsed()),
                    )
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
                    let plan = if failed > 0 || deferred > 0 {
                        // Known failed/deferred prefixes are not settled Empty.
                        Plan::MonotonicDeadline(tokio::time::Instant::now() + IDLE_PAUSE)
                    } else if deleted > 0 || pass_changed {
                        Plan::MonotonicDeadline(
                            tokio::time::Instant::now() + quantum_pause(started.elapsed()),
                        )
                    } else {
                        Plan::Dormant
                    };
                    pass_changed = false;
                    scanned = 0;
                    deleted = 0;
                    deferred = 0;
                    source_bytes = 0;
                    failed = 0;
                    last_progress_log = Instant::now();
                    plan
                }
            }
            Err(QuantumError::Database) => {
                tracing::warn!(
                    reason = "database_error",
                    "projection receipt cleanup quantum deferred"
                );
                Plan::MonotonicDeadline(tokio::time::Instant::now() + IDLE_PAUSE)
            }
        };
        // Preserve a changed old key while finishing this finite immutable-ID
        // pass. Never clear a producer that committed during discovery/apply.
        if !owner.publish(observed, plan) {
            pass_changed = true;
            if let Plan::MonotonicDeadline(at) = plan {
                owner.retry_after(
                    observed,
                    at.saturating_duration_since(tokio::time::Instant::now()),
                );
            }
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
