use pioneer_crud::{CrudStore, ProjectionReceiptCleanupOutcome};
use std::{
    collections::VecDeque,
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
    let database = store.database_connection();
    let mut wake = database.work_signal("maintenance.receipts").subscribe();
    let mut begin = true;
    // Bounded hints from one acknowledged WorkSignal frontier. Database rows
    // remain authoritative; restart/overflow/unknown writes use full discovery.
    let mut addressed: Option<VecDeque<(String, Option<i64>)>> = None;
    let mut recovery_pending = false;
    let mut through_turn_id = None::<String>;
    let mut after_turn_id = None::<String>;
    let mut scanned = 0_u64;
    let mut deleted = 0_u64;
    let mut deferred = 0_u64;
    let mut source_bytes = 0_u64;
    let mut failed = 0_u64;
    let mut last_progress_log = Instant::now();
    loop {
        if begin {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                result = wake.begin_keyed_pass(&database) => match result {
                    Ok(keys) => {
                        addressed = if recovery_pending {
                            None
                        } else {
                            keys.map(|keys| keys.into_iter().map(|id| (id, None)).collect())
                        };
                        recovery_pending = false;
                    },
                    Err(_) => {
                    tokio::select! {
                        _ = cancellation.cancelled() => return,
                        _ = tokio::time::sleep(IDLE_PAUSE) => {},
                    }
                    continue;
                    }
                },
            }
            begin = false;
        }
        if addressed.is_none() && after_turn_id.is_none() {
            let upper = tokio::select! {
                _ = cancellation.cancelled() => return,
                upper = store.projection_receipt_cleanup_upper_bound() => upper,
            };
            match upper {
                Ok(upper) => through_turn_id = Some(upper.unwrap_or_default()),
                Err(_) => {
                    tokio::select! {
                        _ = cancellation.cancelled() => return,
                        _ = tokio::time::sleep(IDLE_PAUSE) => {},
                    }
                    continue;
                }
            }
        }
        let started = Instant::now();
        // Do not put a local timeout around dispatched SQLite work. Dropping
        // the SQLx future does not interrupt sqlite3_step on its worker thread,
        // so a timed-out quantum can keep consuming reader capacity while the
        // maintenance loop starts another one.
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = crate::database::attribution::scope_database_workload_result(
                pioneer_observability::DatabaseWorkload::ProjectionReceiptCleanup, pioneer_observability::DatabaseOperationSite::ProjectionReceiptCleanupQuantum,
                async {
                    let result = match addressed.as_ref() {
                        Some(pending) => match pending.front() {
                            Some((id, through)) => store.cleanup_projection_receipts_for_turn(id, *through).await,
                            None => Ok(ProjectionReceiptCleanupOutcome { backfill_ready: true, ..Default::default() }),
                        },
                        None => store.cleanup_projection_receipts_quantum(after_turn_id.as_deref(), through_turn_id.as_deref()).await,
                    };
                    match result {
                        Ok(outcome) if outcome.failed => Err(QuantumError::StreamFailed(outcome)),
                        Ok(outcome) => Ok(outcome),
                        Err(_) => Err(QuantumError::Database),
                    }
                },
            ) => result,
        };
        if let Some(pending) = addressed.as_mut() {
            let mut retry = false;
            match result {
                Ok(outcome) | Err(QuantumError::StreamFailed(outcome)) => {
                    if !outcome.backfill_ready {
                        // Backfill completion publishes an unscoped wake and
                        // startup discovery then reconstructs all durable work.
                        pending.clear();
                    } else if let Some((id, through)) = pending.pop_front() {
                        scanned += u64::from(outcome.last_turn_id.is_some());
                        deleted += outcome.rows_deleted;
                        source_bytes += outcome.source_bytes;
                        deferred += u64::from(outcome.deferred);
                        failed += u64::from(outcome.failed);
                        retry = outcome.failed;
                        if retry || outcome.rows_deleted > 0 {
                            // Round-robin bounded batches. Freeze the per-Turn
                            // sequence frontier so a continuous late tail cannot
                            // starve this pass or hide new invalidation behind it.
                            pending.push_back((id, through.or(outcome.through_sequence)));
                        }
                    }
                }
                Err(QuantumError::Database) => {
                    retry = true;
                    tracing::warn!(
                        reason = "database_error",
                        "projection receipt cleanup quantum deferred"
                    );
                }
            }
            if pending.is_empty() {
                if scanned > 0 {
                    tracing::info!(
                        streams_scanned = scanned,
                        rows_deleted = deleted,
                        deferred_streams = deferred,
                        failed_streams = failed,
                        source_bytes,
                        "projection receipt cleanup pass completed"
                    );
                }
                scanned = 0;
                deleted = 0;
                deferred = 0;
                failed = 0;
                source_bytes = 0;
                last_progress_log = Instant::now();
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = wake.wait(None) => {},
                }
                begin = true;
            } else {
                let pause = if retry {
                    IDLE_PAUSE
                } else {
                    quantum_pause(started.elapsed())
                };
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = tokio::time::sleep(pause) => {},
                }
            }
            continue;
        }
        let mut parked = false;
        let pause = match result {
            Ok(outcome) if !outcome.backfill_ready => {
                parked = true;
                IDLE_PAUSE
            }
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
                    // Recovery must drain more than one batch per stream before
                    // switching to keyed hints; absence of a new notification
                    // does not mean the durable recovery backlog is empty.
                    recovery_pending = failed > 0 || deleted > 0;
                    begin = true;
                    parked = failed == 0 && deleted == 0;
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
            Err(QuantumError::Database) => {
                tracing::warn!(
                    reason = "database_error",
                    "projection receipt cleanup quantum deferred"
                );
                IDLE_PAUSE
            }
        };
        if parked {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = wake.wait(None) => {},
            }
            begin = true;
        } else {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                _ = tokio::time::sleep(pause) => {},
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
