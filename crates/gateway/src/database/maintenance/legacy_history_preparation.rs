use pioneer_crud::CrudStore;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const MIN_QUANTUM_PAUSE: Duration = Duration::from_millis(25);
const IDLE_PAUSE: Duration = Duration::from_secs(60);

enum QuantumOutcome {
    Progress,
    Idle,
}

/// Register pre-compaction histories after startup without delaying ordinary
/// CLI turns. The repository owns the durable per-thread cursor; this worker
/// owns only fair traversal between histories.
pub(super) async fn run(crud_store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = crud_store.with_maintenance_access();
    let mut after_thread = String::new();
    let mut active = None::<(String, String)>;
    loop {
        let started = Instant::now();
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            outcome = crate::database::attribution::scope_database_workload_result(
                pioneer_observability::DatabaseWorkload::CompactionMaintenance,
                run_quantum(&store, &mut after_thread, &mut active),
            ) => outcome,
        };
        let pause = match outcome {
            Ok(QuantumOutcome::Progress) => quantum_pause(started.elapsed()),
            Ok(QuantumOutcome::Idle) => IDLE_PAUSE,
            Err(()) => {
                tracing::warn!(
                    reason = "database_error",
                    "legacy compaction history preparation quantum deferred"
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

async fn run_quantum(
    store: &CrudStore,
    after_thread: &mut String,
    active: &mut Option<(String, String)>,
) -> Result<QuantumOutcome, ()> {
    if active.is_none() {
        let mut candidate = store
            .compaction_history_preparation_candidate_after(after_thread)
            .await;
        if matches!(candidate, Ok(None)) && !after_thread.is_empty() {
            after_thread.clear();
            candidate = store
                .compaction_history_preparation_candidate_after(after_thread)
                .await;
        }
        match candidate {
            Ok(Some(candidate)) => *active = Some(candidate),
            Ok(None) => return Ok(QuantumOutcome::Idle),
            Err(_) => return Err(()),
        }
    }

    let (workspace, thread) = active.as_ref().expect("active candidate was selected");
    match store
        .compaction_prepare_history_quantum(workspace, thread)
        .await
    {
        Ok(true) => {
            after_thread.clone_from(thread);
            *active = None;
            Ok(QuantumOutcome::Progress)
        }
        Ok(false) => Ok(QuantumOutcome::Progress),
        Err(_) => {
            // Continue after a poison history. The traversal wraps after the
            // remaining candidates have had an opportunity to progress.
            after_thread.clone_from(thread);
            *active = None;
            Err(())
        }
    }
}

fn quantum_pause(elapsed: Duration) -> Duration {
    // Match other database maintenance: target at most 10% duty cycle and
    // release every reader/writer reservation before sleeping.
    elapsed.saturating_mul(9).max(MIN_QUANTUM_PAUSE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_quanta_increase_pause_without_holding_database_access() {
        assert_eq!(quantum_pause(Duration::ZERO), MIN_QUANTUM_PAUSE);
        assert_eq!(
            quantum_pause(Duration::from_millis(100)),
            Duration::from_millis(900)
        );
    }

    #[tokio::test]
    async fn cancelled_worker_does_not_touch_an_unmigrated_database() {
        let database = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        tokio::time::timeout(
            Duration::from_secs(1),
            run(Arc::new(CrudStore::new(database)), cancellation),
        )
        .await
        .expect("cancelled worker must return immediately");
    }
}
