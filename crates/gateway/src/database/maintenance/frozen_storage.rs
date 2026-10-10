use pioneer_crud::{CrudStore, FrozenStorageLifetimeProgress};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub(super) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let mut progress = FrozenStorageLifetimeProgress::default();
    loop {
        let started = Instant::now();
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            // Lifetime reconciliation/expiry/sweep is deliberately NOT called
            // here before independent code acceptance and rollout validation.
            // The operational enabling diff calls accepted_lifetime_quantum
            // with one worker-owned progress value, alternating this existing
            // conversion quantum. It must reuse this primary store clone.
            outcome = store.compact_frozen_storage_quantum(&mut progress) => outcome,
        };
        let pause = match outcome {
            Ok(true) => std::cmp::max(
                Duration::from_millis(100),
                started.elapsed().saturating_mul(9),
            ),
            Ok(false) => Duration::from_secs(60),
            Err(_) => {
                tracing::warn!(
                    "frozen storage maintenance quantum failed; original history preserved"
                );
                Duration::from_secs(60)
            }
        };
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(pause) => {},
        }
    }
}

/// Concrete integration point for the existing worker after separately
/// accepted rollout. It performs real bounded steps; no feature/config service
/// or second worker/reader domain is introduced. Currently not scheduled.
#[allow(dead_code)]
async fn accepted_lifetime_quantum(
    store: &CrudStore,
    progress: &mut FrozenStorageLifetimeProgress,
) -> anyhow::Result<bool> {
    progress.quantum(store).await
}
