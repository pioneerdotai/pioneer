use pioneer_crud::CrudStore;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub(super) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    loop {
        let started = Instant::now();
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            outcome = store.compact_frozen_storage_quantum() => outcome,
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
