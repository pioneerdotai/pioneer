use pioneer_crud::{CrudStore, FrozenStorageLifetimeProgress};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub(crate) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let mut progress = FrozenStorageLifetimeProgress::default();
    let mut lifetime = false;
    let mut idle = [false; 2];
    loop {
        let started = Instant::now();
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            outcome = async {
                if lifetime { progress.quantum(&store).await }
                else { store.compact_frozen_storage_quantum(&mut progress).await }
            } => outcome,
        };
        let path = usize::from(lifetime);
        lifetime = !lifetime;
        let pause = match outcome {
            Ok(more) => pause_after_quantum(&mut idle, path, more, started.elapsed()),
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

// EOF is meaningful only after both discovery paths finish. Start a fresh round
// after the idle wait; a cached EOF must not postpone the other path's next pass.
fn pause_after_quantum(
    idle: &mut [bool; 2],
    path: usize,
    more: bool,
    elapsed: Duration,
) -> Duration {
    idle[path] = !more;
    if idle.iter().all(|path| *path) {
        *idle = [false; 2];
        Duration::from_secs(60)
    } else {
        std::cmp::max(Duration::from_millis(100), elapsed.saturating_mul(9))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_conversion_does_not_sleep_pending_lifetime_and_idle_round_resets() {
        let mut idle = [false; 2];
        let elapsed = Duration::ZERO;
        assert_eq!(
            pause_after_quantum(&mut idle, 0, false, elapsed),
            Duration::from_millis(100)
        );
        for _ in 0..8 {
            assert_eq!(
                pause_after_quantum(&mut idle, 1, true, elapsed),
                Duration::from_millis(100)
            );
            assert_eq!(
                pause_after_quantum(&mut idle, 0, false, elapsed),
                Duration::from_millis(100)
            );
        }
        assert_eq!(
            pause_after_quantum(&mut idle, 1, false, elapsed),
            Duration::from_secs(60)
        );
        assert_eq!(
            pause_after_quantum(&mut idle, 0, false, elapsed),
            Duration::from_millis(100)
        );
    }
}
