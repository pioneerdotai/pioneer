//! The same cancellation/maintenance lifecycle as the existing storage jobs.
use pioneer_crud::{CrudStore, PortablePayloadProgress};
use pioneer_observability::{DatabaseOperationSite, DatabaseWorkload, portable_payload as observe};
use pioneer_sqlite::background::{Plan, Ready};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub(crate) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let work = store.background_work("storage.portable");
    let Some(mut owner) = work.acquire() else {
        return;
    };
    let mut progress = PortablePayloadProgress::default();
    let mut empty = 0;
    let mut previous = None;
    let mut rescan = false;
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
            progress.invalidate();
            empty = 0;
            rescan = true;
            if store
                .database_connection()
                .settle_background_change(&owner)
                .await
                .is_err()
            {
                owner.retry_after(settle_observation, Duration::from_secs(60));
            }
            continue;
        }
        let observed = owner.observe();
        if previous.is_some_and(|old| old != observed) {
            progress.invalidate();
            empty = 0;
            rescan = true;
        }
        previous = Some(observed);
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = owner.scope(crate::database::attribution::scope_database_workload_result(
                DatabaseWorkload::PortablePayloadMaintenance,
                DatabaseOperationSite::PortablePayloadQuantum,
                progress.quantum(&store),
            )) => result,
        };
        let backoff = result.is_err() || owner.requires_settlement();
        let pause = match result {
            Ok(true) if !owner.requires_settlement() => {
                empty = 0;
                Some(Duration::from_millis(1))
            }
            Ok(false) if !owner.requires_settlement() => {
                empty += 1;
                if empty >= 5 {
                    empty = 0;
                    if std::mem::take(&mut rescan) {
                        progress.invalidate();
                        Some(Duration::from_millis(1))
                    } else {
                        None
                    }
                } else {
                    Some(Duration::from_millis(1))
                }
            }
            _ => {
                tracing::warn!(
                    "portable payload maintenance quantum failed; original data retained"
                );
                // The common retry wait is producer-interruptible. Do not
                // put a second unconditional sleep in front of it.
                None
            }
        };
        if backoff {
            owner.retry_after(observed, Duration::from_secs(60));
        } else {
            // A full five-family pass and raw cleanup EOF, generation-fenced.
            // Future source/retirement/last-blocking-hold release interrupts it.
            owner.publish(observed, Plan::Dormant);
        }
        if let Some(pause) = pause {
            let cancelled = observe::pause(pause, backoff, async {
                tokio::select! {
                    _ = cancellation.cancelled() => true,
                    _ = tokio::time::sleep(pause) => false,
                }
            })
            .await;
            if cancelled {
                return;
            }
            if !backoff {
                owner.publish(observed, Plan::Runnable);
            }
        }
    }
}
