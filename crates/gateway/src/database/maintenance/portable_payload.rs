//! The same cancellation/maintenance lifecycle as the existing storage jobs.
use pioneer_crud::{CrudStore, PortablePayloadProgress};
use pioneer_observability::{DatabaseOperationSite, DatabaseWorkload, portable_payload as observe};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub(crate) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let mut progress = PortablePayloadProgress::default();
    let mut empty = 0;
    loop {
        let result = tokio::select! {
            biased;
            _=cancellation.cancelled()=>return,
            result=crate::database::attribution::scope_database_workload_result(
                DatabaseWorkload::PortablePayloadMaintenance,
                DatabaseOperationSite::PortablePayloadQuantum,
                progress.quantum(&store),
            )=>result,
        };
        let backoff = result.is_err();
        let pause = match result {
            Ok(true) => {
                empty = 0;
                Duration::from_millis(1)
            }
            Ok(false) => {
                empty += 1;
                if empty >= 5 {
                    empty = 0;
                    Duration::from_secs(60)
                } else {
                    Duration::from_millis(1)
                }
            }
            Err(_) => {
                tracing::warn!(
                    "portable payload maintenance quantum failed; original data retained"
                );
                Duration::from_secs(60)
            }
        };
        // The completed quantum is gone before a separate bounded sleep work.
        // Its duration/policy are unchanged; only the wait's cause is observed.
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
