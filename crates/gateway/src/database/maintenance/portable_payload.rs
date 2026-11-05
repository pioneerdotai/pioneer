//! The same cancellation/maintenance lifecycle as the existing storage jobs.
use pioneer_crud::{CrudStore, PortablePayloadProgress};
use pioneer_observability::{DatabaseOperationSite, DatabaseWorkload, portable_payload as observe};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub(crate) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let mut progress = PortablePayloadProgress::default();
    let database = store.database_connection();
    let mut wake = database.work_signal("maintenance.portable").subscribe();
    let mut begin = true;
    let mut retry_pending = false;
    loop {
        if begin {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                result = wake.begin_pass(&database) => if result.is_err() {
                    tokio::select! {
                        _ = cancellation.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(60)) => {},
                    }
                    continue;
                },
            }
            begin = false;
        }
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
            Ok(true) => Duration::from_millis(1),
            Ok(false) if retry_pending => {
                retry_pending = false;
                Duration::from_secs(60)
            }
            Ok(false) => {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = wake.wait(None) => {},
                }
                begin = true;
                continue;
            }
            Err(_) => {
                retry_pending = true;
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
