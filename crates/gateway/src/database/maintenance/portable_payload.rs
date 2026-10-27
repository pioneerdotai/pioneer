//! The same cancellation/maintenance lifecycle as the existing storage jobs.
use pioneer_crud::{CrudStore, PortablePayloadProgress};
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
            result=progress.quantum(&store)=>result,
        };
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
        tokio::select! {
            _=cancellation.cancelled()=>return,
            _=tokio::time::sleep(pause)=>{},
        }
    }
}
