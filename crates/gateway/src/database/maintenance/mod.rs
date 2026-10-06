mod frozen_storage;
mod native_event_cleanup;
mod projection_receipt_cleanup;
mod zstd_payload_compression;

#[cfg(test)]
pub(crate) use zstd_payload_compression::run as run_zstd_worker_for_test;

use pioneer_crud::CrudStore;
use std::sync::Arc;

pub(crate) async fn run(
    crud_store: Arc<CrudStore>,
    cancellation: tokio_util::sync::CancellationToken,
) {
    let crud_store = Arc::new(crud_store.with_maintenance_access());
    // A fresh cache per database worker, retained through every outer cycle.
    let cache = super::zstd_column::PreparedDictionaryCache::new(crud_store.as_ref());
    tokio::join!(
        zstd_payload_compression::run(crud_store.clone(), cancellation.clone(), cache),
        projection_receipt_cleanup::run(crud_store.clone(), cancellation.clone()),
        frozen_storage::run(crud_store.clone(), cancellation.clone()),
        native_event_cleanup::run(crud_store, cancellation),
    );
}
