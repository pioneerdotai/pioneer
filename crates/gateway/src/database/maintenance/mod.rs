mod projection_receipt_cleanup;
mod zstd_payload_compression;

use pioneer_crud::CrudStore;
use std::sync::Arc;

pub(crate) async fn run(
    crud_store: Arc<CrudStore>,
    cancellation: tokio_util::sync::CancellationToken,
) {
    let crud_store = Arc::new(crud_store.with_maintenance_access());
    tokio::join!(
        zstd_payload_compression::run(crud_store.clone(), cancellation.clone()),
        projection_receipt_cleanup::run(crud_store, cancellation),
    );
}
