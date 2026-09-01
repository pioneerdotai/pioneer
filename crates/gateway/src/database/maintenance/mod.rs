mod zstd_payload_compression;

use pioneer_crud::CrudStore;
use std::sync::Arc;

pub(crate) async fn run(
    crud_store: Arc<CrudStore>,
    cancellation: tokio_util::sync::CancellationToken,
) {
    let crud_store = Arc::new(crud_store.with_maintenance_access());
    zstd_payload_compression::run(crud_store, cancellation).await;
}
