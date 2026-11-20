mod frozen_storage;
pub(crate) mod html_previews;
mod native_event_cleanup;
mod projection_receipt_cleanup;

#[cfg(test)]
pub(crate) use frozen_storage::run as run_frozen_worker_for_test;

use pioneer_crud::CrudStore;
use std::sync::Arc;

pub(crate) async fn run(
    crud_store: Arc<CrudStore>,
    artifacts: Arc<pioneer_artifacts::ArtifactService>,
    cancellation: tokio_util::sync::CancellationToken,
) {
    let crud_store = Arc::new(crud_store.with_maintenance_access());
    tokio::join!(
        projection_receipt_cleanup::run(crud_store.clone(), cancellation.clone()),
        frozen_storage::run(crud_store.clone(), cancellation.clone()),
        html_previews::run(crud_store.clone(), artifacts, cancellation.clone()),
        native_event_cleanup::run(crud_store, cancellation),
    );
}
