mod zstd_payload_compression;

use pioneer_crud::CrudStore;
use std::sync::Arc;

pub(crate) fn spawn(crud_store: Arc<CrudStore>) {
    zstd_payload_compression::spawn(crud_store);
}
