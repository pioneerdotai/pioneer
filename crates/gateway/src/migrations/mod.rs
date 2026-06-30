mod task_anchor_backfill;
mod timeline_pagination_backfill;

use pioneer_crud::CrudStore;
use std::sync::Arc;

pub(crate) fn spawn_gateway_startup_migrations(crud_store: Arc<CrudStore>) {
    let _handle = tokio::spawn(async move {
        task_anchor_backfill::run(crud_store.as_ref()).await;
        timeline_pagination_backfill::run(crud_store.as_ref()).await;
    });
}

#[cfg(test)]
pub(crate) use task_anchor_backfill::backfill_once as backfill_task_anchors_once;
#[cfg(test)]
pub(crate) use timeline_pagination_backfill::backfill_once as backfill_timeline_pagination_once;
