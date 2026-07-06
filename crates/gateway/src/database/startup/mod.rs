mod agent_diff_event_compaction;
mod cli_runtime_native_event_compaction;
mod task_anchor_backfill;
pub(crate) mod thread_episodic_workspace_capsule_refill;
mod timeline_pagination_backfill;
mod turn_item_attempt_payload_compaction;
mod turn_permission_profile_backfill;
mod zstd_payload_compression;

use pioneer_crud::CrudStore;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) fn spawn(crud_store: Arc<CrudStore>, thread_episodic_storage_root: PathBuf) {
    let _handle = tokio::spawn(async move {
        task_anchor_backfill::run(crud_store.as_ref()).await;
        turn_permission_profile_backfill::run(crud_store.as_ref()).await;
        timeline_pagination_backfill::run(crud_store.as_ref()).await;
        agent_diff_event_compaction::run(crud_store.as_ref()).await;
        cli_runtime_native_event_compaction::run(crud_store.as_ref()).await;
        turn_item_attempt_payload_compaction::run(crud_store.as_ref()).await;
        zstd_payload_compression::run(crud_store.as_ref()).await;
        thread_episodic_workspace_capsule_refill::run(
            crud_store.clone(),
            thread_episodic_storage_root,
        )
        .await;
    });
}

#[cfg(test)]
pub(crate) use task_anchor_backfill::backfill_once as backfill_task_anchors_once;
#[cfg(test)]
pub(crate) use timeline_pagination_backfill::backfill_once as backfill_timeline_pagination_once;
