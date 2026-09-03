use anyhow::Result;
use pioneer_crud::CrudStore;
use tracing::info;

use crate::database::zstd_column::{ZSTD_PAYLOAD_COLUMNS, ensure_compression_schema};

pub(super) async fn run(crud_store: &CrudStore) -> Result<()> {
    super::maintenance_checkpoint().await?;
    let cancellation = super::maintenance_cancellation();
    let summaries =
        ensure_compression_schema(crud_store, ZSTD_PAYLOAD_COLUMNS, &cancellation).await?;
    if cancellation.is_cancelled() {
        return super::maintenance_checkpoint().await;
    }
    for summary in summaries {
        info!(
            table = summary.table,
            column = summary.column,
            enabled_now = summary.enabled_now,
            already_enabled = summary.already_enabled,
            skipped_empty = summary.skipped_empty,
            counts_exact = summary.counts_exact,
            "sqlite-zstd transparent payload schema ready; backlog deferred to background maintenance"
        );
    }
    Ok(())
}
