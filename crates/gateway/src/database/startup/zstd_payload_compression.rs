use pioneer_crud::CrudStore;
use tracing::{info, warn};

use crate::database::zstd_column::{
    STARTUP_MAINTENANCE_SECONDS, STARTUP_TARGET_DB_LOAD, ZSTD_PAYLOAD_COLUMNS, run_startup_once,
};

pub(super) async fn run(crud_store: &CrudStore) {
    for config in ZSTD_PAYLOAD_COLUMNS {
        match run_startup_once(
            crud_store,
            *config,
            Some(STARTUP_MAINTENANCE_SECONDS),
            STARTUP_TARGET_DB_LOAD,
        )
        .await
        {
            Ok(summary) => {
                info!(
                    table = summary.table,
                    column = summary.column,
                    enabled_now = summary.enabled_now,
                    already_enabled = summary.already_enabled,
                    skipped_empty = summary.skipped_empty,
                    total_rows = summary.total_rows,
                    pending_before = summary.pending_before,
                    pending_after = summary.pending_after,
                    maintenance_more_pending = summary.maintenance_more_pending,
                    "sqlite-zstd startup payload compression completed"
                );
            }
            Err(error) => {
                warn!(
                    table = config.table,
                    column = config.column,
                    error = %format!("{error:#}"),
                    "sqlite-zstd startup payload compression failed"
                );
            }
        }
    }
}
