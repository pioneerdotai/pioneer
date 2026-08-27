use anyhow::Result;
use pioneer_crud::CrudStore;
use tracing::{info, warn};

use crate::database::zstd_column::{
    STARTUP_MAINTENANCE_SECONDS, STARTUP_TARGET_DB_LOAD, ZSTD_PAYLOAD_COLUMNS,
    run_cooperative_maintenance_cycle,
};

pub(super) async fn run(crud_store: &CrudStore) -> Result<()> {
    for config in ZSTD_PAYLOAD_COLUMNS {
        super::maintenance_checkpoint().await?;
        let cancellation = super::maintenance_cancellation();
        let outcome = match run_cooperative_maintenance_cycle(
            crud_store,
            std::slice::from_ref(config),
            Some(STARTUP_MAINTENANCE_SECONDS),
            STARTUP_TARGET_DB_LOAD,
            &cancellation,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(
                    table = config.table,
                    column = config.column,
                    error = %format!("{error:#}"),
                    "sqlite-zstd startup payload compression failed"
                );
                return Err(error);
            }
        };
        if outcome.cancelled {
            return super::maintenance_checkpoint().await;
        }
        if let Some(summary) = outcome.summaries.into_iter().next() {
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
                counts_exact = summary.counts_exact,
                "sqlite-zstd startup payload compression completed"
            );
        }
    }
    Ok(())
}
