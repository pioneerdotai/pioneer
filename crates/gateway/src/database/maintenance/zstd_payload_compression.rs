use pioneer_crud::CrudStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::database::zstd_column::{
    PERIODIC_MAINTENANCE_INTERVAL_SECONDS, PERIODIC_MAINTENANCE_SECONDS, PERIODIC_TARGET_DB_LOAD,
    ZSTD_PAYLOAD_COLUMNS, run_cooperative_maintenance_cycle,
};

pub(super) async fn run(
    crud_store: Arc<CrudStore>,
    cancellation: tokio_util::sync::CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(PERIODIC_MAINTENANCE_INTERVAL_SECONDS)) => {}
        }

        let outcome = match run_cooperative_maintenance_cycle(
            crud_store.as_ref(),
            ZSTD_PAYLOAD_COLUMNS,
            Some(PERIODIC_MAINTENANCE_SECONDS),
            PERIODIC_TARGET_DB_LOAD,
            &cancellation,
        )
        .await
        {
            Ok(outcome) if outcome.cancelled => return,
            Ok(outcome) if outcome.deferred => {
                debug!(
                    "sqlite-zstd periodic payload maintenance deferred for higher-priority writes"
                );
                continue;
            }
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "sqlite-zstd periodic payload maintenance failed"
                );
                continue;
            }
        };
        for summary in outcome.summaries.into_iter().filter(|summary| {
            summary.pending_before != 0
                || summary.pending_after != 0
                || summary.enabled_now
                || summary.maintenance_more_pending
        }) {
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
                "sqlite-zstd periodic payload maintenance completed"
            );
        }
    }
}
