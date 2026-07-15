use pioneer_crud::CrudStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::database::zstd_column::{
    PERIODIC_MAINTENANCE_INTERVAL_SECONDS, PERIODIC_MAINTENANCE_SECONDS, PERIODIC_TARGET_DB_LOAD,
    ZSTD_PAYLOAD_COLUMNS, run_periodic_maintenance_once,
};

pub(super) fn spawn(crud_store: Arc<CrudStore>) {
    let _handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(PERIODIC_MAINTENANCE_INTERVAL_SECONDS)).await;

            match run_periodic_maintenance_once(
                crud_store.as_ref(),
                ZSTD_PAYLOAD_COLUMNS,
                Some(PERIODIC_MAINTENANCE_SECONDS),
                PERIODIC_TARGET_DB_LOAD,
            )
            .await
            {
                Ok(outcome) => {
                    if outcome.deferred {
                        debug!(
                            "sqlite-zstd periodic payload maintenance deferred for foreground writes"
                        );
                    }
                    for summary in outcome
                        .summaries
                        .into_iter()
                        .filter(|summary| summary.pending_before != 0 || summary.pending_after != 0)
                    {
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
                            "sqlite-zstd periodic payload maintenance completed"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        error = %format!("{error:#}"),
                        "sqlite-zstd periodic payload maintenance failed"
                    );
                }
            }
        }
    });
}
