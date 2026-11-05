use pioneer_crud::CrudStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::database::zstd_column::{
    PERIODIC_BACKLOG_RECHECK_MILLIS, PERIODIC_MAINTENANCE_INTERVAL_SECONDS,
    PERIODIC_MAINTENANCE_SECONDS, PERIODIC_TARGET_DB_LOAD, PreparedDictionaryCache,
    ZSTD_PAYLOAD_COLUMNS, run_cooperative_maintenance_cycle,
};

pub(crate) async fn run(
    crud_store: Arc<CrudStore>,
    cancellation: tokio_util::sync::CancellationToken,
    mut cache: PreparedDictionaryCache,
) {
    let database = crud_store.database_connection();
    let mut wake = database.work_signal("maintenance.zstd").subscribe();
    loop {
        if cancellation.is_cancelled() {
            return;
        }
        let pass = tokio::select! {
            _ = cancellation.cancelled() => return,
            pass = wake.begin_pass(&database) => pass,
        };
        if pass.is_err() {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(PERIODIC_MAINTENANCE_INTERVAL_SECONDS)) => {},
            }
            continue;
        }
        let mut made_progress_with_backlog = false;
        let mut idle = false;
        match run_cooperative_maintenance_cycle(
            crud_store.as_ref(),
            &mut cache,
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
            }
            Ok(outcome) => {
                idle = outcome
                    .summaries
                    .iter()
                    .all(|summary| !summary.maintenance_runnable);
                for summary in outcome.summaries {
                    made_progress_with_backlog |= summary.maintenance_runnable;
                    if summary.pending_before == 0
                        && summary.pending_after == 0
                        && !summary.enabled_now
                        && !summary.maintenance_more_pending
                    {
                        continue;
                    }
                    info!(
                        table = summary.table,
                        column = summary.column,
                        enabled_now = summary.enabled_now,
                        already_enabled = summary.already_enabled,
                        skipped_empty = summary.skipped_empty,
                        bounded_rows_observed = summary.pending_before,
                        bounded_rows_remaining = summary.pending_after,
                        compressed_rows = summary.compressed_rows,
                        stale_rows = summary.stale_rows,
                        source_bytes = summary.source_bytes,
                        maintenance_more_pending = summary.maintenance_more_pending,
                        counts_exact = summary.counts_exact,
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

        #[cfg(test)]
        if let Some(after_cycle) = cache.after_cycle.as_ref() {
            after_cycle(&cache);
        }

        if idle {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = wake.wait(None) => {},
            }
            continue;
        }
        let delay = if made_progress_with_backlog {
            Duration::from_millis(PERIODIC_BACKLOG_RECHECK_MILLIS)
        } else {
            Duration::from_secs(PERIODIC_MAINTENANCE_INTERVAL_SECONDS)
        };
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}
