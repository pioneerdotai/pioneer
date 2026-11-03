use pioneer_crud::CrudStore;
use pioneer_sqlite::background::{Plan, Ready};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::database::zstd_column::{
    PERIODIC_BACKLOG_RECHECK_MILLIS, PERIODIC_MAINTENANCE_SECONDS, PERIODIC_TARGET_DB_LOAD,
    PreparedDictionaryCache, ZSTD_PAYLOAD_COLUMNS, run_cooperative_maintenance_cycle,
};

pub(crate) async fn run(
    crud_store: Arc<CrudStore>,
    cancellation: tokio_util::sync::CancellationToken,
    mut cache: PreparedDictionaryCache,
) {
    let crud_store = crud_store.with_maintenance_access();
    let work = crud_store.background_work("storage.zstd");
    let Some(mut owner) = work.acquire() else {
        return;
    };
    let mut previous = None;
    loop {
        let ready = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            ready = owner.ready() => ready,
        };
        if ready == Ready::Closed {
            return;
        }
        if ready == Ready::SettlementRequired {
            let settle_observation = owner.observe();
            if crud_store
                .database_connection()
                .settle_background_change(&owner)
                .await
                .is_err()
            {
                owner.retry_after(settle_observation, Duration::from_secs(60));
            }
            continue;
        }
        let observed = owner.observe();
        if previous.is_some_and(|prior| owner.producer_changed_since(prior)) {
            cache.rediscover_backlog();
        }
        previous = Some(observed);
        let mut retry = false;
        if cancellation.is_cancelled() {
            return;
        }
        let mut made_progress_with_backlog = false;
        match owner
            .scope(run_cooperative_maintenance_cycle(
                &crud_store,
                &mut cache,
                ZSTD_PAYLOAD_COLUMNS,
                Some(PERIODIC_MAINTENANCE_SECONDS),
                PERIODIC_TARGET_DB_LOAD,
                &cancellation,
            ))
            .await
        {
            Ok(outcome) if outcome.cancelled => return,
            Ok(outcome) if outcome.deferred => {
                retry = true;
                debug!(
                    "sqlite-zstd periodic payload maintenance deferred for higher-priority writes"
                );
            }
            Ok(outcome) => {
                made_progress_with_backlog |= outcome.continuation;
                for summary in outcome.summaries {
                    made_progress_with_backlog |= summary.maintenance_more_pending
                        && summary.pending_before > summary.pending_after;
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
                retry = true;
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

        if retry || owner.requires_settlement() {
            // Actual storage/capacity error recovery, never successful Empty.
            owner.retry_after(observed, Duration::from_secs(60));
        } else {
            let plan = if made_progress_with_backlog {
                Plan::MonotonicDeadline(
                    tokio::time::Instant::now()
                        + Duration::from_millis(PERIODIC_BACKLOG_RECHECK_MILLIS),
                )
            } else {
                // Small training input waits for another actual payload commit;
                // it cannot become compressible merely by waiting five minutes.
                Plan::Dormant
            };
            let published = owner.publish(observed, plan);
            #[cfg(test)]
            if published && let Some(barrier) = cache.after_publish.as_ref() {
                tokio::select! { _ = cancellation.cancelled() => return, _ = barrier.wait() => {} }
                tokio::select! { _ = cancellation.cancelled() => return, _ = barrier.wait() => {} }
            }
            #[cfg(not(test))]
            let _ = published;
        }
    }
}
