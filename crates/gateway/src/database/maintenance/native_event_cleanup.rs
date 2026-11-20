use pioneer_crud::{CrudStore, NativeEventCleanupMetrics};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

/// Drain the jobs published by committed native-event and lifecycle writes.
pub(super) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let database = store.database_connection();
    let mut wake = database.work_signal("maintenance.native").subscribe();
    let mut last_log = Instant::now();
    let mut progress = Progress::default();
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = wake.begin_pass(&database) => if result.is_err() {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {},
                }
                continue;
            },
        }
        let started = Instant::now();
        let result: anyhow::Result<bool> = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = crate::database::attribution::scope_database_workload_result(
                pioneer_observability::DatabaseWorkload::NativeEventCleanup, pioneer_observability::DatabaseOperationSite::NativeEventCleanupQuantum,
                async {
                    let metrics = store.cleanup_native_events_quantum().await?;
                    let empty = metrics.jobs_examined == 0;
                    progress.add(metrics);
                    Ok(empty)
                },
            ) => result,
        };
        progress.quanta += 1;
        let idle = matches!(&result, Ok(true));
        let pause = match result {
            Ok(empty) if empty => Duration::from_secs(60),
            Ok(_) => started
                .elapsed()
                .saturating_mul(9)
                .max(Duration::from_millis(25)),
            Err(_) => {
                progress.failures += 1;
                tracing::warn!(reason = "database_error", "native event cleanup deferred");
                Duration::from_secs(60)
            }
        };
        if last_log.elapsed() >= Duration::from_secs(30) || pause == Duration::from_secs(60) {
            tracing::info!(
                quanta = progress.quanta,
                failures = progress.failures,
                jobs_examined = progress.metrics.jobs_examined,
                candidate_rows_fetched = progress.metrics.candidate_rows_fetched,
                events_selected = progress.metrics.events_selected,
                events_revalidated = progress.metrics.events_revalidated,
                rows_deleted = progress.metrics.events_deleted,
                selected_bytes = progress.metrics.selected_bytes,
                deleted_bytes = progress.metrics.deleted_bytes,
                errors_deferred = progress.metrics.errors_deferred,
                prepare_reads = progress.metrics.prepare_reads,
                apply_reads = progress.metrics.apply_reads,
                apply_writes = progress.metrics.apply_writes,
                queue_rows_changed = progress.metrics.queue_rows_changed,
                scheduler_rows_changed = progress.metrics.scheduler_rows_changed,
                prepare_elapsed_us = progress.metrics.prepare_elapsed_us,
                apply_elapsed_us = progress.metrics.apply_elapsed_us,
                "native event cleanup progress"
            );
            progress = Progress::default();
            last_log = Instant::now();
        }
        if idle {
            let deadline = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                result = store.native_event_cleanup_deadline() => result,
            };
            if let Ok(deadline) = deadline {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = wake.wait(deadline) => {},
                }
                continue;
            }
        }
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(pause) => {},
        }
    }
}

#[derive(Default)]
struct Progress {
    quanta: u64,
    failures: u64,
    metrics: NativeEventCleanupMetrics,
}
impl Progress {
    fn add(&mut self, m: NativeEventCleanupMetrics) {
        macro_rules! add { ($($field:ident),*) => { $(self.metrics.$field += m.$field;)* }; }
        add!(
            jobs_examined,
            candidate_rows_fetched,
            events_selected,
            selected_bytes,
            events_revalidated,
            events_deleted,
            deleted_bytes,
            prepare_reads,
            apply_reads,
            apply_writes,
            queue_rows_changed,
            scheduler_rows_changed,
            errors_deferred,
            prepare_elapsed_us,
            apply_elapsed_us
        );
    }
}
