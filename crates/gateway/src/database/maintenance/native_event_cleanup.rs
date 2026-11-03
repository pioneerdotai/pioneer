use pioneer_crud::{CrudStore, NativeEventCleanupMetrics};
use pioneer_sqlite::background::{Plan, Ready};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[derive(Default)]
pub(super) struct IdleBarrier {
    pub(super) armed: std::sync::atomic::AtomicBool,
    pub(super) entered: tokio::sync::Notify,
    pub(super) release: tokio::sync::Notify,
}

/// Bootstrap and cleanup alternate bounded portions, so neither a large legacy
/// table nor a continuous live backlog monopolizes this maintenance worker.
pub(super) async fn run(store: Arc<CrudStore>, cancellation: CancellationToken) {
    let store = store.with_maintenance_access();
    let work = store.background_work("storage.native_events");
    let Some(mut owner) = work.acquire() else {
        return;
    };
    let mut bootstrap_pending = true;
    let mut bootstrap_next = true;
    let mut last_log = Instant::now();
    let mut progress = Progress::default();
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
            // Bootstrap's durable completion marker remains authoritative;
            // actual cleanup writes settle before a new queue observation.
            if store
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
        let started = Instant::now();
        let bootstrap_phase = bootstrap_pending && bootstrap_next;
        bootstrap_next = !bootstrap_next;
        // Do not abandon dispatched SQLite work with a local per-query timeout.
        // Cancellation is the worker owner's shutdown signal, not a poll timeout.
        let result: anyhow::Result<bool> = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = owner.scope(crate::database::attribution::scope_database_workload_result(
                pioneer_observability::DatabaseWorkload::NativeEventCleanup, pioneer_observability::DatabaseOperationSite::NativeEventCleanupQuantum,
                async {
                    if bootstrap_phase {
                        let outcome = store.bootstrap_native_event_cleanup_quantum().await?;
                        progress.bootstrap_rows += outcome.rows_scanned;
                        progress.bootstrap_jobs += outcome.jobs_inserted;
                        progress.bootstrap_elapsed_us += micros(started);
                        bootstrap_pending = !outcome.complete;
                        // Always offer a cleanup quantum after the final page;
                        // only empty queue discovery earns the idle sleep.
                        Ok(false)
                    } else {
                        let metrics = store.cleanup_native_events_quantum().await?;
                        let empty = metrics.jobs_examined == 0;
                        progress.add(metrics);
                        Ok(empty)
                    }
                },
            )) => result,
        };
        progress.quanta += 1;
        let plan = match result {
            Ok(empty) if empty && !bootstrap_pending => {
                match store.next_native_event_cleanup_deadline().await {
                    Ok(Some(at)) => Plan::WallDeadline(at.into()),
                    Ok(None) => Plan::Dormant,
                    Err(_) => {
                        progress.failures += 1;
                        tracing::warn!(
                            reason = "database_error",
                            phase = "deadline",
                            "native event cleanup deferred"
                        );
                        Plan::MonotonicDeadline(
                            tokio::time::Instant::now() + Duration::from_secs(60),
                        )
                    }
                }
            }
            Ok(_) => Plan::MonotonicDeadline(
                tokio::time::Instant::now()
                    + started
                        .elapsed()
                        .saturating_mul(9)
                        .max(Duration::from_millis(25)),
            ),
            Err(_) => {
                progress.failures += 1;
                tracing::warn!(
                    reason = "database_error",
                    phase = if bootstrap_phase {
                        "bootstrap"
                    } else {
                        "cleanup"
                    },
                    "native event cleanup deferred"
                );
                Plan::MonotonicDeadline(tokio::time::Instant::now() + Duration::from_secs(60))
            }
        };
        if last_log.elapsed() >= Duration::from_secs(30)
            || matches!(plan, Plan::Dormant | Plan::WallDeadline(_))
        {
            tracing::info!(
                bootstrap_complete = !bootstrap_pending,
                bootstrap_rows = progress.bootstrap_rows,
                bootstrap_jobs = progress.bootstrap_jobs,
                bootstrap_elapsed_us = progress.bootstrap_elapsed_us,
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
        let published = owner.publish(observed, plan);
        if !published && let Plan::MonotonicDeadline(at) = plan {
            owner.retry_after(
                observed,
                at.saturating_duration_since(tokio::time::Instant::now()),
            );
        }
        #[cfg(test)]
        if published && matches!(plan, Plan::Dormant) {
            let barrier = store.database_connection().shared_resource::<IdleBarrier>();
            if barrier
                .armed
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                barrier.entered.notify_one();
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = barrier.release.notified() => {},
                }
            }
        }
    }
}

fn micros(start: Instant) -> u64 {
    start.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
}
#[derive(Default)]
struct Progress {
    bootstrap_rows: u64,
    bootstrap_jobs: u64,
    bootstrap_elapsed_us: u64,
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
