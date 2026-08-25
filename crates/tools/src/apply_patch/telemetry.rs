//! Bounded, source-free Apply Patch tool telemetry.
//!
//! The mutation pipeline records counters and durations only.  No patch text,
//! file bytes or unrestricted diagnostics are retained here; callers can
//! export the snapshot to the application's metrics backend.

use crate::apply_patch::file_mutation::{GuardHorizon, PatchErrorCode, PatchStage};
use crate::apply_patch::{ExecutionReport, ExecutionStatus};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct PatchTelemetry {
    pub calls: AtomicU64,
    pub tool_successes: AtomicU64,
    pub tool_failures: AtomicU64,
    pub task_successes: AtomicU64,
    pub task_failures: AtomicU64,
    pub applied: AtomicU64,
    pub partial: AtomicU64,
    pub rejected: AtomicU64,
    pub failed: AtomicU64,
    pub uncertain: AtomicU64,
    pub committed_changes: AtomicU64,
    pub committed_files: AtomicU64,
    pub committed_hunks: AtomicU64,
    pub committed_bytes: AtomicU64,
    pub planned_files: AtomicU64,
    pub planned_hunks: AtomicU64,
    pub parse_latency_ns: AtomicU64,
    pub plan_latency_ns: AtomicU64,
    pub lock_latency_ns: AtomicU64,
    pub commit_latency_ns: AtomicU64,
    pub persist_latency_ns: AtomicU64,
    pub total_latency_ns: AtomicU64,
    pub tracker_publication_failures: AtomicU64,
    pub applied_record_appends: AtomicU64,
    pub applied_record_append_latency_ns: AtomicU64,
    pub projection_lag: AtomicU64,
    pub pending_ordinals: AtomicU64,
    pub duplicate_suppressions: AtomicU64,
    pub pending_tracking: AtomicU64,
    pub native_calls: AtomicU64,
    pub managed_calls: AtomicU64,
    pub untracked_calls: AtomicU64,
    pub exact_reports: AtomicU64,
    pub inexact_reports: AtomicU64,
    pub observed_guard_stale: AtomicU64,
    pub context_stale: AtomicU64,
    pub prepared_revalidation_stale: AtomicU64,
    pub commit_cas_stale: AtomicU64,
    pub parse_errors: AtomicU64,
    pub prepare_errors: AtomicU64,
    pub lock_errors: AtomicU64,
    pub commit_errors: AtomicU64,
    pub persist_errors: AtomicU64,
    pub snapshot_logical_bytes: AtomicU64,
    pub snapshot_physical_bytes: AtomicU64,
    pub snapshot_references: AtomicU64,
    pub snapshot_referenced_logical_bytes: AtomicU64,
    pub snapshot_dedup_ratio_ppm: AtomicU64,
    pub snapshot_compression_ratio_ppm: AtomicU64,
    pub snapshot_gc_blobs: AtomicU64,
    pub snapshot_gc_bytes: AtomicU64,
    pub shell_fallbacks: AtomicU64,
}

pub use pioneer_observability::PatchTelemetrySnapshot;

impl PatchTelemetry {
    pub fn record_report(&self, report: &ExecutionReport, elapsed: Duration) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if matches!(report.status, ExecutionStatus::Applied) {
            self.tool_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.tool_failures.fetch_add(1, Ordering::Relaxed);
        }
        match report.status {
            ExecutionStatus::Applied => {
                self.applied.fetch_add(1, Ordering::Relaxed);
            }
            ExecutionStatus::Partial => {
                self.partial.fetch_add(1, Ordering::Relaxed);
            }
            ExecutionStatus::Rejected => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
            }
            ExecutionStatus::Failed => {
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
            ExecutionStatus::CommitStateUncertain => {
                self.uncertain.fetch_add(1, Ordering::Relaxed);
                self.pending_tracking.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.committed_changes.fetch_add(
            report.delta.changes.len().try_into().unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.committed_files.fetch_add(
            report.delta.changes.len().try_into().unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        // A committed change is the smallest durable hunk unit available at
        // this layer.  Parser-level hunk totals are recorded separately via
        // record_plan; this counter remains conservative for Add/Replace/
        // Delete/Move and never invents source text or a diff.
        self.committed_hunks.fetch_add(
            report.delta.changes.len().try_into().unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.committed_bytes.fetch_add(
            report
                .delta
                .changes
                .iter()
                .flat_map(|change| {
                    [
                        change.before.as_ref(),
                        change.after.as_ref(),
                        change.overwritten_destination.as_ref(),
                    ]
                })
                .flatten()
                .map(|snapshot| snapshot.bytes.len() as u64)
                .sum(),
            Ordering::Relaxed,
        );
        if report.delta.exact {
            self.exact_reports.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inexact_reports.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(failure) = &report.failure {
            let target = match failure.stage {
                PatchStage::Normalize
                | PatchStage::Parse
                | PatchStage::Resolve
                | PatchStage::Authorize => &self.parse_errors,
                PatchStage::Prepare => &self.prepare_errors,
                PatchStage::Lock => &self.lock_errors,
                PatchStage::Stage | PatchStage::Commit => &self.commit_errors,
                PatchStage::Record | PatchStage::Recover => &self.persist_errors,
            };
            target.fetch_add(1, Ordering::Relaxed);
            if failure.code == PatchErrorCode::StaleFile {
                match failure.guard_horizon {
                    Some(GuardHorizon::Observed) => {
                        self.observed_guard_stale.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(GuardHorizon::Prepared) => {
                        self.prepared_revalidation_stale
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Some(GuardHorizon::Commit) => {
                        self.commit_cas_stale.fetch_add(1, Ordering::Relaxed);
                    }
                    None => {}
                };
            } else if matches!(
                failure.code,
                PatchErrorCode::ContextNotFound | PatchErrorCode::AmbiguousContext
            ) {
                self.context_stale.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.total_latency_ns.fetch_add(
            elapsed.as_nanos().try_into().unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn record_plan(&self, files: u64, hunks: u64) {
        self.planned_files.fetch_add(files, Ordering::Relaxed);
        self.planned_hunks.fetch_add(hunks, Ordering::Relaxed);
    }

    pub fn record_task_result(&self, success: bool) {
        let target = if success {
            &self.task_successes
        } else {
            &self.task_failures
        };
        target.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_applied_record_append(&self, elapsed: Duration) {
        self.applied_record_appends.fetch_add(1, Ordering::Relaxed);
        self.applied_record_append_latency_ns.fetch_add(
            elapsed.as_nanos().try_into().unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn record_projection_lag(&self) {
        self.projection_lag.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_ordinal(&self) {
        self.pending_ordinals.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_snapshot_metrics(
        &self,
        logical_bytes: u64,
        physical_bytes: u64,
        references: u64,
        referenced_logical_bytes: u64,
        gc_blobs: u64,
        gc_bytes: u64,
    ) {
        self.snapshot_logical_bytes
            .store(logical_bytes, Ordering::Relaxed);
        self.snapshot_physical_bytes
            .store(physical_bytes, Ordering::Relaxed);
        self.snapshot_references
            .store(references, Ordering::Relaxed);
        self.snapshot_referenced_logical_bytes
            .store(referenced_logical_bytes, Ordering::Relaxed);
        self.snapshot_dedup_ratio_ppm.store(
            ratio_ppm(referenced_logical_bytes, logical_bytes),
            Ordering::Relaxed,
        );
        self.snapshot_compression_ratio_ppm
            .store(ratio_ppm(physical_bytes, logical_bytes), Ordering::Relaxed);
        self.snapshot_gc_blobs.store(gc_blobs, Ordering::Relaxed);
        self.snapshot_gc_bytes.store(gc_bytes, Ordering::Relaxed);
    }

    pub fn record_authority(&self, authority: &str) {
        match authority {
            "native_patch_engine" => self.native_calls.fetch_add(1, Ordering::Relaxed),
            "managed_claude_patch_engine" => self.managed_calls.fetch_add(1, Ordering::Relaxed),
            _ => self.untracked_calls.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn record_shell_fallback(&self) {
        self.shell_fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_tracker_publication_failure(&self) {
        self.tracker_publication_failures
            .fetch_add(1, Ordering::Relaxed);
        self.pending_tracking.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_duplicate_suppression(&self) {
        self.duplicate_suppressions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_stage_latency(&self, stage: TelemetryStage, elapsed: Duration) {
        let target = match stage {
            TelemetryStage::Parse => &self.parse_latency_ns,
            TelemetryStage::Plan => &self.plan_latency_ns,
            TelemetryStage::Lock => &self.lock_latency_ns,
            TelemetryStage::Commit => &self.commit_latency_ns,
            TelemetryStage::Persist => &self.persist_latency_ns,
        };
        target.fetch_add(
            elapsed.as_nanos().try_into().unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn snapshot(&self) -> PatchTelemetrySnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        PatchTelemetrySnapshot {
            calls: load(&self.calls),
            tool_successes: load(&self.tool_successes),
            tool_failures: load(&self.tool_failures),
            task_successes: load(&self.task_successes),
            task_failures: load(&self.task_failures),
            applied: load(&self.applied),
            partial: load(&self.partial),
            rejected: load(&self.rejected),
            failed: load(&self.failed),
            uncertain: load(&self.uncertain),
            committed_changes: load(&self.committed_changes),
            committed_files: load(&self.committed_files),
            committed_hunks: load(&self.committed_hunks),
            committed_bytes: load(&self.committed_bytes),
            planned_files: load(&self.planned_files),
            planned_hunks: load(&self.planned_hunks),
            parse_latency_ns: load(&self.parse_latency_ns),
            plan_latency_ns: load(&self.plan_latency_ns),
            lock_latency_ns: load(&self.lock_latency_ns),
            commit_latency_ns: load(&self.commit_latency_ns),
            persist_latency_ns: load(&self.persist_latency_ns),
            total_latency_ns: load(&self.total_latency_ns),
            tracker_publication_failures: load(&self.tracker_publication_failures),
            applied_record_appends: load(&self.applied_record_appends),
            applied_record_append_latency_ns: load(&self.applied_record_append_latency_ns),
            projection_lag: load(&self.projection_lag),
            pending_ordinals: load(&self.pending_ordinals),
            duplicate_suppressions: load(&self.duplicate_suppressions),
            pending_tracking: load(&self.pending_tracking),
            native_calls: load(&self.native_calls),
            managed_calls: load(&self.managed_calls),
            untracked_calls: load(&self.untracked_calls),
            exact_reports: load(&self.exact_reports),
            inexact_reports: load(&self.inexact_reports),
            observed_guard_stale: load(&self.observed_guard_stale),
            context_stale: load(&self.context_stale),
            prepared_revalidation_stale: load(&self.prepared_revalidation_stale),
            commit_cas_stale: load(&self.commit_cas_stale),
            parse_errors: load(&self.parse_errors),
            prepare_errors: load(&self.prepare_errors),
            lock_errors: load(&self.lock_errors),
            commit_errors: load(&self.commit_errors),
            persist_errors: load(&self.persist_errors),
            snapshot_logical_bytes: load(&self.snapshot_logical_bytes),
            snapshot_physical_bytes: load(&self.snapshot_physical_bytes),
            snapshot_references: load(&self.snapshot_references),
            snapshot_referenced_logical_bytes: load(&self.snapshot_referenced_logical_bytes),
            snapshot_dedup_ratio_ppm: load(&self.snapshot_dedup_ratio_ppm),
            snapshot_compression_ratio_ppm: load(&self.snapshot_compression_ratio_ppm),
            snapshot_gc_blobs: load(&self.snapshot_gc_blobs),
            snapshot_gc_bytes: load(&self.snapshot_gc_bytes),
            shell_fallbacks: load(&self.shell_fallbacks),
        }
    }
}

fn ratio_ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        0
    } else {
        numerator
            .saturating_mul(1_000_000)
            .checked_div(denominator)
            .unwrap_or(u64::MAX)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetryStage {
    Parse,
    Plan,
    Lock,
    Commit,
    Persist,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{PatchDiagnostic, Retryability};
    use crate::apply_patch::history::AppliedPatchDelta;
    use crate::apply_patch::{ExecutionReport, ExecutionStatus};

    fn rejection(code: PatchErrorCode, guard_horizon: Option<GuardHorizon>) -> ExecutionReport {
        ExecutionReport {
            status: ExecutionStatus::Rejected,
            delta: AppliedPatchDelta::empty(),
            failure: Some(PatchDiagnostic {
                code,
                stage: PatchStage::Prepare,
                message: "bounded diagnostic".to_owned(),
                retryability: if code == PatchErrorCode::StaleFile {
                    Retryability::RetryAfterRead
                } else {
                    Retryability::Never
                },
                operation_index: Some(0),
                path: Some("file.txt".to_owned()),
                guard_horizon,
            }),
        }
    }

    #[test]
    fn stale_counters_use_structured_decision_horizons() {
        let telemetry = PatchTelemetry::default();
        telemetry.record_report(
            &rejection(PatchErrorCode::StaleFile, Some(GuardHorizon::Observed)),
            Duration::ZERO,
        );
        telemetry.record_report(
            &rejection(PatchErrorCode::ContextNotFound, None),
            Duration::ZERO,
        );
        telemetry.record_report(
            &rejection(PatchErrorCode::StaleFile, Some(GuardHorizon::Prepared)),
            Duration::ZERO,
        );
        telemetry.record_report(
            &rejection(PatchErrorCode::StaleFile, Some(GuardHorizon::Commit)),
            Duration::ZERO,
        );

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.observed_guard_stale, 1);
        assert_eq!(snapshot.context_stale, 1);
        assert_eq!(snapshot.prepared_revalidation_stale, 1);
        assert_eq!(snapshot.commit_cas_stale, 1);
    }
}
