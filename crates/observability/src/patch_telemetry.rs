//! Dependency-neutral projection of Apply Patch's internal counters.

use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PatchTelemetrySnapshot {
    pub calls: u64,
    pub tool_successes: u64,
    pub tool_failures: u64,
    pub task_successes: u64,
    pub task_failures: u64,
    pub applied: u64,
    pub partial: u64,
    pub rejected: u64,
    pub failed: u64,
    pub uncertain: u64,
    pub committed_changes: u64,
    pub committed_files: u64,
    pub committed_hunks: u64,
    pub committed_bytes: u64,
    pub planned_files: u64,
    pub planned_hunks: u64,
    pub parse_latency_ns: u64,
    pub plan_latency_ns: u64,
    pub lock_latency_ns: u64,
    pub commit_latency_ns: u64,
    pub persist_latency_ns: u64,
    pub total_latency_ns: u64,
    pub tracker_publication_failures: u64,
    pub applied_record_appends: u64,
    pub applied_record_append_latency_ns: u64,
    pub projection_lag: u64,
    pub pending_ordinals: u64,
    pub duplicate_suppressions: u64,
    pub pending_tracking: u64,
    pub native_calls: u64,
    pub managed_calls: u64,
    pub untracked_calls: u64,
    pub exact_reports: u64,
    pub inexact_reports: u64,
    pub observed_guard_stale: u64,
    pub context_stale: u64,
    pub prepared_revalidation_stale: u64,
    pub commit_cas_stale: u64,
    pub parse_errors: u64,
    pub prepare_errors: u64,
    pub lock_errors: u64,
    pub commit_errors: u64,
    pub persist_errors: u64,
    pub snapshot_logical_bytes: u64,
    pub snapshot_physical_bytes: u64,
    pub snapshot_references: u64,
    pub snapshot_referenced_logical_bytes: u64,
    pub snapshot_dedup_ratio_ppm: u64,
    pub snapshot_compression_ratio_ppm: u64,
    pub snapshot_gc_blobs: u64,
    pub snapshot_gc_bytes: u64,
    pub shell_fallbacks: u64,
}

type SnapshotProvider = fn() -> PatchTelemetrySnapshot;

static SNAPSHOT_PROVIDER: OnceLock<SnapshotProvider> = OnceLock::new();

/// Connect the Apply Patch tool's process-local counters to observability.
/// Re-registering the same process-wide provider is intentionally idempotent.
pub fn register_patch_telemetry_snapshot_provider(provider: SnapshotProvider) {
    let _ = SNAPSHOT_PROVIDER.set(provider);
}

pub(crate) fn snapshot() -> PatchTelemetrySnapshot {
    SNAPSHOT_PROVIDER
        .get()
        .map(|provider| provider())
        .unwrap_or_default()
}
