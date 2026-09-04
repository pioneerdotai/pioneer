use opentelemetry::KeyValue;
use opentelemetry::trace::{Span, SpanBuilder, SpanKind, Status, Tracer};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_FINGERPRINTS_PER_WORK_UNIT: usize = 64;
const QUERY_COUNT_ANOMALY_THRESHOLD: u64 = 32;
const QUERY_DURATION_ANOMALY_THRESHOLD: Duration = Duration::from_millis(100);
const WORK_UNIT_DURATION_ANOMALY_THRESHOLD: Duration = Duration::from_millis(500);
const ANOMALY_TRACE_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_OTEL_METRIC_CARDINALITY_LIMIT: usize = 2_000;
const DATABASE_WORKLOAD_OUTCOME_CARDINALITY: usize = 3;
const DATABASE_WORKLOAD_ANOMALY_REASON_CARDINALITY: usize = 4;
const GATEWAY_LOAD_BUCKET_CARDINALITY: usize = 5;
const DATABASE_SIZE_BUCKET_CARDINALITY: usize = 6;

/// Stable, bounded owners of Gateway database work. Values are deliberately
/// independent from request, workspace, thread, Turn and installation IDs.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum DatabaseWorkload {
    Unclassified,
    AgentProgress,
    TurnEventCommit,
    ProjectionRecovery,
    FinalizationRecovery,
    EventDelivery,
    TaskReconcile,
    ExecutionSupervision,
    TaskEventFanout,
    HookRecovery,
    TimelinePage,
    ThreadTreeLoad,
    SkillsWatch,
    EpisodicMaintenance,
    ZstdMaintenance,
}

impl DatabaseWorkload {
    pub const CARDINALITY: usize = 15;

    const fn index(self) -> usize {
        match self {
            Self::Unclassified => 0,
            Self::AgentProgress => 1,
            Self::TurnEventCommit => 2,
            Self::ProjectionRecovery => 3,
            Self::FinalizationRecovery => 4,
            Self::EventDelivery => 5,
            Self::TaskReconcile => 6,
            Self::ExecutionSupervision => 7,
            Self::TaskEventFanout => 8,
            Self::HookRecovery => 9,
            Self::TimelinePage => 10,
            Self::ThreadTreeLoad => 11,
            Self::SkillsWatch => 12,
            Self::EpisodicMaintenance => 13,
            Self::ZstdMaintenance => 14,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unclassified => "unclassified",
            Self::AgentProgress => "agent.progress",
            Self::TurnEventCommit => "turn_event.commit_projection",
            Self::ProjectionRecovery => "projection.recovery",
            Self::FinalizationRecovery => "finalization.recovery",
            Self::EventDelivery => "event.delivery",
            Self::TaskReconcile => "task.reconcile",
            Self::ExecutionSupervision => "execution.supervision",
            Self::TaskEventFanout => "task_event.fanout",
            Self::HookRecovery => "hook.recovery",
            Self::TimelinePage => "timeline.page",
            Self::ThreadTreeLoad => "thread_tree.load",
            Self::SkillsWatch => "skills.watch",
            Self::EpisodicMaintenance => "episodic.maintenance",
            Self::ZstdMaintenance => "zstd.maintenance",
        }
    }
}

const _: () = assert!(
    DatabaseWorkload::CARDINALITY
        * DATABASE_WORKLOAD_OUTCOME_CARDINALITY
        * GATEWAY_LOAD_BUCKET_CARDINALITY
        * DATABASE_SIZE_BUCKET_CARDINALITY
        < DEFAULT_OTEL_METRIC_CARDINALITY_LIMIT
);
const _: () = assert!(
    DatabaseWorkload::CARDINALITY
        * DATABASE_WORKLOAD_ANOMALY_REASON_CARDINALITY
        * GATEWAY_LOAD_BUCKET_CARDINALITY
        * DATABASE_SIZE_BUCKET_CARDINALITY
        < DEFAULT_OTEL_METRIC_CARDINALITY_LIMIT
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseQueryKind {
    Read,
    Write,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseWorkloadOutcome {
    Ok,
    Error,
    Cancelled,
}

impl DatabaseWorkloadOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Default)]
struct DatabaseWorkloadQueryStats {
    query_count: u64,
    read_count: u64,
    write_count: u64,
    query_duration_nanos: u64,
    fingerprint_counts: HashMap<u64, u64>,
    fingerprint_overflow_count: u64,
}

impl DatabaseWorkloadQueryStats {
    fn record(&mut self, fingerprint: u64, kind: DatabaseQueryKind, elapsed: Duration) {
        self.query_count = self.query_count.saturating_add(1);
        match kind {
            DatabaseQueryKind::Read => self.read_count = self.read_count.saturating_add(1),
            DatabaseQueryKind::Write => self.write_count = self.write_count.saturating_add(1),
            DatabaseQueryKind::Other => {}
        }
        self.query_duration_nanos = self
            .query_duration_nanos
            .saturating_add(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));

        if let Some(count) = self.fingerprint_counts.get_mut(&fingerprint) {
            *count = count.saturating_add(1);
        } else if self.fingerprint_counts.len() < MAX_FINGERPRINTS_PER_WORK_UNIT {
            self.fingerprint_counts.insert(fingerprint, 1);
        } else {
            self.fingerprint_overflow_count = self.fingerprint_overflow_count.saturating_add(1);
        }
    }

    fn top_fingerprint(&self) -> (u64, u64) {
        let observed = self
            .fingerprint_counts
            .iter()
            .map(|(fingerprint, count)| (*fingerprint, *count))
            .chain(std::iter::once((0, self.fingerprint_overflow_count)));
        observed.max_by_key(|(_, count)| *count).unwrap_or((0, 0))
    }
}

#[derive(Debug)]
struct DatabaseWorkloadState {
    workload: DatabaseWorkload,
    query_stats: Mutex<DatabaseWorkloadQueryStats>,
}

impl DatabaseWorkloadState {
    fn lock(&self) -> MutexGuard<'_, DatabaseWorkloadQueryStats> {
        self.query_stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Cloneable context installed around one logical unit of database-capable
/// work. The SQL callback updates it synchronously after each statement.
#[derive(Clone, Debug)]
pub struct DatabaseWorkloadContext {
    state: Arc<DatabaseWorkloadState>,
}

impl DatabaseWorkloadContext {
    pub fn workload(&self) -> DatabaseWorkload {
        self.state.workload
    }

    pub fn record_query(&self, fingerprint: u64, kind: DatabaseQueryKind, elapsed: Duration) {
        self.state.lock().record(fingerprint, kind, elapsed);
    }
}

/// One logical work-unit recorder. Ordinary work emits only aggregate metrics;
/// anomalous units emit at most one bounded trace per workload every 30 seconds.
#[must_use = "database workload traces must be completed"]
pub struct DatabaseWorkloadTrace {
    context: DatabaseWorkloadContext,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    started_instant: Instant,
    finished: bool,
}

impl DatabaseWorkloadTrace {
    pub fn start(workload: DatabaseWorkload) -> Self {
        let (telemetry_enabled, consent_generation) = super::telemetry_consent_snapshot();
        Self {
            context: DatabaseWorkloadContext {
                state: Arc::new(DatabaseWorkloadState {
                    workload,
                    query_stats: Mutex::new(DatabaseWorkloadQueryStats::default()),
                }),
            },
            consent_generation: telemetry_enabled.then_some(consent_generation),
            started_at: SystemTime::now(),
            started_instant: Instant::now(),
            finished: false,
        }
    }

    pub fn context(&self) -> DatabaseWorkloadContext {
        self.context.clone()
    }

    pub fn finish_success(mut self) {
        self.finish(DatabaseWorkloadOutcome::Ok);
    }

    pub fn finish_error(mut self) {
        self.finish(DatabaseWorkloadOutcome::Error);
    }

    pub fn finish_cancelled(mut self) {
        self.finish(DatabaseWorkloadOutcome::Cancelled);
    }

    fn finish(&mut self, outcome: DatabaseWorkloadOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        let stats = {
            let mut stats = self.context.state.lock();
            std::mem::take(&mut *stats)
        };
        emit_database_workload(DatabaseWorkloadSnapshot {
            workload: self.context.workload(),
            consent_generation: self.consent_generation,
            started_at: self.started_at,
            elapsed: self.started_instant.elapsed(),
            outcome,
            stats,
        });
    }
}

impl Drop for DatabaseWorkloadTrace {
    fn drop(&mut self) {
        self.finish(DatabaseWorkloadOutcome::Cancelled);
    }
}

struct DatabaseWorkloadSnapshot {
    workload: DatabaseWorkload,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    elapsed: Duration,
    outcome: DatabaseWorkloadOutcome,
    stats: DatabaseWorkloadQueryStats,
}

static LAST_ANOMALY_TRACE_MILLIS: [AtomicU64; DatabaseWorkload::CARDINALITY] =
    [const { AtomicU64::new(0) }; DatabaseWorkload::CARDINALITY];

fn emit_database_workload(snapshot: DatabaseWorkloadSnapshot) {
    if !super::telemetry_sample_allowed(snapshot.consent_generation) {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    if state.target != super::telemetry::TelemetryTarget::Gateway {
        return;
    }
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };

    let mut attributes = vec![
        KeyValue::new("db.workload.name", snapshot.workload.as_str()),
        KeyValue::new("outcome", snapshot.outcome.as_str()),
    ];
    attributes.extend(super::metrics::database_runtime_metric_attributes());
    metrics.database_workload_operations.add(1, &attributes);
    metrics
        .database_workload_duration
        .record(snapshot.elapsed.as_secs_f64() * 1_000.0, &attributes);
    metrics
        .database_workload_query_count
        .record(snapshot.stats.query_count, &attributes);
    metrics.database_workload_query_duration.record(
        Duration::from_nanos(snapshot.stats.query_duration_nanos).as_secs_f64() * 1_000.0,
        &attributes,
    );
    let Some(reason) = anomaly_reason(&snapshot) else {
        return;
    };
    let mut anomaly_attributes = vec![
        KeyValue::new("db.workload.name", snapshot.workload.as_str()),
        KeyValue::new("db.workload.anomaly.reason", reason),
    ];
    anomaly_attributes.extend(super::metrics::database_runtime_metric_attributes());
    metrics
        .database_workload_anomalies
        .add(1, &anomaly_attributes);
    if !reserve_anomaly_trace(snapshot.workload) {
        return;
    }

    let (top_fingerprint, top_fingerprint_count) = snapshot.stats.top_fingerprint();
    anomaly_attributes.extend([
        KeyValue::new("outcome", snapshot.outcome.as_str()),
        KeyValue::new(
            "db.query.count",
            i64::try_from(snapshot.stats.query_count).unwrap_or(i64::MAX),
        ),
        KeyValue::new(
            "db.query.read_count",
            i64::try_from(snapshot.stats.read_count).unwrap_or(i64::MAX),
        ),
        KeyValue::new(
            "db.query.write_count",
            i64::try_from(snapshot.stats.write_count).unwrap_or(i64::MAX),
        ),
        KeyValue::new(
            "db.query.duration_ms",
            Duration::from_nanos(snapshot.stats.query_duration_nanos).as_secs_f64() * 1_000.0,
        ),
        KeyValue::new(
            "db.query.top_fingerprint",
            fingerprint_attribute_value(top_fingerprint),
        ),
        KeyValue::new(
            "db.query.top_fingerprint_count",
            i64::try_from(top_fingerprint_count).unwrap_or(i64::MAX),
        ),
    ]);
    anomaly_attributes.extend(super::metrics::database_runtime_trace_attributes());
    let builder = SpanBuilder::from_name("gateway.database.workload.anomaly")
        .with_kind(SpanKind::Internal)
        .with_start_time(snapshot.started_at)
        .with_attributes(anomaly_attributes);
    let mut span = state.tracer.build(builder);
    span.set_status(match snapshot.outcome {
        DatabaseWorkloadOutcome::Error => Status::Error {
            description: std::borrow::Cow::Borrowed("database workload failed"),
        },
        DatabaseWorkloadOutcome::Cancelled => Status::Unset,
        DatabaseWorkloadOutcome::Ok => Status::Ok,
    });
    span.end_with_timestamp(
        snapshot
            .started_at
            .checked_add(snapshot.elapsed)
            .unwrap_or_else(SystemTime::now),
    );
}

fn anomaly_reason(snapshot: &DatabaseWorkloadSnapshot) -> Option<&'static str> {
    if snapshot.outcome == DatabaseWorkloadOutcome::Error {
        Some("error")
    } else if snapshot.stats.query_count >= QUERY_COUNT_ANOMALY_THRESHOLD {
        Some("query_amplification")
    } else if Duration::from_nanos(snapshot.stats.query_duration_nanos)
        >= QUERY_DURATION_ANOMALY_THRESHOLD
    {
        Some("database_time")
    } else if snapshot.elapsed >= WORK_UNIT_DURATION_ANOMALY_THRESHOLD {
        Some("latency")
    } else {
        None
    }
}

fn reserve_anomaly_trace(workload: DatabaseWorkload) -> bool {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let now_millis = u64::try_from(now_millis).unwrap_or(u64::MAX);
    let interval_millis = u64::try_from(ANOMALY_TRACE_INTERVAL.as_millis()).unwrap_or(u64::MAX);
    let slot = &LAST_ANOMALY_TRACE_MILLIS[workload.index()];
    let mut previous = slot.load(Ordering::Acquire);
    loop {
        if previous != 0 && now_millis.saturating_sub(previous) < interval_millis {
            return false;
        }
        match slot.compare_exchange_weak(previous, now_millis, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return true,
            Err(actual) => previous = actual,
        }
    }
}

pub(crate) fn fingerprint_attribute_value(fingerprint: u64) -> i64 {
    i64::try_from(fingerprint & i64::MAX as u64).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_names_are_stable_and_bounded() {
        let workloads = [
            DatabaseWorkload::Unclassified,
            DatabaseWorkload::AgentProgress,
            DatabaseWorkload::TurnEventCommit,
            DatabaseWorkload::ProjectionRecovery,
            DatabaseWorkload::FinalizationRecovery,
            DatabaseWorkload::EventDelivery,
            DatabaseWorkload::TaskReconcile,
            DatabaseWorkload::ExecutionSupervision,
            DatabaseWorkload::TaskEventFanout,
            DatabaseWorkload::HookRecovery,
            DatabaseWorkload::TimelinePage,
            DatabaseWorkload::ThreadTreeLoad,
            DatabaseWorkload::SkillsWatch,
            DatabaseWorkload::EpisodicMaintenance,
            DatabaseWorkload::ZstdMaintenance,
        ];
        assert_eq!(workloads.len(), DatabaseWorkload::CARDINALITY);
        assert_eq!(
            workloads.map(DatabaseWorkload::as_str),
            [
                "unclassified",
                "agent.progress",
                "turn_event.commit_projection",
                "projection.recovery",
                "finalization.recovery",
                "event.delivery",
                "task.reconcile",
                "execution.supervision",
                "task_event.fanout",
                "hook.recovery",
                "timeline.page",
                "thread_tree.load",
                "skills.watch",
                "episodic.maintenance",
                "zstd.maintenance",
            ]
        );
    }

    #[test]
    fn query_stats_bound_distinct_fingerprints_and_keep_an_overflow_bucket() {
        let mut stats = DatabaseWorkloadQueryStats::default();
        for fingerprint in 1..=u64::try_from(MAX_FINGERPRINTS_PER_WORK_UNIT + 5).unwrap() {
            stats.record(
                fingerprint,
                DatabaseQueryKind::Read,
                Duration::from_millis(1),
            );
        }
        assert_eq!(
            stats.fingerprint_counts.len(),
            MAX_FINGERPRINTS_PER_WORK_UNIT
        );
        assert_eq!(stats.fingerprint_overflow_count, 5);
        assert_eq!(stats.query_count, 69);
        assert_eq!(stats.read_count, 69);
        assert_eq!(stats.write_count, 0);
    }

    #[test]
    fn anomaly_classification_prioritizes_failure_and_query_amplification() {
        let snapshot = |outcome: DatabaseWorkloadOutcome,
                        query_count: u64,
                        query_duration: Duration,
                        elapsed: Duration| DatabaseWorkloadSnapshot {
            workload: DatabaseWorkload::ProjectionRecovery,
            consent_generation: None,
            started_at: SystemTime::now(),
            elapsed,
            outcome,
            stats: DatabaseWorkloadQueryStats {
                query_count,
                query_duration_nanos: u64::try_from(query_duration.as_nanos()).unwrap_or(u64::MAX),
                ..Default::default()
            },
        };
        assert_eq!(
            anomaly_reason(&snapshot(
                DatabaseWorkloadOutcome::Error,
                100,
                Duration::ZERO,
                Duration::ZERO,
            )),
            Some("error")
        );
        assert_eq!(
            anomaly_reason(&snapshot(
                DatabaseWorkloadOutcome::Ok,
                QUERY_COUNT_ANOMALY_THRESHOLD,
                Duration::ZERO,
                Duration::ZERO,
            )),
            Some("query_amplification")
        );
    }
}
