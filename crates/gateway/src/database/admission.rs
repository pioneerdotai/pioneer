use pioneer_observability::queues::{QueueBytes, QueueKind, QueueObserver, QueueOutcome};
use std::sync::Arc;

use pioneer_observability::{
    DatabaseAdmissionMetric, DatabaseReadAdmissionMetric, DatabaseReadMetric,
    record_database_admission, record_database_read, record_database_read_admission,
};
use pioneer_sqlite::{
    SqliteAdmissionSnapshot, SqliteReadEvent, SqliteReadObserver, SqliteWriteEvent,
    SqliteWriteObserver, SqliteWriteQueueSnapshot,
};

struct GatewaySqliteReadObserver {
    queue: QueueObserver,
}
impl Default for GatewaySqliteReadObserver {
    fn default() -> Self {
        Self {
            queue: QueueObserver::new(
                QueueKind::DatabaseReadAdmission,
                None,
                "unsupported: maintenance read permits carry no payload size; oldest wait unavailable from reader observer",
            ),
        }
    }
}

impl SqliteReadObserver for GatewaySqliteReadObserver {
    fn observe_queue_snapshot(&self, snapshot: SqliteAdmissionSnapshot) {
        apply_current(&self.queue, snapshot);
    }
    fn observe(&self, event: SqliteReadEvent) {
        match event {
            SqliteReadEvent::AdmissionEnqueued { .. } => {
                self.queue.enqueued(QueueBytes::Unsupported);
                pioneer_observability::live::admission_wait(true);
            }
            SqliteReadEvent::AdmissionAcquired { .. } => {
                self.queue.dequeued(QueueBytes::Unsupported);
                pioneer_observability::live::admission_wait(false);
            }
            SqliteReadEvent::AdmissionCancelled { .. } => {
                self.queue.dequeued(QueueBytes::Unsupported);
                self.queue.outcome(QueueOutcome::Cancelled, 1);
                pioneer_observability::live::admission_wait(false);
            }
            SqliteReadEvent::AdmissionReleased { .. } => {
                self.queue.outcome(QueueOutcome::Processed, 1);
            }
            _ => {}
        }

        match event {
            SqliteReadEvent::OperationFinished {
                class,
                outcome,
                elapsed,
            } => record_database_read(DatabaseReadMetric {
                class: class.as_str(),
                outcome: outcome.as_str(),
                elapsed,
            }),
            SqliteReadEvent::AdmissionEnqueued {
                class,
                queue_depth,
                active,
            } => record_read_admission("enqueued", class.as_str(), queue_depth, active, None, None),
            SqliteReadEvent::AdmissionAcquired {
                class,
                waited,
                queue_depth,
                active,
            } => record_read_admission(
                "acquired",
                class.as_str(),
                queue_depth,
                active,
                Some(waited),
                None,
            ),
            SqliteReadEvent::AdmissionReleased {
                class,
                held,
                queue_depth,
                active,
            } => record_read_admission(
                "released",
                class.as_str(),
                queue_depth,
                active,
                None,
                Some(held),
            ),
            SqliteReadEvent::AdmissionCancelled {
                class,
                waited,
                queue_depth,
                active,
            } => record_read_admission(
                "cancelled",
                class.as_str(),
                queue_depth,
                active,
                Some(waited),
                None,
            ),
        }
    }
}

fn record_read_admission(
    event: &'static str,
    class: &'static str,
    queue_depth: usize,
    active: usize,
    waited: Option<std::time::Duration>,
    held: Option<std::time::Duration>,
) {
    record_database_read_admission(DatabaseReadAdmissionMetric {
        event,
        class,
        queue_depth: u64::try_from(queue_depth).unwrap_or(u64::MAX),
        active: u64::try_from(active).unwrap_or(u64::MAX),
        waited,
        held,
    });
}

struct GatewaySqliteWriteObserver {
    queue: QueueObserver,
}
impl Default for GatewaySqliteWriteObserver {
    fn default() -> Self {
        Self {
            queue: QueueObserver::new(
                QueueKind::DatabaseAdmission,
                None,
                "unsupported: writer permits carry no payload size; class scheduling is not FIFO",
            ),
        }
    }
}

impl SqliteWriteObserver for GatewaySqliteWriteObserver {
    fn observe_queue_snapshot(&self, snapshot: SqliteAdmissionSnapshot) {
        apply_current(&self.queue, snapshot);
    }
    fn observe(&self, event: SqliteWriteEvent) {
        let queue = match event {
            SqliteWriteEvent::Enqueued { queue, .. } => {
                self.queue.enqueued(QueueBytes::Unsupported);
                pioneer_observability::live::admission_wait(true);
                queue
            }
            SqliteWriteEvent::Acquired { queue, .. } => {
                self.queue.dequeued(QueueBytes::Unsupported);
                pioneer_observability::live::admission_wait(false);
                queue
            }
            SqliteWriteEvent::Cancelled { queue, .. } => {
                self.queue.dequeued(QueueBytes::Unsupported);
                self.queue.outcome(QueueOutcome::Cancelled, 1);
                pioneer_observability::live::admission_wait(false);
                queue
            }
            SqliteWriteEvent::Released { queue, .. } => {
                self.queue.outcome(QueueOutcome::Processed, 1);
                queue
            }
        };
        let _ = queue; // current depth comes from the owner snapshot callback below.

        let metric = match event {
            SqliteWriteEvent::Enqueued { class, queue } => {
                admission_metric("enqueued", class.as_str(), "none", queue, None, None)
            }
            SqliteWriteEvent::Acquired {
                class,
                reason,
                waited,
                queue,
            } => admission_metric(
                "acquired",
                class.as_str(),
                reason.as_str(),
                queue,
                Some(waited),
                None,
            ),
            SqliteWriteEvent::Released { class, held, queue } => {
                admission_metric("released", class.as_str(), "none", queue, None, Some(held))
            }
            SqliteWriteEvent::Cancelled {
                class,
                waited,
                queue,
            } => admission_metric(
                "cancelled",
                class.as_str(),
                "none",
                queue,
                Some(waited),
                None,
            ),
        };
        record_database_admission(metric);
    }
}

fn admission_metric(
    event: &'static str,
    class: &'static str,
    reason: &'static str,
    queue: SqliteWriteQueueSnapshot,
    waited: Option<std::time::Duration>,
    held: Option<std::time::Duration>,
) -> DatabaseAdmissionMetric {
    DatabaseAdmissionMetric {
        event,
        class,
        reason,
        critical_queue: u64::try_from(queue.critical).unwrap_or(u64::MAX),
        interactive_queue: u64::try_from(queue.interactive).unwrap_or(u64::MAX),
        maintenance_queue: u64::try_from(queue.maintenance).unwrap_or(u64::MAX),
        waited,
        held,
    }
}

pub(crate) fn write_observer() -> Arc<dyn SqliteWriteObserver> {
    Arc::new(GatewaySqliteWriteObserver::default())
}

pub(crate) fn read_observer() -> Arc<dyn SqliteReadObserver> {
    Arc::new(GatewaySqliteReadObserver::default())
}

fn apply_current(queue: &QueueObserver, snapshot: SqliteAdmissionSnapshot) {
    queue.native_admission_snapshot(
        snapshot.consent_generation,
        snapshot.revision,
        snapshot.sampled_monotonic_ms,
        snapshot.depth,
        snapshot.oldest_waited_ms,
    );
}

#[cfg(test)]
mod current_tests {
    use super::*;
    #[test]
    fn both_native_callbacks_keep_newest_state_even_after_late_milestones() {
        let previous = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(true);
        let epoch = pioneer_observability::telemetry_consent_snapshot().1;
        let writer = GatewaySqliteWriteObserver {
            queue: QueueObserver::new(
                QueueKind::DatabaseAdmission,
                None,
                "fixture versioned writer",
            ),
        };
        let reader = GatewaySqliteReadObserver {
            queue: QueueObserver::new(
                QueueKind::DatabaseReadAdmission,
                None,
                "fixture versioned reader",
            ),
        };
        let old = SqliteAdmissionSnapshot {
            revision: 1,
            sampled_monotonic_ms: 100,
            consent_generation: epoch,
            depth: 1,
            oldest_waited_ms: Some(90),
        };
        let new = SqliteAdmissionSnapshot {
            revision: 2,
            sampled_monotonic_ms: 110,
            depth: 0,
            oldest_waited_ms: None,
            ..old
        };
        writer.observe_queue_snapshot(new);
        writer.observe(SqliteWriteEvent::Enqueued {
            class: pioneer_sqlite::SqliteWriteClass::Maintenance,
            queue: SqliteWriteQueueSnapshot {
                maintenance: 1,
                ..Default::default()
            },
        });
        writer.observe_queue_snapshot(old);
        reader.observe_queue_snapshot(new);
        reader.observe(SqliteReadEvent::AdmissionEnqueued {
            class: pioneer_sqlite::SqliteReadClass::Maintenance,
            queue_depth: 1,
            active: 0,
        });
        reader.observe_queue_snapshot(SqliteAdmissionSnapshot {
            oldest_waited_ms: None,
            ..old
        });
        let s = pioneer_observability::live::snapshot().unwrap();
        for definition in ["fixture versioned writer", "fixture versioned reader"] {
            let q = s
                .queues
                .iter()
                .find(|q| q.byte_definition == definition)
                .unwrap();
            assert_eq!(
                (q.depth, q.oldest_age_ms, q.last_observed_ms),
                (0, None, 110)
            );
            assert_eq!(q.enqueued, 1); // historical milestone is still counted
        }
        pioneer_observability::set_telemetry_enabled(previous);
    }
}
