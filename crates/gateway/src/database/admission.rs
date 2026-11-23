use pioneer_observability::queues::{
    AdmissionTransition, QueueBytes, QueueKind, QueueObserver, QueueOutcome,
};
use std::sync::Arc;

use pioneer_observability::{
    DatabaseAdmissionMetric, DatabaseReadAdmissionMetric, DatabaseReadMetric,
    record_database_admission_for_consent, record_database_read_admission_for_consent,
    record_database_read_for_consent,
};
use pioneer_postgres_pools::{
    PostgresAdmissionSnapshot, PostgresReadEvent, PostgresReadObserver, PostgresWriteEvent,
    PostgresWriteObserver, PostgresWriteQueueSnapshot,
};

struct GatewayPostgresReadObserver {
    queue: QueueObserver,
}
impl Default for GatewayPostgresReadObserver {
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

impl PostgresReadObserver for GatewayPostgresReadObserver {
    fn observe_admission(&self, event: PostgresReadEvent, snapshot: PostgresAdmissionSnapshot) {
        if pioneer_observability::telemetry_consent_snapshot()
            != (true, snapshot.consent_generation)
        {
            return;
        }
        let transition = match event {
            PostgresReadEvent::AdmissionEnqueued { .. } => AdmissionTransition::Enqueued,
            PostgresReadEvent::AdmissionAcquired { .. } => AdmissionTransition::Acquired,
            PostgresReadEvent::AdmissionReleased { .. } => AdmissionTransition::Released,
            PostgresReadEvent::AdmissionCancelled { .. } => AdmissionTransition::Cancelled,
            PostgresReadEvent::OperationFinished { .. } => {
                self.record_metric(event, Some(snapshot.consent_generation));
                return;
            }
        };
        self.queue.native_admission_transition(
            transition,
            snapshot.consent_generation,
            snapshot.revision,
            snapshot.sampled_monotonic_ms,
            snapshot.depth,
            snapshot.oldest_waited_ms,
        );
        self.record_metric(event, Some(snapshot.consent_generation));
    }
    fn observe_queue_snapshot(&self, snapshot: PostgresAdmissionSnapshot) {
        apply_current(&self.queue, snapshot);
    }
    fn observe(&self, event: PostgresReadEvent) {
        let (enabled, epoch) = pioneer_observability::telemetry_consent_snapshot();
        let generation = enabled.then_some(epoch);
        match event {
            PostgresReadEvent::AdmissionEnqueued { .. } => {
                self.queue.enqueued(QueueBytes::Unsupported);
                pioneer_observability::live::admission_wait(true);
            }
            PostgresReadEvent::AdmissionAcquired { .. } => {
                self.queue.dequeued(QueueBytes::Unsupported);
                pioneer_observability::live::admission_wait(false);
            }
            PostgresReadEvent::AdmissionCancelled { .. } => {
                self.queue.dequeued(QueueBytes::Unsupported);
                self.queue.outcome(QueueOutcome::Cancelled, 1);
                pioneer_observability::live::admission_wait(false);
            }
            PostgresReadEvent::AdmissionReleased { .. } => {
                self.queue.outcome(QueueOutcome::Processed, 1);
            }
            _ => {}
        }

        self.record_metric(event, generation);
    }
}

impl GatewayPostgresReadObserver {
    fn record_metric(&self, event: PostgresReadEvent, generation: Option<u64>) {
        match event {
            PostgresReadEvent::OperationFinished {
                class,
                outcome,
                elapsed,
            } => record_database_read_for_consent(
                DatabaseReadMetric {
                    class: class.as_str(),
                    outcome: outcome.as_str(),
                    elapsed,
                },
                generation,
            ),
            PostgresReadEvent::AdmissionEnqueued {
                class,
                queue_depth,
                active,
            } => record_read_admission(
                generation,
                "enqueued",
                class.as_str(),
                queue_depth,
                active,
                None,
                None,
            ),
            PostgresReadEvent::AdmissionAcquired {
                class,
                waited,
                queue_depth,
                active,
            } => record_read_admission(
                generation,
                "acquired",
                class.as_str(),
                queue_depth,
                active,
                Some(waited),
                None,
            ),
            PostgresReadEvent::AdmissionReleased {
                class,
                held,
                queue_depth,
                active,
            } => record_read_admission(
                generation,
                "released",
                class.as_str(),
                queue_depth,
                active,
                None,
                Some(held),
            ),
            PostgresReadEvent::AdmissionCancelled {
                class,
                waited,
                queue_depth,
                active,
            } => record_read_admission(
                generation,
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
    generation: Option<u64>,
    event: &'static str,
    class: &'static str,
    queue_depth: usize,
    active: usize,
    waited: Option<std::time::Duration>,
    held: Option<std::time::Duration>,
) {
    record_database_read_admission_for_consent(
        DatabaseReadAdmissionMetric {
            event,
            class,
            queue_depth: u64::try_from(queue_depth).unwrap_or(u64::MAX),
            active: u64::try_from(active).unwrap_or(u64::MAX),
            waited,
            held,
        },
        generation,
    );
}

struct GatewayPostgresWriteObserver {
    queue: QueueObserver,
}
impl Default for GatewayPostgresWriteObserver {
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

impl PostgresWriteObserver for GatewayPostgresWriteObserver {
    fn observe_admission(&self, event: PostgresWriteEvent, snapshot: PostgresAdmissionSnapshot) {
        if pioneer_observability::telemetry_consent_snapshot()
            != (true, snapshot.consent_generation)
        {
            return;
        }
        let transition = match event {
            PostgresWriteEvent::Enqueued { .. } => AdmissionTransition::Enqueued,
            PostgresWriteEvent::Acquired { .. } => AdmissionTransition::Acquired,
            PostgresWriteEvent::Released { .. } => AdmissionTransition::Released,
            PostgresWriteEvent::Cancelled { .. } => AdmissionTransition::Cancelled,
        };
        self.queue.native_admission_transition(
            transition,
            snapshot.consent_generation,
            snapshot.revision,
            snapshot.sampled_monotonic_ms,
            snapshot.depth,
            snapshot.oldest_waited_ms,
        );
        self.record_metric(event, Some(snapshot.consent_generation));
    }
    fn observe_queue_snapshot(&self, snapshot: PostgresAdmissionSnapshot) {
        apply_current(&self.queue, snapshot);
    }
    fn observe(&self, event: PostgresWriteEvent) {
        let (enabled, epoch) = pioneer_observability::telemetry_consent_snapshot();
        let generation = enabled.then_some(epoch);
        let queue = match event {
            PostgresWriteEvent::Enqueued { queue, .. } => {
                self.queue.enqueued(QueueBytes::Unsupported);
                pioneer_observability::live::admission_wait(true);
                queue
            }
            PostgresWriteEvent::Acquired { queue, .. } => {
                self.queue.dequeued(QueueBytes::Unsupported);
                pioneer_observability::live::admission_wait(false);
                queue
            }
            PostgresWriteEvent::Cancelled { queue, .. } => {
                self.queue.dequeued(QueueBytes::Unsupported);
                self.queue.outcome(QueueOutcome::Cancelled, 1);
                pioneer_observability::live::admission_wait(false);
                queue
            }
            PostgresWriteEvent::Released { queue, .. } => {
                self.queue.outcome(QueueOutcome::Processed, 1);
                queue
            }
        };
        let _ = queue; // current depth comes from the owner snapshot callback below.

        self.record_metric(event, generation);
    }
}

impl GatewayPostgresWriteObserver {
    fn record_metric(&self, event: PostgresWriteEvent, generation: Option<u64>) {
        let metric = match event {
            PostgresWriteEvent::Enqueued { class, queue } => {
                admission_metric("enqueued", class.as_str(), "none", queue, None, None)
            }
            PostgresWriteEvent::Acquired {
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
            PostgresWriteEvent::Released { class, held, queue } => {
                admission_metric("released", class.as_str(), "none", queue, None, Some(held))
            }
            PostgresWriteEvent::Cancelled {
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
        record_database_admission_for_consent(metric, generation);
    }
}

fn admission_metric(
    event: &'static str,
    class: &'static str,
    reason: &'static str,
    queue: PostgresWriteQueueSnapshot,
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

pub(crate) fn write_observer() -> Arc<dyn PostgresWriteObserver> {
    Arc::new(GatewayPostgresWriteObserver::default())
}

pub(crate) fn read_observer() -> Arc<dyn PostgresReadObserver> {
    Arc::new(GatewayPostgresReadObserver::default())
}

fn apply_current(queue: &QueueObserver, snapshot: PostgresAdmissionSnapshot) {
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
        let writer = GatewayPostgresWriteObserver {
            queue: QueueObserver::new(
                QueueKind::DatabaseAdmission,
                None,
                "fixture versioned writer",
            ),
        };
        let reader = GatewayPostgresReadObserver {
            queue: QueueObserver::new(
                QueueKind::DatabaseReadAdmission,
                None,
                "fixture versioned reader",
            ),
        };
        let old = PostgresAdmissionSnapshot {
            revision: 1,
            sampled_monotonic_ms: 100,
            consent_generation: epoch,
            depth: 1,
            oldest_waited_ms: Some(90),
        };
        let new = PostgresAdmissionSnapshot {
            revision: 2,
            sampled_monotonic_ms: 110,
            depth: 0,
            oldest_waited_ms: None,
            ..old
        };
        writer.observe_queue_snapshot(new);
        writer.observe_admission(
            PostgresWriteEvent::Enqueued {
                class: pioneer_postgres_pools::PostgresWriteClass::Maintenance,
                queue: PostgresWriteQueueSnapshot {
                    maintenance: 1,
                    ..Default::default()
                },
            },
            old,
        );
        reader.observe_queue_snapshot(new);
        reader.observe_admission(
            PostgresReadEvent::AdmissionEnqueued {
                class: pioneer_postgres_pools::PostgresReadClass::Maintenance,
                queue_depth: 1,
                active: 0,
            },
            PostgresAdmissionSnapshot {
                oldest_waited_ms: None,
                ..old
            },
        );
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
