use std::sync::Arc;

use pioneer_observability::{
    DatabaseAdmissionMetric, DatabaseReadAdmissionMetric, DatabaseReadMetric,
    record_database_admission, record_database_read, record_database_read_admission,
};
use pioneer_sqlite::{
    SqliteReadEvent, SqliteReadObserver, SqliteWriteEvent, SqliteWriteObserver,
    SqliteWriteQueueSnapshot,
};

#[derive(Default)]
struct GatewaySqliteReadObserver;

impl SqliteReadObserver for GatewaySqliteReadObserver {
    fn observe(&self, event: SqliteReadEvent) {
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

#[derive(Default)]
struct GatewaySqliteWriteObserver;

impl SqliteWriteObserver for GatewaySqliteWriteObserver {
    fn observe(&self, event: SqliteWriteEvent) {
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
    Arc::new(GatewaySqliteWriteObserver)
}

pub(crate) fn read_observer() -> Arc<dyn SqliteReadObserver> {
    Arc::new(GatewaySqliteReadObserver)
}
