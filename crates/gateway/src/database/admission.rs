use std::sync::Arc;

use pioneer_observability::{DatabaseAdmissionMetric, record_database_admission};
use pioneer_sqlite::{SqliteWriteEvent, SqliteWriteObserver, SqliteWriteQueueSnapshot};

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
