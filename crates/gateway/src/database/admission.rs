use std::sync::Arc;

use pioneer_observability::{DatabaseAdmissionMetric, record_database_admission};
use pioneer_sqlite::{
    SqliteAdmissionEvent, SqliteAdmissionObserver, SqliteAdmissionQueueSnapshot,
    SqliteWriteCoordinator,
};

#[derive(Default)]
struct GatewaySqliteAdmissionObserver;

impl SqliteAdmissionObserver for GatewaySqliteAdmissionObserver {
    fn observe(&self, event: SqliteAdmissionEvent) {
        let metric = match event {
            SqliteAdmissionEvent::Enqueued { class, queue } => {
                admission_metric("enqueued", class.as_str(), "none", queue, None, None)
            }
            SqliteAdmissionEvent::Acquired {
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
            SqliteAdmissionEvent::Released { class, held, queue } => {
                admission_metric("released", class.as_str(), "none", queue, None, Some(held))
            }
            SqliteAdmissionEvent::Cancelled {
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
    queue: SqliteAdmissionQueueSnapshot,
    waited: Option<std::time::Duration>,
    held: Option<std::time::Duration>,
) -> DatabaseAdmissionMetric {
    DatabaseAdmissionMetric {
        event,
        class,
        reason,
        foreground_queue: queue.foreground as u64,
        background_queue: queue.background as u64,
        waited,
        held,
    }
}

pub(crate) fn new_write_coordinator() -> SqliteWriteCoordinator {
    SqliteWriteCoordinator::with_observer(Arc::new(GatewaySqliteAdmissionObserver))
}
