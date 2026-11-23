use crate::{CrudStore, DeliveryRecoveryCursor, DeliveryRecoverySnapshot};
use anyhow::Result;

impl CrudStore {
    pub async fn task_delivery_recovery_page(
        &self,
        cutoff: i64,
        after: Option<&DeliveryRecoveryCursor>,
        limit: u64,
    ) -> Result<Vec<DeliveryRecoveryCursor>> {
        self.task_event_store()
            .task_delivery_recovery_page(cutoff, after, limit)
            .await
    }

    pub async fn due_task_delivery_recovery_retries(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<pioneer_entity::task_delivery_recovery_retry::Model>> {
        self.task_event_store()
            .due_task_delivery_recovery_retries(now, limit)
            .await
    }

    pub async fn task_delivery_recovery_snapshot(
        &self,
        id: &str,
    ) -> Result<Option<DeliveryRecoverySnapshot>> {
        self.task_event_store()
            .task_delivery_recovery_snapshot(id)
            .await
    }

    /// No domain mutation; clock is read only after writer admission. Failure
    /// to commit this bookkeeping must be reported as a durability limitation.
    pub async fn defer_task_delivery_recovery(
        &self,
        expected: &DeliveryRecoverySnapshot,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<bool> {
        self.task_event_store()
            .defer_task_delivery_recovery(expected, clock)
            .await
    }
}
