use crate::delivery_recovery::{
    self as recovery, DeliveryRecoveryCursor, DeliveryRecoverySnapshot,
};
use crate::event_store::TaskEventStore;
use crate::task_projector::TaskOutputBinder;
use anyhow::Result;
use sea_orm::TransactionTrait;

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    pub async fn task_delivery_recovery_page(
        &self,
        cutoff: i64,
        after: Option<&DeliveryRecoveryCursor>,
        limit: u64,
    ) -> Result<Vec<DeliveryRecoveryCursor>> {
        recovery::source_page(&self.connection, cutoff, after, limit).await
    }

    pub async fn due_task_delivery_recovery_retries(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<pioneer_entity::task_delivery_recovery_retry::Model>> {
        recovery::due_page(&self.connection, now, limit).await
    }

    pub async fn task_delivery_recovery_snapshot(
        &self,
        id: &str,
    ) -> Result<Option<DeliveryRecoverySnapshot>> {
        let read = self.connection.begin_read().await?;
        let snapshot = recovery::snapshot(&read, id).await?;
        read.commit().await?;
        Ok(snapshot)
    }

    /// No domain mutation; clock is read only after writer admission. Failure
    /// to commit this bookkeeping must be reported as a durability limitation.
    pub async fn defer_task_delivery_recovery(
        &self,
        expected: &DeliveryRecoverySnapshot,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<bool> {
        let token = pioneer_protocol::generate_id(21);
        #[cfg(any(test, feature = "test-support"))]
        self.delivery_commit_test_hooks
            .pause_delivery_commit_for_test(
                crate::delivery_transitions::TaskDeliveryCommitTestKind::RecoveryRetry,
            )
            .await;
        let tx = self.connection.begin().await?;
        let result = recovery::defer(&tx, expected, clock(), &token).await;
        match result {
            Ok(changed) => {
                tx.commit().await?;
                Ok(changed)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
}
