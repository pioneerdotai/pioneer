use crate::{
    ClaimedTurnEventDeliveryRecord, CrudStore, TurnEventDeliveryClaimBatch,
    TurnEventDeliverySettlement,
};
use anyhow::Result;
use pioneer_crud_conversations::turn_event_delivery;

impl CrudStore {
    /// Advisory timers preserve database errors.
    pub async fn next_turn_event_delivery_deadline(&self, consumer: &str) -> Result<Option<i64>> {
        pioneer_crud_conversations::delivery_service::next_turn_event_delivery_deadline(
            &self.connection,
            consumer,
        )
        .await
    }

    pub async fn next_turn_event_projection_deadline(&self) -> Result<Option<i64>> {
        Ok(
            pioneer_crud_conversations::event_projection::next_deadline(&self.connection)
                .await?
                .map(pioneer_crud_execution::time::deadline_unix_seconds),
        )
    }

    pub async fn claim_due_turn_event_deliveries(
        &self,
        consumer: &str,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<ClaimedTurnEventDeliveryRecord>> {
        Ok(self
            .claim_due_turn_event_deliveries_with_deadline(consumer, now_unix, limit)
            .await?
            .deliveries)
    }

    pub async fn claim_due_turn_event_deliveries_with_deadline(
        &self,
        consumer: &str,
        now_unix: i64,
        limit: u64,
    ) -> Result<TurnEventDeliveryClaimBatch> {
        pioneer_crud_conversations::delivery_service::claim_due_turn_event_deliveries_with_deadline(
            &self.connection,
            consumer,
            now_unix,
            limit,
        )
        .await
    }

    /// Ready outcomes only: no batching delay, no writer held during delivery.
    pub async fn settle_turn_event_deliveries(
        &self,
        settlements: &[TurnEventDeliverySettlement],
    ) -> Result<u64> {
        pioneer_crud_conversations::delivery_service::settle_turn_event_deliveries(
            &self.connection,
            settlements,
        )
        .await
    }

    pub async fn complete_turn_event_delivery(
        &self,
        id: &str,
        claim_token: &str,
        now_unix: i64,
    ) -> Result<bool> {
        pioneer_crud_conversations::delivery_service::complete_turn_event_delivery(
            &self.connection,
            id,
            claim_token,
            now_unix,
        )
        .await
    }

    /// Existing canonical/LIVE receipts retain an independent item's delivery
    /// owner after its startup publication and process-local obligation retire.
    pub async fn independent_item_uses_live_delivery(
        &self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> Result<bool> {
        turn_event_delivery::independent_item_uses_live(
            &self.connection,
            thread_id,
            turn_id,
            item_id,
        )
        .await
    }

    /// Release a LIVE claim pending its foreground start without consuming a
    /// normal delivery retry. The exact claim token fences replacement workers.
    pub async fn defer_turn_event_delivery_for_foreground_start(
        &self,
        id: &str,
        claim_token: &str,
        now_unix: i64,
    ) -> Result<bool> {
        pioneer_crud_conversations::delivery_service::defer_turn_event_delivery_for_foreground_start(&self.connection, id, claim_token, now_unix).await
    }

    pub async fn fail_turn_event_delivery(
        &self,
        id: &str,
        claim_token: &str,
        attempt_count: i64,
        error: String,
        now_unix: i64,
    ) -> Result<bool> {
        pioneer_crud_conversations::delivery_service::fail_turn_event_delivery(
            &self.connection,
            id,
            claim_token,
            attempt_count,
            error,
            now_unix,
        )
        .await
    }
}
