use crate::store::events::event_preparation::{
    TURN_EVENT_PROJECTION_LEASE_SECS, prepare_projected_turn_event_for_permanent_storage,
};
use crate::{
    CrudStore, ExecutionWindowStartProjectionPending, NativeExecutionWindowTransition,
    NewTurnExecutionCheckpointRecord, NewTurnExecutionWindowRecord, TurnExecutionCheckpointRecord,
    TurnExecutionDataCleanupRecord, TurnExecutionWindowRecord, TurnExecutionWindowStatsRecord,
    TurnExecutionWindowTerminalItemCountsRecord, TurnExecutionWindowUsageAggregateRecord,
    terminal_cleanup,
};
use anyhow::{Context, Result};
use pioneer_crud_execution::gateway_control::event_wakes;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::{event_append as turn_event, turn_execution_window};
use sea_orm::TransactionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

impl CrudStore {
    pub async fn materialize_native_execution_window_transition(
        &self,
        transition: NativeExecutionWindowTransition,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        let control_wakes = event_wakes(std::slice::from_ref(&transition.event_payload()));
        // These events advance replay/delivery receipts and compressed raw
        // history; started/checkpointed/continued also update recovery liveness.
        let cleanup_turns = vec![transition.event_payload().turn_id().to_owned()];
        let cleanup_change = terminal_cleanup::Change::events(&self.connection);
        let changes = ["gateway.control", "projection"]
            .map(|domain| self.control_work_change(domain, &control_wakes));
        let result = async {
            let _release_wake =
                matches!(&transition, NativeExecutionWindowTransition::Blocked { .. })
                    .then(|| self.execution_heartbeats.release_wake());
            let created_at = unix_to_datetime(event_timestamp_secs);
            let claim_expires_at = unix_to_datetime(
                event_timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS),
            );
            let prepared_event = prepare_projected_turn_event_for_permanent_storage(
                &self.connection,
                transition.event_payload(),
                created_at,
            )
            .await?;
            let window =
                pioneer_crud_execution::native_execution_window::PreparedWindowTransition::new(
                    transition,
                )?;
            self.retry_write_on_conflict(|| {
                let window = window.clone();
                let prepared_event = prepared_event.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin native execution-window transition")?;
                    let result: Result<bool> = async {
                        // Exact delivery replay is successful regardless of how far a
                        // later valid transition has advanced the window. The event
                        // and authoritative mutation were committed by this same
                        // transaction, so existence of the stable event identity is
                        // the durable acknowledgement fence.
                        pioneer_crud_conversations::turn_event::lock_stream(
                            &transaction,
                            prepared_event.event.payload().turn_id(),
                        )
                        .await?;
                        if turn_event::prepared_event_already_exists(
                            &transaction,
                            &prepared_event.event,
                        )
                        .await?
                        {
                            return Ok(false);
                        }

                        window
                            .apply(&transaction, |transaction| {
                                self.append_and_project_turn_event_in_transaction(
                                    transaction,
                                    prepared_event,
                                    created_at,
                                    claim_expires_at,
                                    true,
                                )
                            })
                            .await?;
                        Ok(true)
                    }
                    .await;
                    match result {
                        Ok(changed) => {
                            transaction
                                .commit()
                                .await
                                .context("failed to commit native execution-window transition")?;
                            Ok(changed)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            Err(error)
                        }
                    }
                }
            })
            .await
        }
        .await;
        let changed = match &result {
            Ok(changed) => Some(*changed),
            Err(error) if error.is::<ExecutionWindowStartProjectionPending>() => Some(false),
            Err(_) => None,
        };
        if let Some(changed) = changed {
            cleanup_change
                .complete(&self.connection, changed, &cleanup_turns)
                .await;
            for change in changes {
                change.completed(changed);
            }
        }
        result.map(|_| ())
    }

    pub async fn create_turn_execution_window(
        &self,
        record: NewTurnExecutionWindowRecord,
        created_at: DateTimeWithTimeZone,
        updated_at: DateTimeWithTimeZone,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::create_turn_execution_window(
            &self.connection,
            record,
            created_at,
            updated_at,
        )
        .await
    }

    pub async fn get_turn_execution_window(
        &self,
        window_id: &str,
    ) -> Result<Option<TurnExecutionWindowRecord>> {
        turn_execution_window::get_turn_execution_window(&self.connection, window_id).await
    }

    pub async fn list_turn_execution_windows(
        &self,
        turn_id: &str,
    ) -> Result<Vec<TurnExecutionWindowRecord>> {
        turn_execution_window::list_turn_execution_windows(&self.connection, turn_id).await
    }

    pub async fn latest_turn_execution_window(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnExecutionWindowRecord>> {
        turn_execution_window::latest_turn_execution_window(&self.connection, turn_id).await
    }

    pub async fn aggregate_turn_execution_window_usage(
        &self,
        turn_id: &str,
    ) -> Result<TurnExecutionWindowUsageAggregateRecord> {
        turn_execution_window::aggregate_turn_execution_window_usage(&self.connection, turn_id)
            .await
    }

    pub async fn mark_turn_execution_window_exhausted(
        &self,
        window_id: &str,
        reason: pioneer_protocol::ExecutionWindowExhaustionReason,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_exhausted(
            &self.connection,
            window_id,
            reason,
            stats,
        )
        .await
    }

    pub async fn mark_turn_execution_window_checkpointed(
        &self,
        window_id: &str,
        updated_at: DateTimeWithTimeZone,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_checkpointed(
            &self.connection,
            window_id,
            updated_at,
        )
        .await
    }

    pub async fn mark_turn_execution_window_continued(
        &self,
        window_id: &str,
        updated_at: DateTimeWithTimeZone,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_continued(
            &self.connection,
            window_id,
            updated_at,
        )
        .await
    }

    pub async fn mark_turn_execution_window_completed(
        &self,
        window_id: &str,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_completed(
            &self.connection,
            window_id,
            stats,
        )
        .await
    }

    pub async fn mark_turn_execution_window_failed(
        &self,
        window_id: &str,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_failed(&self.connection, window_id, stats)
            .await
    }

    pub async fn mark_turn_execution_window_interrupted(
        &self,
        window_id: &str,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_interrupted(
            &self.connection,
            window_id,
            stats,
        )
        .await
    }

    pub async fn mark_turn_execution_window_blocked(
        &self,
        window_id: &str,
        reason: Option<pioneer_protocol::ExecutionWindowExhaustionReason>,
        stats: TurnExecutionWindowStatsRecord,
    ) -> Result<TurnExecutionWindowRecord> {
        turn_execution_window::mark_turn_execution_window_blocked(
            &self.connection,
            window_id,
            reason,
            stats,
        )
        .await
    }

    pub async fn close_active_execution_windows_for_terminal_turns(
        &self,
        now: DateTimeWithTimeZone,
    ) -> Result<u64> {
        self.retry_write_on_conflict(|| async {
            turn_execution_window::close_active_execution_windows_for_terminal_turns(
                &self.connection,
                now,
            )
            .await
        })
        .await
    }

    pub async fn count_turn_execution_window_terminal_items(
        &self,
        turn_id: &str,
    ) -> Result<TurnExecutionWindowTerminalItemCountsRecord> {
        turn_execution_window::count_turn_execution_window_terminal_items(&self.connection, turn_id)
            .await
    }

    pub async fn count_turn_execution_window_terminal_items_since(
        &self,
        turn_id: &str,
        started_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<TurnExecutionWindowTerminalItemCountsRecord> {
        turn_execution_window::count_turn_execution_window_terminal_items_since(
            &self.connection,
            turn_id,
            started_at,
        )
        .await
    }

    pub async fn list_turn_execution_window_items(
        &self,
        turn_id: &str,
        started_at: DateTimeWithTimeZone,
    ) -> Result<Vec<pioneer_protocol::TurnItem>> {
        turn_execution_window::list_turn_execution_window_items(
            &self.connection,
            turn_id,
            started_at,
        )
        .await
    }

    pub async fn save_turn_execution_checkpoint(
        &self,
        record: NewTurnExecutionCheckpointRecord,
    ) -> Result<TurnExecutionCheckpointRecord> {
        turn_execution_window::save_turn_execution_checkpoint(&self.connection, record).await
    }

    pub async fn get_turn_execution_checkpoint(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<TurnExecutionCheckpointRecord>> {
        turn_execution_window::get_turn_execution_checkpoint(&self.connection, checkpoint_id).await
    }

    pub async fn list_turn_execution_checkpoints_for_window(
        &self,
        window_id: &str,
    ) -> Result<Vec<TurnExecutionCheckpointRecord>> {
        turn_execution_window::list_turn_execution_checkpoints_for_window(
            &self.connection,
            window_id,
        )
        .await
    }

    pub async fn latest_turn_execution_checkpoint_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnExecutionCheckpointRecord>> {
        turn_execution_window::latest_turn_execution_checkpoint_for_turn(&self.connection, turn_id)
            .await
    }

    pub async fn delete_turn_execution_checkpoints_for_turn(&self, turn_id: &str) -> Result<u64> {
        turn_execution_window::delete_turn_execution_checkpoints_for_turn(&self.connection, turn_id)
            .await
    }

    pub async fn delete_turn_execution_checkpoints_for_window(
        &self,
        window_id: &str,
    ) -> Result<u64> {
        turn_execution_window::delete_turn_execution_checkpoints_for_window(
            &self.connection,
            window_id,
        )
        .await
    }

    pub async fn delete_turn_execution_windows_for_turn(&self, turn_id: &str) -> Result<u64> {
        turn_execution_window::delete_turn_execution_windows_for_turn(&self.connection, turn_id)
            .await
    }

    pub async fn delete_turn_execution_data_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<TurnExecutionDataCleanupRecord> {
        turn_execution_window::delete_turn_execution_data_for_turn(&self.connection, turn_id).await
    }
}
