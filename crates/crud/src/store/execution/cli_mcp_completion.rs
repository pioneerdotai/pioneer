//! Finalize a managed CLI MCP item from its confirmed invocation outcome.
use crate::CrudStore;
use crate::events::TurnEventPayload;
use crate::store::events::event_preparation::{
    TURN_EVENT_PROJECTION_LEASE_SECS, prepare_projected_turn_event_for_permanent_storage,
};
use pioneer_crud_execution::time::unix_to_datetime;
use anyhow::Result;
use sea_orm::TransactionTrait;

pub use pioneer_crud_execution::cli_mcp_completion::CliMcpCallIdentity;
use pioneer_crud_execution::cli_mcp_completion::{
    completion_already_confirmed, find_confirmed_completion,
};
pub use pioneer_crud_execution::cli_mcp_item::is_managed_cli_mcp_item;

impl CrudStore {
    /// The checked native claim is supplied by the managed adapter. Recheck its
    /// durable execution/generation and the current item attempt in the same
    /// writer transaction as canonical acceptance. Projection uses the existing
    /// replay lane if an accepted predecessor has not yet been projected.
    pub async fn materialize_cli_mcp_call_completed(
        &self,
        notification: pioneer_protocol::ItemCompletedNotification,
        identity: CliMcpCallIdentity,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        let cleanup_turns = vec![notification.turn_id.clone()];
        let cleanup_change = crate::terminal_cleanup::Change::events(&self.connection);
        let changes = [
            "gateway.control",
            "gateway.cli",
            "projection",
            "compaction",
            "tasks.lifecycle",
        ]
        .map(|domain| {
            self.control_work_change(
                domain,
                &[crate::GatewayControlWake::Turn(
                    notification.turn_id.clone(),
                )],
            )
        });
        let result = async {
            anyhow::ensure!(
                notification.item.item_id() == identity.native_item_id,
                "CLI MCP completion item identity mismatch"
            );
            let created_at = unix_to_datetime(event_timestamp_secs);
            let claim_expires_at = unix_to_datetime(
                event_timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS),
            );
            // No response serialization or policy preparation holds the writer.
            let prepared = prepare_projected_turn_event_for_permanent_storage(
                &self.connection,
                TurnEventPayload::ItemCompleted(notification.clone()),
                created_at,
            )
            .await?;
            let confirmed_replay =
                find_confirmed_completion(&self.connection, &notification, &identity).await?;
            self.run_serialized_write(|| async {
                let prepared = prepared.clone();
                let transaction = self.connection.begin().await?;
                if completion_already_confirmed(
                    &transaction,
                    &notification,
                    &identity,
                    &prepared.event,
                    confirmed_replay.as_ref(),
                )
                .await?
                {
                    transaction.rollback().await?;
                    return Ok(false);
                }
                let changed = self
                    .append_and_project_turn_event_with_replay_in_transaction(
                        &transaction,
                        prepared,
                        created_at,
                        claim_expires_at,
                        true,
                        false,
                        true,
                    )
                    .await?;
                transaction.commit().await?;
                Ok(changed)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            cleanup_change
                .complete(&self.connection, *changed, &cleanup_turns)
                .await;
            for change in changes {
                change.completed(*changed);
            }
        }
        result.map(|_| ())
    }
}
