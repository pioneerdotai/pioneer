use crate::{AppendedTurnEvent, CliRuntimePendingRequestRecord, CrudStore};
use anyhow::Result;
use sea_orm::DatabaseTransaction;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

impl CrudStore {
    pub(crate) async fn project_semantic_timeline_live_turn_event(
        &self,
        transaction: &DatabaseTransaction,
        event: &AppendedTurnEvent,
        item: Option<serde_json::Value>,
    ) -> Result<()> {
        pioneer_crud_timeline::timeline_live_projection::project_semantic_timeline_live_turn_event(
            transaction,
            event,
            item,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn project_cli_runtime_pending_request(
        &self,
        transaction: &DatabaseTransaction,
        request: &CliRuntimePendingRequestRecord,
    ) -> Result<()> {
        pioneer_crud_timeline::timeline_live_projection::project_cli_runtime_pending_request(
            transaction,
            request,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn project_cli_runtime_turn_binding_state(
        &self,
        transaction: &DatabaseTransaction,
        turn_id: &str,
        refreshed_at: DateTimeWithTimeZone,
    ) -> Result<()> {
        pioneer_crud_timeline::timeline_live_projection::project_cli_runtime_turn_binding_state(
            transaction,
            turn_id,
            refreshed_at,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn project_semantic_timeline_snapshot_turn_item(
        &self,
        transaction: &DatabaseTransaction,
        turn_id: &str,
        item_id: &str,
        source_sequence: i64,
        refreshed_at: DateTimeWithTimeZone,
        item: serde_json::Value,
    ) -> Result<()> {
        pioneer_crud_timeline::timeline_live_projection::project_semantic_timeline_snapshot_turn_item(
            transaction,
            turn_id,
            item_id,
            source_sequence,
            refreshed_at,
            item,
        )
        .await?;
        Ok(())
    }
}
