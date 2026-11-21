use crate::events::TurnEventPayload;
use crate::store::events::event_preparation::{
    TURN_EVENT_PROJECTION_LEASE_SECS, prepare_projected_turn_event_for_permanent_storage,
};
use pioneer_crud_execution::time::unix_to_datetime;
use crate::{
    CrudStore, DeleteTurnMessageRequest, DeleteTurnMessageResult, EditTurnMessageRequest,
    GatewayControlWake, compaction, list_turn_message_revisions, terminal_cleanup,
    turn_message_revision_from_model,
};
use anyhow::{Context, Result};
use pioneer_crud_conversations::message_commands::{
    MessageDeletePreparation, prepare_message_delete, prepare_message_edit,
    revalidate_message_delete, revalidate_message_edit,
};
use pioneer_protocol::{Turn, TurnMessageEditedEvent, TurnMessageRevision};
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn edit_turn_message(
        &self,
        request: EditTurnMessageRequest,
    ) -> Result<TurnMessageEditedEvent> {
        self.run_serialized_write(|| {
            let request = request.clone();
            async move { self.edit_turn_message_once(request).await }
        })
        .await
    }

    async fn edit_turn_message_once(
        &self,
        request: EditTurnMessageRequest,
    ) -> Result<TurnMessageEditedEvent> {
        let payload_change = terminal_cleanup::Change::payload(&self.connection);
        let receipt_keys = vec![request.turn_id.clone()];
        let receipt_change = self
            .connection
            .work_signal("maintenance.receipts")
            .changing();
        let maintenance_changes = [
            "projection",
            "compaction",
            "maintenance.html",
            "maintenance.native",
        ]
        .map(|domain| {
            self.control_work_change(domain, &[GatewayControlWake::Turn(request.turn_id.clone())])
        });
        let maintenance_result = async {
            let changed_at = unix_to_datetime(request.changed_at_unix);
            let prepared = prepare_message_edit(&self.connection, &request).await?;
            let event = prepared.event.clone();
            let prepared_event = prepare_projected_turn_event_for_permanent_storage(
                &self.connection,
                TurnEventPayload::TurnMessageEdited(event.clone()),
                unix_to_datetime(request.changed_at_unix),
            )
            .await?;

            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin Turn message edit transaction")?;
            let outcome = async {
                revalidate_message_edit(&transaction, &request, &prepared).await?;

                // This canonical, append-only mutation event is also the durable moderation record:
                // its server-owned actor differs from the immutable original author for a
                // Superuser action. Reusing it keeps Epic 6 DDL-only and avoids a second audit path.
                self.append_and_project_turn_event_in_transaction(
                    &transaction,
                    prepared_event,
                    changed_at,
                    unix_to_datetime(
                        request
                            .changed_at_unix
                            .saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS),
                    ),
                    false,
                )
                .await?;
                Ok(event)
            }
            .await;

            match outcome {
                Ok(event) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit Turn message edit transaction")?;
                    Ok(event)
                }
                Err(error) => Err(crate::store::connection::rollback_preserving_error(transaction, error).await),
            }
        }
        .await;
        let maintenance_changed = maintenance_result.is_ok();
        if maintenance_result.is_ok() {
            payload_change
                .complete(&self.connection, maintenance_changed, &receipt_keys)
                .await;
            receipt_change.completed_keys(if maintenance_changed {
                &receipt_keys
            } else {
                &[]
            });
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn delete_turn_message(
        &self,
        request: DeleteTurnMessageRequest,
    ) -> Result<DeleteTurnMessageResult> {
        self.run_serialized_write(|| {
            let request = request.clone();
            async move { self.delete_turn_message_once(request).await }
        })
        .await
    }

    async fn delete_turn_message_once(
        &self,
        request: DeleteTurnMessageRequest,
    ) -> Result<DeleteTurnMessageResult> {
        let payload_change = terminal_cleanup::Change::payload(&self.connection);
        let receipt_keys = vec![request.turn_id.clone()];
        let receipt_change = self
            .connection
            .work_signal("maintenance.receipts")
            .changing();
        let maintenance_changes = [
            "projection",
            "compaction",
            "maintenance.html",
            "maintenance.portable",
            "maintenance.native",
        ]
        .map(|domain| {
            self.control_work_change(domain, &[GatewayControlWake::Turn(request.turn_id.clone())])
        });
        let maintenance_result = async {
            let changed_at = unix_to_datetime(request.changed_at_unix);
            let prepared = match prepare_message_delete(&self.connection, &request).await? {
                MessageDeletePreparation::Ready(prepared) => prepared,
                MessageDeletePreparation::AlreadyDeleted(result) => return Ok(result),
            };
            let event = prepared.event.clone();
            let prepared_event = prepare_projected_turn_event_for_permanent_storage(
                &self.connection,
                TurnEventPayload::TurnMessageDeleted(event.clone()),
                unix_to_datetime(request.changed_at_unix),
            )
            .await?;

            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin Turn message delete transaction")?;
            let outcome = async {
                if let Some(result) =
                    revalidate_message_delete(&transaction, &request, &prepared).await?
                {
                    return Ok(result);
                }

                // The canonical delete event preserves the authenticated moderator together with
                // the stable Turn identity; no parallel administration-audit schema is required.
                self.append_and_project_turn_event_in_transaction(
                    &transaction,
                    prepared_event,
                    changed_at,
                    unix_to_datetime(
                        request
                            .changed_at_unix
                            .saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS),
                    ),
                    false,
                )
                .await?;
                Ok(DeleteTurnMessageResult {
                    turn: event.turn.clone(),
                    event: Some(event),
                })
            }
            .await;

            match outcome {
                Ok(result) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit Turn message delete transaction")?;
                    Ok(result)
                }
                Err(error) => Err(crate::store::connection::rollback_preserving_error(transaction, error).await),
            }
        }
        .await;
        let maintenance_changed = maintenance_result
            .as_ref()
            .is_ok_and(|outcome| outcome.event.is_some());
        if maintenance_result.is_ok() {
            payload_change
                .complete(&self.connection, maintenance_changed, &receipt_keys)
                .await;
            receipt_change.completed_keys(if maintenance_changed {
                &receipt_keys
            } else {
                &[]
            });
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn get_turn_message_revisions(
        &self,
        turn_id: &str,
        before_revision: Option<u64>,
        limit: u64,
    ) -> Result<Vec<TurnMessageRevision>> {
        pioneer_crud_conversations::turn_message_revision::list_turn_message_revisions(
            &self.connection,
            turn_id,
            before_revision,
            limit,
        )
        .await?
        .into_iter()
        .map(pioneer_crud_conversations::turn_message_revision::turn_message_revision_from_model)
        .collect()
    }
}
