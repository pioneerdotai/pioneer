use crate::events::TurnEventPayload;
use crate::{CrudStore, GatewayControlWake, TurnItemAttemptDeadlines, terminal_cleanup};
use anyhow::{Context, Result};
use pioneer_crud_conversations::turn_item::turn_item_type_to_db;
use pioneer_crud_conversations::turn_liveness;
use pioneer_crud_execution::event_append as turn_event;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_timeline::timeline_live_projection::project_semantic_timeline_snapshot_turn_item;
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn materialize_item_started(
        &self,
        notification: pioneer_protocol::ItemStartedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event_with_attempt_deadlines(
            TurnEventPayload::ItemStarted(notification),
            event_timestamp_secs,
            None,
        )
        .await
    }

    pub async fn materialize_item_started_with_attempt_deadlines(
        &self,
        notification: pioneer_protocol::ItemStartedNotification,
        event_timestamp_secs: i64,
        deadlines: TurnItemAttemptDeadlines,
    ) -> Result<()> {
        self.materialize_turn_event_with_attempt_deadlines(
            TurnEventPayload::ItemStarted(notification),
            event_timestamp_secs,
            Some(deadlines),
        )
        .await
    }

    pub async fn materialize_item_completed(
        &self,
        notification: pioneer_protocol::ItemCompletedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemCompleted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_snapshot_updated(
        &self,
        notification: pioneer_protocol::ItemUpdatedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        let payload_change = terminal_cleanup::Change::payload(&self.connection);
        let payload_turns = vec![notification.turn_id.clone()];
        let control_wakes = vec![GatewayControlWake::Activity(notification.turn_id.clone())];
        let maintenance_changes = ["gateway.control", "compaction", "maintenance.html"]
            .map(|domain| self.control_work_change(domain, &control_wakes));
        let maintenance_result = async {
        let updated_at = unix_to_datetime(event_timestamp_secs);
        let status =
            crate::turn_item_terminal::terminal_turn_item_status_from_payload(&notification.item);
        let prepared_item = pioneer_crud_conversations::turn_item::prepare_turn_item_projection(
            notification.turn_id.as_str(),
            &notification.item,
            Some(status),
            updated_at,
            updated_at,
        )?;
        let episodic_source = self
            .prepare_episodic_source_save(
                &notification.workspace_id,
                &notification.thread_id,
                &notification.turn_id,
                &notification.item,
            )
            .await?;
        let timeline_item = serde_json::to_value(&notification.item)?;
        self.retry_write_on_conflict(|| {
            let notification = notification.clone();
            let connection = self.connection.clone();
            let mut prepared_item = prepared_item.clone();
            let episodic_source = episodic_source.clone();
            let timeline_item = timeline_item.clone();
            async move {
                let transaction = connection
                    .begin()
                    .await
                    .context("failed to begin item snapshot update transaction")?;

                let result: Result<()> = async {
                    let Some(_turn_model) = pioneer_crud_conversations::turn_reads::find_turn_by_thread_and_id(
                        &transaction,
                        notification.thread_id.as_str(),
                        notification.turn_id.as_str(),
                    )
                    .await?
                    else {
                        anyhow::bail!(
                            "item snapshot update cannot find turn `{}` in thread `{}`",
                            notification.turn_id,
                            notification.thread_id
                        );
                    };
                    let Some(thread_model) =
                        pioneer_crud_conversations::thread_metadata::find_thread_by_id(&transaction, notification.thread_id.as_str())
                            .await?
                    else {
                        anyhow::bail!(
                            "item snapshot update cannot find thread `{}`",
                            notification.thread_id
                        );
                    };
                    if thread_model.workspace_id != notification.workspace_id {
                        anyhow::bail!(
                            "item snapshot update workspace mismatch for thread `{}`: expected `{}`, got `{}`",
                            notification.thread_id,
                            thread_model.workspace_id,
                            notification.workspace_id
                        );
                    }

                    let source_sequence = turn_event::max_sequence_for_turn(
                        &transaction, notification.turn_id.as_str(),
                    ).await?;
                    prepared_item.preserve_uncommitted_source_status(&transaction).await?;
                    pioneer_crud_conversations::turn_item::upsert_prepared_turn_item(&transaction, prepared_item).await?;
                    self.commit_episodic_source_save(&transaction, &episodic_source, updated_at).await?;
                    project_semantic_timeline_snapshot_turn_item(
                        &*transaction,
                        notification.turn_id.as_str(),
                        notification.item.item_id(),
                        source_sequence,
                        updated_at,
                        timeline_item,
                    )
                    .await?;
                    turn_liveness::observe_activity(
                        &transaction,
                        turn_liveness::TurnLivenessObservation {
                            turn_id: notification.turn_id.clone(),
                            thread_id: notification.thread_id.clone(),
                            activity_sequence: source_sequence,
                            activity_kind: "item/snapshot_updated".to_owned(),
                            item_id: Some(notification.item.item_id().to_owned()),
                            item_type: Some(
                                turn_item_type_to_db(notification.item.item_type()).to_owned(),
                            ),
                            observed_at: updated_at,
                        },
                    )
                    .await?;
                    Ok(())
                }
                .await;

                match result {
                    Ok(()) => transaction
                        .commit()
                        .await
                        .context("failed to commit item snapshot update transaction"),
                    Err(error) => {
                        Err(crate::store::connection::rollback_preserving_error(transaction, error).await)
                    }
                }
            }
        })
        .await?;
        self.episodic_work.notify_one();
        Ok(())
        }.await;
        let maintenance_changed = maintenance_result.is_ok();
        if maintenance_result.is_ok() {
            payload_change
                .complete(&self.connection, maintenance_changed, &payload_turns)
                .await;
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn materialize_item_timeout_detected(
        &self,
        notification: pioneer_protocol::ItemTimeoutDetectedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemTimeoutDetected(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_recovery_opened(
        &self,
        notification: pioneer_protocol::ItemRecoveryOpenedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRecoveryOpened(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_recovery_attached(
        &self,
        notification: pioneer_protocol::ItemRecoveryAttachedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRecoveryAttached(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_retry_scheduled(
        &self,
        notification: pioneer_protocol::ItemRetryScheduledNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRetryScheduled(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_retry_attempt_started(
        &self,
        notification: pioneer_protocol::ItemRetryAttemptStartedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRetryAttemptStarted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_recovery_succeeded(
        &self,
        notification: pioneer_protocol::ItemRecoverySucceededNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRecoverySucceeded(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_recovery_exhausted(
        &self,
        notification: pioneer_protocol::ItemRecoveryExhaustedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemRecoveryExhausted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_tool_retry_scheduled(
        &self,
        notification: pioneer_protocol::ItemToolRetryScheduledNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemToolRetryScheduled(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_tool_retry_resolved(
        &self,
        notification: pioneer_protocol::ItemToolRetryResolvedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemToolRetryResolved(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_item_tool_retry_exhausted(
        &self,
        notification: pioneer_protocol::ItemToolRetryExhaustedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemToolRetryExhausted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_tool_loop_budget_exceeded(
        &self,
        notification: pioneer_protocol::TurnToolLoopBudgetExceededNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnToolLoopBudgetExceeded(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_started(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowStartedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowStarted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_exhausted(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowExhaustedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowExhausted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_checkpointed(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowCheckpointedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowCheckpointed(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_continued(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowContinuedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowContinued(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_execution_window_blocked(
        &self,
        notification: pioneer_protocol::TurnExecutionWindowBlockedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnExecutionWindowBlocked(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_permission_audit(
        &self,
        event: pioneer_protocol::TurnPermissionAuditEvent,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnPermissionAudit(event),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_completed(
        &self,
        notification: pioneer_protocol::TurnCompletedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnCompleted(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_failed(
        &self,
        notification: pioneer_protocol::TurnFailedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnFailed(notification),
            event_timestamp_secs,
        )
        .await
    }

    pub async fn materialize_turn_blocked(
        &self,
        notification: pioneer_protocol::TurnBlockedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::TurnBlocked(notification),
            event_timestamp_secs,
        )
        .await
    }
}
