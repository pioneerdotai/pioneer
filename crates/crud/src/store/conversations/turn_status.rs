use crate::{CrudStore, GatewayControlWake, compaction, terminal_cleanup};
use anyhow::{Context, Result};
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::turn_execution;
use pioneer_crud_execution::turn_execution::terminal_turn_execution_status;
use pioneer_protocol::{Turn, TurnStatus};
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn update_turn_status(
        &self,
        thread_id: &str,
        turn_id: &str,
        status: TurnStatus,
        error: Option<&str>,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        let payload_change = terminal_cleanup::Change::payload(&self.connection);
        let payload_turns = vec![turn_id.to_owned()];
        let mut control_wakes = vec![GatewayControlWake::Turn(turn_id.to_owned())];
        if status != TurnStatus::InProgress {
            control_wakes.extend([
                GatewayControlWake::Resources,
                GatewayControlWake::Actions,
                GatewayControlWake::Ledger,
                GatewayControlWake::NativeRetention,
                GatewayControlWake::Supervision,
                GatewayControlWake::Terminalization,
            ]);
        }
        let changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("projection", &control_wakes),
            self.connection.work_signal("compaction").changing(),
        ];
        let result = async {
            let maintenance_changes = [
                self.connection.work_signal("maintenance.native").changing(),
                self.connection.work_signal("maintenance.html").changing(),
            ];
            let maintenance_result = async {
                let task_change = pioneer_crud_tasks::work_change::TaskTurnChange::begin(
                    &self.connection,
                    vec![turn_id.to_owned()],
                )
                .await?;
                let result = async {
                    let _release_wake = (!matches!(status, TurnStatus::InProgress))
                        .then(|| self.execution_heartbeats.release_wake());
                    self.run_serialized_write(|| async {
                        let updated_at = unix_to_datetime(event_timestamp_secs);
                        let transaction = self
                            .connection
                            .begin()
                            .await
                            .context("failed to begin Turn status transaction")?;
                        let updated = pioneer_crud_tasks::turn_sources::update_turn_status(
                            &transaction,
                            thread_id,
                            turn_id,
                            status,
                            error,
                            updated_at,
                        )
                        .await?;
                        if updated {
                            pioneer_crud_conversations::turn_writes::append_turn_status_history(
                                &transaction,
                                turn_id,
                                status,
                                error.map(str::to_owned),
                                updated_at,
                            )
                            .await?;
                            if let Some(execution_status) = terminal_turn_execution_status(status) {
                                turn_execution::mark_terminal(
                                    &transaction,
                                    turn_id,
                                    execution_status,
                                    updated_at,
                                )
                                .await?;
                            }
                        }
                        transaction
                            .commit()
                            .await
                            .context("failed to commit Turn status transaction")?;
                        Ok(updated)
                    })
                    .await
                }
                .await;
                if result.is_ok() {
                    task_change.completed(&self.connection).await?;
                }
                result
            }
            .await;
            let maintenance_changed = maintenance_result.as_ref().is_ok_and(|changed| *changed);
            if maintenance_result.is_ok() {
                for change in maintenance_changes {
                    change.completed(maintenance_changed);
                }
            }
            maintenance_result
        }
        .await;
        if let Ok(changed) = &result {
            payload_change
                .complete(&self.connection, *changed, &payload_turns)
                .await;
            for change in changes {
                change.completed(true);
            }
        }
        result
    }
}
