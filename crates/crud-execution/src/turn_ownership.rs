//! Durable ownership and activation fences for native execution.
use crate::execution_heartbeat::Registries;
use crate::gateway_control::{GatewayControlWake, control_work_change};
use crate::retry::run_serialized_write;
use crate::time::{unix_to_datetime, utc_now};
use crate::turn_execution::{self, TurnExecutionRecord};
use anyhow::{Context, Result};
use pioneer_crud_conversations::projection_receipts::has_accepted_terminal;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::TransactionTrait;

pub struct TurnOwnership<'a> {
    connection: &'a SqliteDatabase,
    execution_heartbeats: &'a Registries,
}

impl<'a> TurnOwnership<'a> {
    pub fn new(connection: &'a SqliteDatabase, execution_heartbeats: &'a Registries) -> Self {
        Self {
            connection,
            execution_heartbeats,
        }
    }

    pub async fn mark_turn_execution_running_current(
        &self,
        expected: &TurnExecutionRecord,
        lease_seconds: i64,
    ) -> Result<bool> {
        let control_wakes = vec![GatewayControlWake::Supervision];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            run_serialized_write(|| async {
                let tx = self.connection.begin().await?;
                if has_accepted_terminal(&tx, &expected.turn_id).await? {
                    return Ok(false);
                }
                let now = chrono::Utc::now().fixed_offset();
                let updated = turn_execution::mark_running_current(
                    &tx,
                    expected,
                    now,
                    now + chrono::Duration::seconds(lease_seconds),
                )
                .await?;
                tx.commit().await?;
                Ok(updated)
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    pub async fn settle_foreground_start_for_recovery(
        &self,
        expected: &TurnExecutionRecord,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            run_serialized_write(|| async {
                let tx = self.connection.begin().await?;
                let Some(current) = turn_execution::find(&tx, &expected.turn_id).await? else {
                    return Ok(false);
                };
                if current.owner_id != expected.owner_id
                    || current.owner_generation != expected.owner_generation
                    || current.thread_id != expected.thread_id
                    || current.workspace_id != expected.workspace_id
                    || !current.status.is_active()
                    || has_accepted_terminal(&tx, &expected.turn_id).await?
                {
                    return Ok(false);
                }
                let updated = turn_execution::mark_recovering_owned(
                    &tx,
                    &expected.turn_id,
                    &expected.owner_id,
                    utc_now(),
                )
                .await?;
                tx.commit().await?;
                Ok(updated)
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    pub async fn mark_turn_execution_running_owned(
        &self,
        turn_id: &str,
        owner_id: &str,
        started_at_unix: i64,
        lease_until_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![GatewayControlWake::Supervision];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let turn_id = turn_id.to_owned();
            let owner_id = owner_id.to_owned();
            run_serialized_write(|| async {
                turn_execution::mark_running_owned(
                    self.connection,
                    turn_id.as_str(),
                    owner_id.as_str(),
                    unix_to_datetime(started_at_unix),
                    unix_to_datetime(lease_until_unix),
                )
                .await
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    pub async fn claim_unactivated_agent_turn(
        &self,
        expected: &TurnExecutionRecord,
    ) -> Result<bool> {
        run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            if has_accepted_terminal(&tx, &expected.turn_id).await? {
                return Ok(false);
            }
            // Writer admission (and a storage retry) may outlive the lease.
            // Read the clock in this attempt, after acquiring the writer and
            // immediately before evaluating the activation fence.
            let claimed = turn_execution::claim_unactivated(&tx, expected, utc_now()).await?;
            tx.commit().await?;
            Ok(claimed)
        })
        .await
    }

    pub async fn list_expired_foreign_turn_executions(
        &self,
        current_owner_id: &str,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<TurnExecutionRecord>> {
        turn_execution::list_expired_foreign_active(
            self.connection,
            current_owner_id,
            unix_to_datetime(now_unix),
            limit,
        )
        .await
    }

    pub async fn list_owned_recovering_turn_executions(
        &self,
        owner_id: &str,
        limit: u64,
    ) -> Result<Vec<TurnExecutionRecord>> {
        turn_execution::list_owned_recovering(self.connection, owner_id, limit).await
    }

    pub async fn claim_expired_turn_execution(
        &self,
        expected: &TurnExecutionRecord,
        new_owner_id: &str,
        claimed_at_unix: i64,
        lease_until_unix: i64,
    ) -> Result<Option<TurnExecutionRecord>> {
        let control_wakes = vec![GatewayControlWake::Supervision];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let _ownership_change = self.execution_heartbeats.changing(new_owner_id);
            let expected = expected.clone();
            let new_owner_id = new_owner_id.to_owned();
            run_serialized_write(|| async {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin Turn execution ownership claim")?;
                let claimed = turn_execution::claim_expired(
                    &transaction,
                    &expected,
                    new_owner_id.as_str(),
                    unix_to_datetime(claimed_at_unix),
                    unix_to_datetime(lease_until_unix),
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit Turn execution ownership claim")?;
                if let Some(execution) = claimed.as_ref() {
                    _ownership_change.register(execution);
                }
                Ok(claimed)
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(value.is_some());
        }
        control_result
    }

    pub async fn record_agent_execution_progress(
        &self,
        execution_id: &str,
        expected_attempt_generation: i64,
        progress_frontier_json: &str,
        progress_at: i64,
        idle_deadline_at: Option<i64>,
    ) -> Result<bool> {
        let change = control_work_change(
            self.connection,
            "gateway.control",
            &[GatewayControlWake::Resources],
        );
        let result = run_serialized_write(|| async {
            let transaction = self.connection.begin().await?;
            let changed = crate::agent_execution::record_agent_execution_progress(
                &transaction,
                execution_id,
                expected_attempt_generation,
                progress_frontier_json,
                unix_to_datetime(progress_at),
                idle_deadline_at.map(unix_to_datetime),
            )
            .await?;
            transaction.commit().await?;
            Ok(changed)
        })
        .await;
        if let Ok(changed) = &result {
            change.completed(*changed);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn agent_activation_claim_rechecks_lease_after_waiting_for_writer() {
        pioneer_postgres_test::with_database(async |fixture| {
            use crate::turn_execution::{NewTurnExecution, TurnExecutionStatus, TurnExecutorKind};
            let workspace_id = "ws_activation_lease";
            let thread_id = "thr_activation_lease";
            let turn_id = "turn_activation_lease";
            let db =
                crate::test_fixture::fixture_event_scope(fixture, workspace_id, thread_id, turn_id)
                    .await;
            let registries = Registries::default();
            let store = TurnOwnership::new(&db, &registries);
            let now = utc_now();
            let owner = turn_execution::insert_immutable(
                &db,
                NewTurnExecution {
                    turn_id: turn_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    workspace_id: workspace_id.to_owned(),
                    executor_kind: TurnExecutorKind::ApiProvider,
                    executor_key: Some("openai".to_owned()),
                    status: TurnExecutionStatus::Starting,
                    owner_id: "activation-owner".to_owned(),
                    lease_until: now + chrono::Duration::seconds(2),
                    created_at: now,
                },
            )
            .await
            .unwrap();
            let writer = db.begin().await.unwrap();
            let mut claim = Box::pin(store.claim_unactivated_agent_turn(&owner));
            // Use the existing queued-future synchronization pattern: first prove
            // that activation is waiting for the occupied writer with a live lease.
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(claim.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            assert!(utc_now() < owner.lease_until);
            // Deliberately retain this fixture's writer across the lease boundary;
            // the waiter must evaluate time only after that writer is released.
            tokio::time::sleep(
                (owner.lease_until - utc_now()).to_std().unwrap_or_default()
                    + std::time::Duration::from_millis(1),
            )
            .await;
            writer.rollback().await.unwrap();
            assert!(!claim.await.unwrap());
            let unchanged = turn_execution::find(&db, turn_id).await.unwrap().unwrap();
            assert_eq!(unchanged.status, TurnExecutionStatus::Starting);
            assert_eq!(unchanged.owner_generation, owner.owner_generation);
            assert!(unchanged.started_at.is_none());

            // An actually renewed lease can be claimed once; a duplicate claim
            // still cannot reuse the starting receipt.
            crate::execution_heartbeat::renew(&db, "activation-owner", 2)
                .await
                .unwrap();
            let renewed = turn_execution::find(&db, turn_id).await.unwrap().unwrap();
            assert!(store.claim_unactivated_agent_turn(&renewed).await.unwrap());
            let activated = turn_execution::find(&db, turn_id).await.unwrap().unwrap();
            assert_eq!(activated.status, TurnExecutionStatus::Starting);
            assert!(activated.started_at.is_some());
            assert_eq!(activated.owner_generation, renewed.owner_generation);
            assert!(!store.claim_unactivated_agent_turn(&renewed).await.unwrap());
            Ok(())
        })
        .await
        .unwrap();
    }
}
