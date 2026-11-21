//! Scoped preparation, completion, and retention of durable terminal effects.
use crate::gateway_control::{GatewayControlWake, control_work_change};
use crate::native_terminal_effect_outbox::{self, NativeTerminalEffectStats};
use crate::time::unix_to_datetime;
use anyhow::{Context, Result};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};
use std::{future::Future, time::Duration};

pub struct TerminalEffectStore<'a> {
    connection: &'a SqliteDatabase,
}

impl<'a> TerminalEffectStore<'a> {
    pub fn new(connection: &'a SqliteDatabase) -> Self {
        Self { connection }
    }

    async fn run_serialized_write<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            operation,
            is_retryable_database_conflict,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn prepare_native_terminal_effects(
        &self,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        now_unix: i64,
    ) -> Result<()> {
        let control_wakes = vec![
            GatewayControlWake::NativeRetention,
            GatewayControlWake::Actions,
        ];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let prepared = native_terminal_effect_outbox::prepare_input(preparation)?;
            self.run_serialized_write(|| {
                let prepared = prepared.clone();
                async move {
                    let transaction = self.connection.begin().await.context(
                        "failed to begin native terminal-effect preparation transaction",
                    )?;
                    let result = native_terminal_effect_outbox::prepare(
                        &transaction,
                        prepared,
                        unix_to_datetime(now_unix),
                    )
                    .await;
                    match result {
                        Ok(()) => transaction
                            .commit()
                            .await
                            .context("failed to commit native terminal-effect preparation"),
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
        if control_result.is_ok() {
            control_change.completed(true);
        }
        control_result
    }

    pub async fn post_turn_batch_exists(&self, turn_id: &str, batch_id: &str) -> Result<bool> {
        native_terminal_effect_outbox::post_turn_batch_exists(self.connection, turn_id, batch_id)
            .await
    }

    pub async fn prepare_cli_post_turn_once(
        &self,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        expected_cli_attempt_index: i64,
        now_unix: i64,
    ) -> Result<()> {
        let prepared = native_terminal_effect_outbox::prepare_input(preparation)?;
        self.run_serialized_write(|| {
            let prepared = prepared.clone();
            async move {
                let tx = self.connection.begin().await?;
                native_terminal_effect_outbox::prepare_post_turn_once(
                    &tx,
                    prepared,
                    expected_cli_attempt_index,
                    unix_to_datetime(now_unix),
                )
                .await?;
                tx.commit().await?;
                Ok(())
            }
        })
        .await
    }

    pub async fn next_native_terminal_effect_recovery_deadline(
        &self,
        now_unix: i64,
    ) -> Result<Option<i64>> {
        native_terminal_effect_outbox::next_unresolved_retry_at(
            self.connection,
            unix_to_datetime(now_unix),
        )
        .await
    }

    pub async fn next_native_terminal_effect_execution_deadline(&self) -> Result<Option<i64>> {
        native_terminal_effect_outbox::next_execution_deadline(self.connection).await
    }

    pub async fn requeue_retryable_unresolved_native_terminal_effects(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<u64> {
        self.run_serialized_write(|| {
            native_terminal_effect_outbox::requeue_retryable_unresolved(
                self.connection,
                unix_to_datetime(now_unix),
                unix_to_datetime(now_unix.saturating_sub(60 * 60)),
                unix_to_datetime(now_unix.saturating_sub(24 * 60 * 60)),
                limit,
            )
        })
        .await
    }

    pub async fn complete_native_terminal_effect(
        &self,
        effect_id: &str,
        claim_token: &str,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::NativeRetention,
            GatewayControlWake::Actions,
        ];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            self.run_serialized_write(|| {
                native_terminal_effect_outbox::mark_succeeded(
                    self.connection,
                    effect_id,
                    claim_token,
                    unix_to_datetime(now_unix),
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    pub async fn native_terminal_effect_handler_checkpoint(
        &self,
        effect_id: &str,
        claim_token: &str,
    ) -> Result<Option<String>> {
        native_terminal_effect_outbox::load_handler_checkpoint(
            self.connection,
            effect_id,
            claim_token,
        )
        .await
    }

    pub async fn store_native_terminal_effect_handler_checkpoint(
        &self,
        effect_id: &str,
        claim_token: &str,
        checkpoint_json: &str,
        now_unix: i64,
    ) -> Result<()> {
        self.run_serialized_write(|| {
            native_terminal_effect_outbox::store_handler_checkpoint(
                self.connection,
                effect_id,
                claim_token,
                checkpoint_json,
                unix_to_datetime(now_unix),
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fail_native_terminal_effect(
        &self,
        effect_id: &str,
        claim_token: &str,
        error_code: &str,
        error_message: &str,
        retryable: bool,
        retry_at_unix: i64,
        now_unix: i64,
    ) -> Result<bool> {
        self.run_serialized_write(|| {
            native_terminal_effect_outbox::mark_failed(
                self.connection,
                effect_id,
                claim_token,
                error_code,
                error_message,
                retryable,
                unix_to_datetime(retry_at_unix),
                unix_to_datetime(now_unix),
            )
        })
        .await
    }

    pub async fn native_terminal_effect_stats(&self) -> Result<NativeTerminalEffectStats> {
        native_terminal_effect_outbox::load_stats(self.connection).await
    }

    pub async fn purge_resolved_native_terminal_effects_before(
        &self,
        cutoff_unix: i64,
        limit: u64,
    ) -> Result<u64> {
        self.run_serialized_write(|| {
            native_terminal_effect_outbox::purge_resolved_before(
                self.connection,
                unix_to_datetime(cutoff_unix),
                limit,
            )
        })
        .await
    }

    pub async fn native_lifecycle_read_write_probe(&self) -> Result<()> {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin native lifecycle readiness transaction")?;
            transaction
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Postgres,
                    "SELECT 1 AS ready".to_owned(),
                ))
                .await
                .context("failed native lifecycle readiness read probe")?
                .context("native lifecycle readiness read probe returned no row")?;
            transaction
                .execute_unprepared("UPDATE workspace SET id = id WHERE FALSE")
                .await
                .context("failed native lifecycle readiness write probe")?;
            transaction
                .commit()
                .await
                .context("failed to commit native lifecycle readiness probe")
        })
        .await
    }
}

#[cfg(test)]
#[path = "terminal_effect_store_tests.rs"]
mod tests;
