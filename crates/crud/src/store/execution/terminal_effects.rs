use crate::{
    CrudStore, NativeLifecycleDurableHealthSnapshot, NativeTerminalEffectClaimOutcome,
    NativeTerminalEffectStats, NativeTerminalEffectStatusRecord,
};
use anyhow::Result;
use pioneer_crud_execution::native_terminal_effect_outbox;

impl CrudStore {
    pub async fn prepare_native_terminal_effects(
        &self,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        now_unix: i64,
    ) -> Result<()> {
        self.terminal_effect_store()
            .prepare_native_terminal_effects(preparation, now_unix)
            .await
    }

    pub async fn post_turn_batch_exists(&self, turn_id: &str, batch_id: &str) -> Result<bool> {
        self.terminal_effect_store()
            .post_turn_batch_exists(turn_id, batch_id)
            .await
    }

    pub async fn prepare_cli_post_turn_once(
        &self,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        expected_cli_attempt_index: i64,
        now_unix: i64,
    ) -> Result<()> {
        self.terminal_effect_store()
            .prepare_cli_post_turn_once(preparation, expected_cli_attempt_index, now_unix)
            .await
    }

    pub async fn claim_due_native_terminal_effects(
        &self,
        now_unix: i64,
        claim_lease_secs: u64,
        limit: u64,
    ) -> Result<NativeTerminalEffectClaimOutcome> {
        self.claim_due_native_terminal_effects_with_clock(
            now_unix,
            claim_lease_secs,
            limit,
            &|| chrono::Utc::now().timestamp(),
        )
        .await
    }

    /// Deterministic clock for terminal-effect regression fixtures. Production
    /// uses `claim_due_native_terminal_effects` and reads UTC after admission.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub async fn claim_due_native_terminal_effects_at(
        &self,
        now_unix: i64,
        claim_lease_secs: u64,
        limit: u64,
    ) -> Result<NativeTerminalEffectClaimOutcome> {
        self.claim_due_native_terminal_effects_with_clock(
            now_unix,
            claim_lease_secs,
            limit,
            &|| now_unix,
        )
        .await
    }

    pub(crate) async fn claim_due_native_terminal_effects_with_clock(
        &self,
        now_unix: i64,
        claim_lease_secs: u64,
        limit: u64,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<NativeTerminalEffectClaimOutcome> {
        native_terminal_effect_outbox::claim_records(
            &self.connection,
            now_unix,
            claim_lease_secs,
            limit,
            clock,
        )
        .await
    }

    pub async fn next_native_terminal_effect_recovery_deadline(
        &self,
        now_unix: i64,
    ) -> Result<Option<i64>> {
        self.terminal_effect_store()
            .next_native_terminal_effect_recovery_deadline(now_unix)
            .await
    }

    pub async fn next_native_terminal_effect_execution_deadline(&self) -> Result<Option<i64>> {
        self.terminal_effect_store()
            .next_native_terminal_effect_execution_deadline()
            .await
    }

    pub async fn requeue_retryable_unresolved_native_terminal_effects(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<u64> {
        self.terminal_effect_store()
            .requeue_retryable_unresolved_native_terminal_effects(now_unix, limit)
            .await
    }

    pub async fn complete_native_terminal_effect(
        &self,
        effect_id: &str,
        claim_token: &str,
        now_unix: i64,
    ) -> Result<bool> {
        self.terminal_effect_store()
            .complete_native_terminal_effect(effect_id, claim_token, now_unix)
            .await
    }

    pub async fn native_terminal_effect_handler_checkpoint(
        &self,
        effect_id: &str,
        claim_token: &str,
    ) -> Result<Option<String>> {
        self.terminal_effect_store()
            .native_terminal_effect_handler_checkpoint(effect_id, claim_token)
            .await
    }

    pub async fn store_native_terminal_effect_handler_checkpoint(
        &self,
        effect_id: &str,
        claim_token: &str,
        checkpoint_json: &str,
        now_unix: i64,
    ) -> Result<()> {
        self.terminal_effect_store()
            .store_native_terminal_effect_handler_checkpoint(
                effect_id,
                claim_token,
                checkpoint_json,
                now_unix,
            )
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
        self.terminal_effect_store()
            .fail_native_terminal_effect(
                effect_id,
                claim_token,
                error_code,
                error_message,
                retryable,
                retry_at_unix,
                now_unix,
            )
            .await
    }

    pub async fn native_terminal_effect_stats(&self) -> Result<NativeTerminalEffectStats> {
        self.terminal_effect_store()
            .native_terminal_effect_stats()
            .await
    }

    pub async fn purge_resolved_native_terminal_effects_before(
        &self,
        cutoff_unix: i64,
        limit: u64,
    ) -> Result<u64> {
        self.terminal_effect_store()
            .purge_resolved_native_terminal_effects_before(cutoff_unix, limit)
            .await
    }

    /// Bounded end-to-end database canary for process readiness. The update
    /// intentionally matches no rows: PostgreSQL still admits and commits a
    /// write transaction, while the probe mutates no domain state.
    pub async fn native_lifecycle_read_write_probe(&self) -> Result<()> {
        self.terminal_effect_store()
            .native_lifecycle_read_write_probe()
            .await
    }

    pub async fn native_lifecycle_durable_health_snapshot(
        &self,
        now_unix: i64,
        stale_after_secs: u64,
    ) -> Result<NativeLifecycleDurableHealthSnapshot> {
        self.recovery_store()
            .native_lifecycle_durable_health_snapshot(now_unix, stale_after_secs)
            .await
    }

    pub async fn native_terminal_effect_status(
        &self,
        effect_id: &str,
    ) -> Result<Option<NativeTerminalEffectStatusRecord>> {
        native_terminal_effect_outbox::load_status(&self.connection, effect_id).await
    }
}
