use crate::{
    ActiveExecutionAuthorityRecord, ActiveTaskExecutionAuthorityRecord, CrudStore,
    TurnExecutionBinding, TurnExecutionRecord, TurnExecutionSecuritySnapshotRecord,
};
use anyhow::Result;
use pioneer_crud_execution::turn_ownership::TurnOwnership;
use pioneer_crud_execution::{execution_heartbeat, turn_admission, turn_execution};
use pioneer_crud_tasks::execution_store::TaskExecutionStore;
use pioneer_protocol::{TurnExecutionSecuritySnapshot, TurnStatus};

impl CrudStore {
    pub async fn list_active_execution_authorities(
        &self,
        after_turn_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<ActiveExecutionAuthorityRecord>> {
        pioneer_crud_execution::authority::active_turns(&self.connection, after_turn_id, limit)
            .await
    }

    pub async fn list_active_task_execution_authorities(
        &self,
        after_task_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<ActiveTaskExecutionAuthorityRecord>> {
        pioneer_crud_execution::authority::active_agent_tasks(
            &self.connection,
            after_task_id,
            limit,
        )
        .await
    }

    pub(crate) fn turn_execution_store(&self) -> TurnOwnership<'_> {
        TurnOwnership::new(&self.connection, &self.execution_heartbeats)
    }

    pub(crate) fn task_execution_store(&self) -> TaskExecutionStore<'_> {
        TaskExecutionStore::new(&self.connection)
    }

    pub async fn get_turn_admission(
        &self,
        turn_id: &str,
    ) -> Result<Option<pioneer_entity::turn_admission::Model>> {
        turn_admission::find(&self.connection, turn_id).await
    }

    pub async fn get_turn_execution(&self, turn_id: &str) -> Result<Option<TurnExecutionRecord>> {
        turn_execution::find(&self.connection, turn_id).await
    }

    /// Retained, owner-specific notification; scoped stores share the same registry.
    pub fn turn_execution_heartbeat_notification(
        &self,
        owner_id: &str,
    ) -> std::sync::Arc<tokio::sync::Notify> {
        self.execution_heartbeats.notification(owner_id)
    }

    /// Returns true only when an authoritative observation permits parking.
    pub async fn supervise_turn_execution_heartbeat(
        &self,
        owner_id: &str,
        lease_seconds: i64,
    ) -> Result<bool> {
        self.execution_heartbeats
            .supervise(&self.connection, owner_id, lease_seconds)
            .await
    }

    /// Extend active receipts using the database clock at each UPDATE attempt.
    pub async fn heartbeat_turn_executions_owned_by(
        &self,
        owner_id: &str,
        lease_seconds: i64,
    ) -> Result<u64> {
        execution_heartbeat::renew(&self.connection, owner_id, lease_seconds).await
    }

    /// Initial native handoff CAS, fenced against accepted terminal and exact
    /// process ownership generation in the same write transaction.
    pub async fn mark_turn_execution_running_current(
        &self,
        expected: &TurnExecutionRecord,
        lease_seconds: i64,
    ) -> Result<bool> {
        self.turn_execution_store()
            .mark_turn_execution_running_current(expected, lease_seconds)
            .await
    }

    /// Failed foreground handoff leaves an exact receipt discoverable by the
    /// existing Recovering scanner. Stale owners have no side effects.
    pub async fn settle_foreground_start_for_recovery(
        &self,
        expected: &TurnExecutionRecord,
    ) -> Result<bool> {
        self.turn_execution_store()
            .settle_foreground_start_for_recovery(expected)
            .await
    }

    pub async fn mark_turn_execution_running_owned(
        &self,
        turn_id: &str,
        owner_id: &str,
        started_at_unix: i64,
        lease_until_unix: i64,
    ) -> Result<bool> {
        self.turn_execution_store()
            .mark_turn_execution_running_owned(turn_id, owner_id, started_at_unix, lease_until_unix)
            .await
    }

    /// Fence duplicate outbox activations using the starting receipt's timestamp.
    /// This CAS precedes runtime registration. Canonical terminal acceptance
    /// also checks and updates the execution record.
    pub async fn claim_unactivated_agent_turn(
        &self,
        expected: &TurnExecutionRecord,
    ) -> Result<bool> {
        self.turn_execution_store()
            .claim_unactivated_agent_turn(expected)
            .await
    }

    pub async fn list_expired_foreign_turn_executions(
        &self,
        current_owner_id: &str,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<TurnExecutionRecord>> {
        self.turn_execution_store()
            .list_expired_foreign_turn_executions(current_owner_id, now_unix, limit)
            .await
    }

    pub async fn list_owned_recovering_turn_executions(
        &self,
        owner_id: &str,
        limit: u64,
    ) -> Result<Vec<TurnExecutionRecord>> {
        self.turn_execution_store()
            .list_owned_recovering_turn_executions(owner_id, limit)
            .await
    }

    pub async fn claim_expired_turn_execution(
        &self,
        expected: &TurnExecutionRecord,
        new_owner_id: &str,
        claimed_at_unix: i64,
        lease_until_unix: i64,
    ) -> Result<Option<TurnExecutionRecord>> {
        self.turn_execution_store()
            .claim_expired_turn_execution(expected, new_owner_id, claimed_at_unix, lease_until_unix)
            .await
    }

    pub async fn get_turn_execution_binding(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TurnExecutionBinding>> {
        pioneer_crud_access::turn_execution::find_turn_execution_binding(
            &self.connection,
            thread_id,
            turn_id,
        )
        .await
    }

    pub async fn get_turn_execution_status(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TurnStatus>> {
        pioneer_crud_access::turn_execution::find_turn_execution_status(
            &self.connection,
            thread_id,
            turn_id,
        )
        .await
    }

    pub async fn set_turn_execution_security_snapshot(
        &self,
        turn_id: &str,
        snapshot: &TurnExecutionSecuritySnapshot,
    ) -> Result<bool> {
        let turn_id = turn_id.to_owned();
        let snapshot = snapshot.clone();
        self.retry_write_on_conflict(|| async {
            pioneer_crud_execution::turn_envelope::set_turn_execution_security_snapshot(
                &self.connection,
                turn_id.as_str(),
                &snapshot,
            )
            .await
        })
        .await
    }

    /// Pin the authorized native budget before resuming the executor.
    pub async fn pin_turn_native_context(
        &self,
        workspace: &str,
        turn_id: &str,
        budget: &pioneer_protocol::NativeContextBudget,
        summary: Option<&pioneer_protocol::NativeSummaryContext>,
    ) -> Result<bool> {
        self.retry_write_on_conflict(|| async {
            pioneer_crud_execution::turn_envelope::pin_native_context(
                &self.connection,
                workspace,
                turn_id,
                budget,
                summary,
            )
            .await
        })
        .await
    }

    pub async fn get_turn_execution_security_snapshot(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnExecutionSecuritySnapshotRecord>> {
        let turn_id = turn_id.to_owned();
        self.retry_write_on_conflict(|| async {
            pioneer_crud_execution::turn_envelope::find_turn_execution_security_snapshot(
                &self.connection,
                turn_id.as_str(),
            )
            .await
        })
        .await
    }

    pub async fn set_turn_execution_envelope(
        &self,
        turn_id: &str,
        snapshot: &TurnExecutionSecuritySnapshot,
        authorization_context_json: &str,
    ) -> Result<bool> {
        let turn_id = turn_id.to_owned();
        let snapshot = snapshot.clone();
        let authorization_context_json = authorization_context_json.to_owned();
        self.retry_write_on_conflict(|| async {
            pioneer_crud_execution::turn_envelope::set_turn_execution_envelope(
                &self.connection,
                turn_id.as_str(),
                &snapshot,
                authorization_context_json.as_str(),
            )
            .await
        })
        .await
    }

    pub async fn set_turn_execution_authorization_context(
        &self,
        turn_id: &str,
        context_json: &str,
    ) -> Result<bool> {
        let turn_id = turn_id.to_owned();
        let context_json = context_json.to_owned();
        self.retry_write_on_conflict(|| async {
            pioneer_crud_execution::turn_envelope::set_turn_execution_authorization_context(
                &self.connection,
                turn_id.as_str(),
                context_json.as_str(),
            )
            .await
        })
        .await
    }

    /// Bounded read-only authority envelope selection for page validation.
    /// Durable derivation must still be verified by the authorization owner.
    pub async fn get_turn_execution_authorization_contexts(
        &self,
        turn_ids: &[String],
    ) -> Result<std::collections::HashMap<String, String>> {
        pioneer_crud_access::turn_execution::find_turn_execution_authorization_contexts(
            &self.connection,
            turn_ids,
        )
        .await
    }

    pub async fn get_turn_execution_authorization_context(
        &self,
        turn_id: &str,
    ) -> Result<Option<String>> {
        pioneer_crud_access::turn_execution::find_turn_execution_authorization_context(
            &self.connection,
            turn_id,
        )
        .await
    }
}
