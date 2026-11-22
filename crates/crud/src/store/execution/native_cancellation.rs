use crate::store::events::event_preparation::TurnEventProjectionContext;
use crate::{
    CanonicalTurnEventPayload, CrudStore, NativeCancellationContext, TurnExecutionRecord,
    TurnExecutionStatus, utc_now,
};
use anyhow::{Context, Result};
use pioneer_crud_execution::cli_runtime_store::native_recovery_start_transition_allowed;
use pioneer_crud_execution::native_cancellation_context;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::{
    event_append as turn_event, native_terminal_effect_outbox, turn_execution,
};
use sea_orm::TransactionTrait;

#[derive(Clone, Debug)]
pub(crate) struct PreparedNativeCancellation {
    pub(crate) revalidation: sea_orm::Statement,
    pub(crate) effects: native_terminal_effect_outbox::PreparedNativeTerminalEffectPreparation,
}

impl CrudStore {
    /// Bounded durable acceptance proof for a local terminal pending projection.
    pub async fn native_turn_has_accepted_terminal(&self, turn_id: &str) -> Result<bool> {
        pioneer_crud_conversations::projection_receipts::has_accepted_terminal(
            &self.connection,
            turn_id,
        )
        .await
    }

    /// Persist only a bounded immutable cancellation description, before provider work.
    pub async fn persist_native_cancellation_context(
        &self,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        owner_id: &str,
        now_unix: i64,
        initial_turn: bool,
    ) -> Result<()> {
        self.persist_native_cancellation_context_with_source(
            preparation,
            owner_id,
            now_unix,
            initial_turn,
            None,
            None,
        )
        .await
    }

    pub async fn confirm_native_recovery_cancellation_context(
        &self,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        execution: &TurnExecutionRecord,
        recovery: &pioneer_protocol::RecoveryAttemptContext,
        owner_id: &str,
        now_unix: i64,
    ) -> Result<()> {
        anyhow::ensure!(
            execution.owner_id == owner_id
                && execution.turn_id == preparation.turn_id
                && execution.thread_id == preparation.thread_id
                && execution.workspace_id == preparation.workspace_id,
            "native recovery source scope changed"
        );
        self.persist_native_cancellation_context_with_source(
            preparation,
            owner_id,
            now_unix,
            false,
            Some((execution, recovery)),
            None,
        )
        .await
    }

    /// Foreground activation is admitted by the original durable New receipt,
    /// atomically with registration and against Stop/replacement.
    pub async fn confirm_foreground_start_cancellation_context(
        &self,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        expected: &TurnExecutionRecord,
        now_unix: i64,
    ) -> Result<()> {
        anyhow::ensure!(
            preparation.turn_id == expected.turn_id
                && preparation.thread_id == expected.thread_id
                && preparation.workspace_id == expected.workspace_id,
            "foreground activation scope changed"
        );
        self.persist_native_cancellation_context_with_source(
            preparation,
            &expected.owner_id,
            now_unix,
            true,
            None,
            Some(expected),
        )
        .await
    }

    async fn persist_native_cancellation_context_with_source(
        &self,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        owner_id: &str,
        now_unix: i64,
        initial_turn: bool,
        recovery_source: Option<(
            &TurnExecutionRecord,
            &pioneer_protocol::RecoveryAttemptContext,
        )>,
        foreground_source: Option<&TurnExecutionRecord>,
    ) -> Result<()> {
        native_terminal_effect_outbox::prepare_input(preparation.clone())?;
        let candidate = native_cancellation_context::prepare_registration(
            &preparation,
            owner_id,
            unix_to_datetime(now_unix),
        )?;
        self.retry_write_on_conflict(|| async {
            let candidate = candidate.clone();
            let tx = self.connection.begin().await?;
            pioneer_crud_conversations::turn_event::lock_stream(&tx, &preparation.turn_id).await?;
            if let Some((execution, recovery)) = recovery_source {
                anyhow::ensure!(
                    native_recovery_start_transition_allowed(&tx, execution, recovery, now_unix)
                        .await?,
                    "native recovery start source changed before ACK"
                );
            }
            anyhow::ensure!(
                !pioneer_crud_conversations::projection_receipts::has_accepted_terminal(
                    &tx,
                    &preparation.turn_id,
                )
                .await?,
                "native cancellation context cannot confirm an accepted terminal Turn"
            );
            if let Some(execution) = turn_execution::find(&tx, &preparation.turn_id).await? {
                anyhow::ensure!(
                    execution.owner_id == owner_id
                        && execution.thread_id == preparation.thread_id
                        && execution.workspace_id == preparation.workspace_id
                        && execution.status.is_active(),
                    "native cancellation context has stale execution scope or owner"
                );
            }
            if let Some(expected) = foreground_source {
                let current = turn_execution::find(&tx, &expected.turn_id)
                    .await?
                    .context("foreground activation execution missing")?;
                anyhow::ensure!(
                    current.owner_id == expected.owner_id
                        && current.owner_generation == expected.owner_generation
                        && current.thread_id == expected.thread_id
                        && current.workspace_id == expected.workspace_id
                        && matches!(
                            current.status,
                            TurnExecutionStatus::Starting | TurnExecutionStatus::Running
                        )
                        && current.lease_until > utc_now(),
                    "foreground activation source changed"
                );
            }
            native_cancellation_context::insert_once(
                &tx,
                &preparation,
                candidate,
                initial_turn,
                owner_id,
            )
            .await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }

    pub async fn native_cancellation_context(
        &self,
        turn_id: &str,
    ) -> Result<Option<NativeCancellationContext>> {
        native_cancellation_context::load(&self.connection, turn_id).await
    }

    pub async fn native_cancellation_was_accepted(&self, turn_id: &str) -> Result<bool> {
        native_cancellation_context::has_accepted(&self.connection, turn_id).await
    }

    pub async fn native_cancellation_was_accepted_owned(
        &self,
        turn_id: &str,
        owner_id: &str,
    ) -> Result<bool> {
        native_cancellation_context::has_accepted_owned(&self.connection, turn_id, owner_id).await
    }

    pub async fn native_cancellation_receipt_owned(
        &self,
        turn_id: &str,
        owner_id: &str,
    ) -> Result<Option<pioneer_protocol::NativeDurableCancellationReceipt>> {
        native_cancellation_context::receipt_owned(&self.connection, turn_id, owner_id).await
    }

    pub async fn native_cancellation_accepted_notification_owned(
        &self,
        turn_id: &str,
        owner_id: &str,
    ) -> Result<Option<pioneer_protocol::TurnFailedNotification>> {
        let Some(receipt) = self
            .native_cancellation_receipt_owned(turn_id, owner_id)
            .await?
        else {
            return Ok(None);
        };
        let accepted = turn_event::find_event_by_id(&self.connection, &receipt.canonical_event_id)
            .await?
            .context("accepted native cancellation has no canonical event")?;
        let CanonicalTurnEventPayload::TurnFailed(notification) = accepted.payload else {
            anyhow::bail!("accepted native cancellation has a conflicting canonical event");
        };
        anyhow::ensure!(
            notification.turn.id == receipt.turn_id
                && notification.thread_id == receipt.thread_id
                && notification.workspace_id == receipt.workspace_id
                && notification.turn.status == pioneer_protocol::TurnStatus::Interrupted,
            "accepted native cancellation has conflicting canonical identity"
        );
        Ok(Some(notification))
    }

    /// Canonical append + prepared obligations + receipt are one transaction.
    /// Projection and activation retain the existing subsequent atomic boundary.
    pub async fn materialize_native_cancellation_owned(
        &self,
        event: CanonicalTurnEventPayload,
        now_unix: i64,
        context: NativeCancellationContext,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        owner_id: &str,
    ) -> Result<()> {
        self.materialize_native_cancellation_owned_as(
            event,
            now_unix,
            context,
            preparation,
            owner_id,
            None,
        )
        .await
    }

    /// Accept the authenticated Stop actor with the canonical interruption.
    /// The saved per-run intent survives projection receipt compaction/restart;
    /// the personal receipt is deferred to the actual RunCancelled transaction.
    pub async fn materialize_native_cancellation_owned_as(
        &self,
        event: CanonicalTurnEventPayload,
        now_unix: i64,
        context: NativeCancellationContext,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        owner_id: &str,
        cancellation_actor_id: Option<String>,
    ) -> Result<()> {
        self.materialize_native_cancellation_with_generation(
            event,
            now_unix,
            context,
            preparation,
            owner_id,
            cancellation_actor_id,
            None,
        )
        .await
    }

    pub async fn materialize_native_cancellation_owned_at_generation(
        &self,
        event: CanonicalTurnEventPayload,
        now_unix: i64,
        context: NativeCancellationContext,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        owner_id: &str,
        cancellation_actor_id: Option<String>,
        expected_generation: u64,
    ) -> Result<()> {
        self.materialize_native_cancellation_with_generation(
            event,
            now_unix,
            context,
            preparation,
            owner_id,
            cancellation_actor_id,
            Some(expected_generation),
        )
        .await
    }

    async fn materialize_native_cancellation_with_generation(
        &self,
        event: CanonicalTurnEventPayload,
        now_unix: i64,
        context: NativeCancellationContext,
        preparation: pioneer_protocol::NativeTerminalEffectPreparation,
        owner_id: &str,
        cancellation_actor_id: Option<String>,
        expected_generation: Option<u64>,
    ) -> Result<()> {
        anyhow::ensure!(
            matches!(&event, CanonicalTurnEventPayload::TurnFailed(n)
            if n.turn.status == pioneer_protocol::TurnStatus::Interrupted
                && n.turn.id == preparation.turn_id && n.thread_id == preparation.thread_id
                && n.workspace_id == preparation.workspace_id),
            "native cancellation event has mismatched scope or status"
        );
        native_cancellation_context::validate_derived_preparation(&context, &preparation)?;
        let event = if let Some(event_id) = context.accepted_event_id.as_deref() {
            let accepted = turn_event::find_event_by_id(&self.connection, event_id)
                .await?
                .context("native cancellation receipt has no canonical event")?;
            anyhow::ensure!(
                accepted.turn_id == preparation.turn_id
                    && accepted.thread_id == preparation.thread_id
                    && matches!(&accepted.payload, CanonicalTurnEventPayload::TurnFailed(n)
                    if n.turn.status == pioneer_protocol::TurnStatus::Interrupted
                        && n.workspace_id == preparation.workspace_id),
                "native cancellation receipt has conflicting canonical result"
            );
            accepted.payload
        } else {
            event
        };
        let effects = native_terminal_effect_outbox::prepare_input(preparation)?;
        let revalidation = native_cancellation_context::prepare_revalidation(&context, owner_id);
        let generation_guard = expected_generation.map(|generation| -> Result<_> {
            Ok(sea_orm::Statement::from_sql_and_values(
                sea_orm::DbBackend::Postgres,
                "SELECT 1 FROM turn_execution WHERE turn_id = $1 AND owner_id = $2 AND owner_generation = $3 FOR UPDATE",
                [context.preparation.turn_id.clone().into(), owner_id.to_owned().into(), i64::try_from(generation)?.into()],
            ))
        }).transpose()?;
        self.materialize_turn_event_with_projection_context_and_guard(
            event,
            now_unix,
            TurnEventProjectionContext {
                item_started_deadlines: None,
                enqueue_optional_deliveries: true,
                cancellation_actor_id,
                unactivated_terminal: None,
                native_cancellation: Some(PreparedNativeCancellation {
                    revalidation,
                    effects,
                }),
            },
            Some(owner_id),
            generation_guard,
        )
        .await
    }

    pub(crate) fn terminal_effect_store(
        &self,
    ) -> pioneer_crud_execution::terminal_effect_store::TerminalEffectStore<'_> {
        pioneer_crud_execution::terminal_effect_store::TerminalEffectStore::new(&self.connection)
    }
}
