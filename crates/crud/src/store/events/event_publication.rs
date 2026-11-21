use crate::DB_ID_LEN;
use crate::events::TurnEventPayload;
use crate::store::events::event_preparation::{
    TURN_EVENT_PROJECTION_LEASE_SECS, TurnEventProjectionContext,
};
use crate::store::events::event_projection::TurnEventProjectionOutcome;
use pioneer_crud_execution::time::unix_to_datetime;
use crate::{
    CanonicalTurnEventPayload, CliRuntimeTurnTerminalGuard, CrudStore,
    TurnEventProjectionAfterAppendError, TurnExecutionRecord, TurnExecutorKind,
    TurnItemAttemptDeadlines, UnactivatedTurnPhaseChanged,
    turn_event_was_appended_before_error,
};
use anyhow::{Context, Result};
use pioneer_crud_conversations::events;
use pioneer_crud_execution::{event_append as turn_event, turn_execution};
use pioneer_protocol::{TurnStatus, generate_id};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, is_retryable_database_conflict,
    retry_with_backoff,
};
use sea_orm::Statement;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use std::future::Future;
use std::time::Duration;
use tracing::{debug, warn};

impl CrudStore {
    pub(crate) async fn materialize_turn_event(
        &self,
        event: TurnEventPayload,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event_with_attempt_deadlines(event, event_timestamp_secs, None)
            .await
    }

    pub(crate) async fn materialize_turn_event_with_attempt_deadlines(
        &self,
        event: TurnEventPayload,
        event_timestamp_secs: i64,
        item_started_deadlines: Option<TurnItemAttemptDeadlines>,
    ) -> Result<()> {
        let projection_context = TurnEventProjectionContext {
            item_started_deadlines,
            enqueue_optional_deliveries: false,
            native_cancellation: None,
            unactivated_terminal: None,
            cancellation_actor_id: None,
        };
        self.materialize_turn_event_with_projection_context(
            event,
            event_timestamp_secs,
            projection_context,
        )
        .await
    }

    /// Materialize a native-agent causal event and atomically enqueue its
    /// optional live/episodic consumers. Other runtimes retain their existing
    /// delivery path and cannot accidentally receive duplicate fan-out.
    pub async fn materialize_native_agent_turn_event(
        &self,
        event: CanonicalTurnEventPayload,
        event_timestamp_secs: i64,
        item_started_deadlines: Option<TurnItemAttemptDeadlines>,
    ) -> Result<()> {
        self.materialize_native_agent_turn_event_with_owner(
            event,
            event_timestamp_secs,
            item_started_deadlines,
            None,
        )
        .await
    }

    /// Materialize a provider event only while the emitting Gateway still
    /// owns the durable execution lease. The owner check and canonical append
    /// share one transaction, fencing callbacks from a replaced process.
    pub async fn materialize_native_agent_turn_event_owned(
        &self,
        event: CanonicalTurnEventPayload,
        event_timestamp_secs: i64,
        item_started_deadlines: Option<TurnItemAttemptDeadlines>,
        execution_owner_id: &str,
    ) -> Result<()> {
        self.materialize_native_agent_turn_event_with_owner(
            event,
            event_timestamp_secs,
            item_started_deadlines,
            Some(execution_owner_id),
        )
        .await
    }

    /// CLI/legacy equivalent of native cancellation attribution. Attribution is
    /// saved only for a newly accepted Interrupted event and exact active runs.
    pub async fn materialize_native_agent_turn_event_owned_as(
        &self,
        event: CanonicalTurnEventPayload,
        event_timestamp_secs: i64,
        item_started_deadlines: Option<TurnItemAttemptDeadlines>,
        execution_owner_id: &str,
        cancellation_actor_id: Option<String>,
    ) -> Result<()> {
        self.materialize_turn_event_with_projection_context_and_owner(
            event,
            event_timestamp_secs,
            TurnEventProjectionContext {
                item_started_deadlines,
                enqueue_optional_deliveries: true,
                cancellation_actor_id,
                native_cancellation: None,
                unactivated_terminal: None,
            },
            Some(execution_owner_id),
        )
        .await
    }

    /// Accepted-but-unactivated cancellation uses the same canonical event
    /// pipeline. The writer revalidates ownership/generation and activation
    /// evidence atomically with acceptance; no synthetic native context exists.
    pub async fn materialize_unactivated_turn_cancellation_owned_as(
        &self,
        event: CanonicalTurnEventPayload,
        event_timestamp_secs: i64,
        expected: TurnExecutionRecord,
        execution_owner_id: &str,
        cancellation_actor_id: Option<String>,
    ) -> Result<()> {
        anyhow::ensure!(
            matches!(&event, CanonicalTurnEventPayload::TurnFailed(n)
            if n.turn.status == TurnStatus::Interrupted && n.turn.id == expected.turn_id
                && n.thread_id == expected.thread_id && n.workspace_id == expected.workspace_id)
                && expected.owner_id == execution_owner_id,
            "unactivated cancellation differs from its accepted execution"
        );
        self.materialize_turn_event_with_projection_context_and_owner(
            event,
            event_timestamp_secs,
            TurnEventProjectionContext {
                enqueue_optional_deliveries: true,
                cancellation_actor_id,
                unactivated_terminal: Some(expected),
                ..Default::default()
            },
            Some(execution_owner_id),
        )
        .await
    }

    /// Continue an early Stop after the initial CLI attempt won admission.
    /// The caller holds the existing CLI transition gate through cleanup.
    pub async fn materialize_cli_runtime_cancellation_owned_as(
        &self,
        notification: pioneer_protocol::TurnFailedNotification,
        expected: &TurnExecutionRecord,
        source: &CliRuntimeTurnTerminalGuard,
        cancellation_actor_id: Option<String>,
        now_unix: i64,
    ) -> Result<()> {
        let attempt = source
            .attempt
            .as_ref()
            .context("CLI cancellation attempt is missing")?;
        anyhow::ensure!(
            expected.executor_kind == TurnExecutorKind::CliRuntime
                && source.turn_status == TurnStatus::InProgress
                && attempt.status.is_active()
                && attempt.turn_id == expected.turn_id
                && attempt.runtime_id == source.binding.runtime_id
                && attempt.runtime_kind == source.binding.runtime_kind
                && notification.turn.status == TurnStatus::Interrupted
                && notification.turn.id == expected.turn_id
                && notification.thread_id == expected.thread_id
                && notification.workspace_id == expected.workspace_id
                && source.binding.turn_id == expected.turn_id
                && source.binding.thread_id == expected.thread_id
                && source.binding.workspace_id == expected.workspace_id
                && source.execution_owner.as_ref()
                    == Some(&(expected.owner_id.clone(), expected.owner_generation)),
            UnactivatedTurnPhaseChanged
        );
        // The canonical append pipeline accepts a Statement guard, evaluated
        // inside its writer transaction. A prior repository read cannot fence
        // an ownership transfer between gate acquisition and canonical append.
        let guard = sea_orm::Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres,
            "SELECT 1 FROM turn_execution e JOIN turn_cli_runtime_binding b ON b.turn_id = e.turn_id JOIN turn_cli_runtime_attempt a ON a.turn_id = e.turn_id WHERE e.turn_id = $1 AND e.owner_id = $2 AND e.owner_generation = $3 AND e.thread_id = $4 AND e.workspace_id = $5 AND b.runtime_id = $6 AND b.continuation_thread_id = $7 AND a.id = $8 AND a.status = $9 FOR SHARE OF e, b, a",
            [
                expected.turn_id.clone().into(),
                expected.owner_id.clone().into(),
                i64::try_from(expected.owner_generation)?.into(),
                expected.thread_id.clone().into(),
                expected.workspace_id.clone().into(),
                source.binding.runtime_id.clone().into(),
                source.binding.continuation_thread_id.clone().into(),
                attempt.id.clone().into(),
                attempt.status.as_str().into(),
            ],
        );
        self.materialize_turn_event_with_projection_context_and_guard(
            CanonicalTurnEventPayload::TurnFailed(notification),
            now_unix,
            TurnEventProjectionContext {
                enqueue_optional_deliveries: true,
                cancellation_actor_id,
                ..Default::default()
            },
            Some(&expected.owner_id),
            Some(guard),
        )
        .await
    }

    /// Definitive pre-activation refusals use the canonical terminal pipeline
    /// and the same owner/generation/activation fence as early cancellation.
    pub async fn materialize_unactivated_turn_refusal_owned(
        &self,
        event: CanonicalTurnEventPayload,
        event_timestamp_secs: i64,
        expected: TurnExecutionRecord,
    ) -> Result<()> {
        anyhow::ensure!(
            matches!(&event,
            CanonicalTurnEventPayload::TurnFailed(n) if n.turn.status == TurnStatus::Failed)
                || matches!(&event, CanonicalTurnEventPayload::TurnBlocked(n) if n.turn.status == TurnStatus::Blocked),
            "unactivated refusal requires a Failed or Blocked event"
        );
        anyhow::ensure!(
            event.turn_id() == expected.turn_id
                && event.thread_id() == expected.thread_id
                && event.workspace_id() == expected.workspace_id,
            "unactivated refusal differs from its execution scope"
        );
        let owner_id = expected.owner_id.clone();
        self.materialize_turn_event_with_projection_context_and_owner(
            event,
            event_timestamp_secs,
            TurnEventProjectionContext {
                enqueue_optional_deliveries: true,
                unactivated_terminal: Some(expected),
                ..Default::default()
            },
            Some(&owner_id),
        )
        .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn persisted_turn_event_for_test(
        &self,
        event_id: &str,
    ) -> Result<Option<CanonicalTurnEventPayload>> {
        Ok(turn_event::find_event_by_id(&self.connection, event_id)
            .await?
            .map(|event| event.payload))
    }

    /// Pause the ordinary parent's projector after durable acceptance. The
    /// production claim is retained, so duplicate materialization is pending.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn accept_turn_event_before_projection_for_test(
        &self,
        event: CanonicalTurnEventPayload,
        timestamp: i64,
    ) -> Result<()> {
        self.append_claimed_turn_event_projection_once(
            event,
            unix_to_datetime(timestamp),
            TurnEventProjectionContext {
                item_started_deadlines: None,
                enqueue_optional_deliveries: true,
                cancellation_actor_id: None,
                native_cancellation: None,
                unactivated_terminal: None,
            },
            generate_id(DB_ID_LEN),
            &|| unix_to_datetime(timestamp + 60),
            None,
            None,
        )
        .await?;
        Ok(())
    }

    /// Reproduce process loss after canonical acceptance and before projection,
    /// retaining the production owner fences and replay/delivery obligations.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn accept_native_turn_event_without_projection_for_test(
        &self,
        event: CanonicalTurnEventPayload,
        event_timestamp_secs: i64,
        execution_owner_id: &str,
    ) -> Result<()> {
        let now = unix_to_datetime(event_timestamp_secs);
        self.append_claimed_turn_event_projection_once(
            event,
            now,
            TurnEventProjectionContext {
                item_started_deadlines: None,
                enqueue_optional_deliveries: true,
                native_cancellation: None,
                unactivated_terminal: None,
                cancellation_actor_id: None,
            },
            generate_id(DB_ID_LEN),
            // An expired claim models the lost projector, without a timed wait.
            &|| now,
            Some(execution_owner_id),
            None,
        )
        .await?;
        Ok(())
    }

    async fn materialize_native_agent_turn_event_with_owner(
        &self,
        event: CanonicalTurnEventPayload,
        event_timestamp_secs: i64,
        item_started_deadlines: Option<TurnItemAttemptDeadlines>,
        execution_owner_id: Option<&str>,
    ) -> Result<()> {
        let projection_context = TurnEventProjectionContext {
            item_started_deadlines,
            enqueue_optional_deliveries: true,
            native_cancellation: None,
            unactivated_terminal: None,
            cancellation_actor_id: None,
        };
        self.materialize_turn_event_with_projection_context_and_owner(
            event,
            event_timestamp_secs,
            projection_context,
            execution_owner_id,
        )
        .await
    }

    async fn materialize_turn_event_with_projection_context(
        &self,
        event: TurnEventPayload,
        event_timestamp_secs: i64,
        projection_context: TurnEventProjectionContext,
    ) -> Result<()> {
        self.materialize_turn_event_with_projection_context_and_owner(
            event,
            event_timestamp_secs,
            projection_context,
            None,
        )
        .await
    }

    async fn materialize_turn_event_with_projection_context_and_owner(
        &self,
        event: TurnEventPayload,
        event_timestamp_secs: i64,
        projection_context: TurnEventProjectionContext,
        execution_owner_id: Option<&str>,
    ) -> Result<()> {
        self.materialize_turn_event_with_projection_context_and_guard(
            event,
            event_timestamp_secs,
            projection_context,
            execution_owner_id,
            None,
        )
        .await
    }

    pub(crate) async fn materialize_turn_event_with_projection_context_and_guard(
        &self,
        event: TurnEventPayload,
        event_timestamp_secs: i64,
        projection_context: TurnEventProjectionContext,
        execution_owner_id: Option<&str>,
        operation_guard: Option<sea_orm::Statement>,
    ) -> Result<()> {
        let claim_token = generate_id(DB_ID_LEN);
        let created_at = unix_to_datetime(event_timestamp_secs);
        // Canonical event time may be historical. Start the scheduling lease
        // only after writer admission, independently of that business timestamp.
        let claim_expires_at = || {
            unix_to_datetime(
                chrono::Utc::now()
                    .timestamp()
                    .saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS),
            )
        };
        let execution_owner_id = execution_owner_id.map(str::to_owned);

        self.run_serialized_turn_event_materialization(|| {
            self.materialize_turn_event_with_attempt_deadlines_once(
                event.clone(),
                event_timestamp_secs,
                projection_context.clone(),
                claim_token.clone(),
                created_at,
                &claim_expires_at,
                execution_owner_id.clone(),
                operation_guard.clone(),
            )
        })
        .await
    }

    async fn materialize_turn_event_with_attempt_deadlines_once(
        &self,
        event: TurnEventPayload,
        event_timestamp_secs: i64,
        projection_context: TurnEventProjectionContext,
        claim_token: String,
        created_at: DateTimeWithTimeZone,
        claim_expires_at: &(dyn Fn() -> DateTimeWithTimeZone + Send + Sync),
        execution_owner_id: Option<String>,
        operation_guard: Option<sea_orm::Statement>,
    ) -> Result<()> {
        let Some(appended_event) = self
            .append_claimed_turn_event_projection_once(
                event,
                created_at,
                projection_context.clone(),
                claim_token.clone(),
                claim_expires_at,
                execution_owner_id.as_deref(),
                operation_guard,
            )
            .await?
        else {
            return Ok(());
        };

        if !appended_event.was_inserted {
            return Ok(());
        }

        let projection = retry_with_backoff(
            || {
                self.project_claimed_turn_event_once(
                    appended_event.clone(),
                    projection_context.clone(),
                    claim_token.clone(),
                    created_at,
                )
            },
            is_retryable_database_conflict,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await;
        match projection {
            Ok(TurnEventProjectionOutcome::Projected) => {}
            Ok(TurnEventProjectionOutcome::DeferredByPredecessor) => {
                let pending_record = retry_with_backoff(
                    || {
                        self.mark_turn_event_projection_pending_once(
                            appended_event.id.clone(),
                            appended_event.turn_id.clone(),
                            claim_token.clone(),
                            event_timestamp_secs,
                        )
                    },
                    is_retryable_database_conflict,
                    DEFAULT_LOCK_RETRY_ATTEMPTS,
                    Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
                )
                .await;
                match pending_record {
                    Ok(true) => debug!(
                        event_id = appended_event.id,
                        turn_id = appended_event.turn_id,
                        sequence = appended_event.sequence,
                        "turn event projection deferred until its predecessor is projected"
                    ),
                    Ok(false) => warn!(
                        event_id = appended_event.id,
                        turn_id = appended_event.turn_id,
                        sequence = appended_event.sequence,
                        "deferred turn event projection claim was no longer owned"
                    ),
                    Err(error) => warn!(
                        event_id = appended_event.id,
                        turn_id = appended_event.turn_id,
                        sequence = appended_event.sequence,
                        error = %format!("{error:#}"),
                        "failed to release deferred turn event projection claim; lease expiry will retry it"
                    ),
                }
                return Err(TurnEventProjectionAfterAppendError::pending(
                    appended_event.id,
                    appended_event.turn_id,
                    appended_event.sequence,
                    "authoritative projection is waiting for an earlier turn event".to_owned(),
                )
                .into());
            }
            Err(error) => {
                let error_message = format!("{error:#}");
                if let Err(mark_error) = retry_with_backoff(
                    || {
                        self.mark_turn_event_projection_failure_once(
                            appended_event.id.clone(),
                            appended_event.thread_id.clone(),
                            appended_event.turn_id.clone(),
                            claim_token.clone(),
                            0,
                            error_message.clone(),
                            event_timestamp_secs,
                            false,
                        )
                    },
                    is_retryable_database_conflict,
                    DEFAULT_LOCK_RETRY_ATTEMPTS,
                    Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
                )
                .await
                {
                    return Err(TurnEventProjectionAfterAppendError::new(
                        appended_event.id,
                        appended_event.turn_id,
                        appended_event.sequence,
                        error_message,
                        Some(format!("{mark_error:#}")),
                    )
                    .into());
                }
                return Err(TurnEventProjectionAfterAppendError::new(
                    appended_event.id,
                    appended_event.turn_id,
                    appended_event.sequence,
                    error_message,
                    None,
                )
                .into());
            }
        }

        Ok(())
    }

    async fn run_serialized_turn_event_materialization<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut operation = operation;
        retry_with_backoff(
            || operation(),
            |error| {
                !turn_event_was_appended_before_error(error)
                    && is_retryable_database_conflict(error)
            },
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub(crate) async fn materialize_turn_events_atomically(
        &self,
        events: Vec<TurnEventPayload>,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_events_atomically_with_optional_admission(
            events,
            event_timestamp_secs,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .map(|_| ())
    }
}
