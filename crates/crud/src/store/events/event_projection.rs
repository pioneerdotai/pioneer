use crate::events::TurnEventPayload;
use crate::store::events::event_preparation::{
    PreparedProjectedTurnEvent, TurnEventProjectionContext,
    prepare_turn_event_for_permanent_storage, serialize_turn_event_projection_context,
};
use crate::store::events::projection_receipts;
use crate::{
    AppendedTurnEvent, CrudStore, TurnExecutionStatus, UnactivatedTurnPhaseChanged, compaction,
    finalize_agent_execution, load_agent_execution, load_agent_turn_response, terminal_cleanup,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_conversations::{events, turn_event_delivery};
use pioneer_crud_execution::cli_mcp_completion::cli_mcp_observation_is_superseded;
use pioneer_crud_execution::event_storage_validation::{
    validate_turn_event_durable_owner, validate_turn_event_execution_owner,
};
use pioneer_crud_execution::gateway_control::event_wakes;
use pioneer_crud_execution::native_cancellation_context;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::turn_execution::terminal_turn_execution_status_for_event;
use pioneer_crud_execution::{
    cli_runtime_binding, event_append as turn_event, execution_admission_lease,
    native_terminal_effect_outbox, turn_execution, turn_item_attempt,
};
use pioneer_crud_timeline::timeline_live_projection::project_semantic_timeline_live_turn_event;
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, retry_with_backoff,
};
use pioneer_protocol::{Task, Turn, TurnStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Statement, TransactionTrait,
};
use std::error::Error as StdError;
use std::fmt;
use std::time::Duration;

#[derive(Debug)]
pub struct TurnEventProjectionAfterAppendError {
    event_id: String,
    turn_id: String,
    sequence: i64,
    projection_error: String,
    projection_failure_record_error: Option<String>,
    pending: bool,
}

impl TurnEventProjectionAfterAppendError {
    pub(crate) fn new(
        event_id: String,
        turn_id: String,
        sequence: i64,
        projection_error: String,
        projection_failure_record_error: Option<String>,
    ) -> Self {
        Self {
            event_id,
            turn_id,
            sequence,
            projection_error,
            projection_failure_record_error,
            pending: false,
        }
    }

    pub(super) fn pending(
        event_id: String,
        turn_id: String,
        sequence: i64,
        message: String,
    ) -> Self {
        let mut error = Self::new(event_id, turn_id, sequence, message, None);
        error.pending = true;
        error
    }

    /// A durable append whose authoritative receipt is owned by projection replay.
    /// This never acknowledges completion and never describes a failed projector.
    pub fn is_pending(&self) -> bool {
        self.pending
    }
}

impl fmt::Display for TurnEventProjectionAfterAppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "turn event `{}` for turn `{}` sequence {} was appended but projection failed: {}",
            self.event_id, self.turn_id, self.sequence, self.projection_error
        )?;
        if let Some(mark_error) = self.projection_failure_record_error.as_deref() {
            write!(
                formatter,
                "; failed to persist projection failure state: {mark_error}"
            )?;
        }
        Ok(())
    }
}

impl StdError for TurnEventProjectionAfterAppendError {}

pub fn turn_event_was_appended_before_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.is::<TurnEventProjectionAfterAppendError>())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnEventProjectionOutcome {
    Projected,
    DeferredByPredecessor,
}

#[derive(Debug)]
struct ProjectionPredecessorPending;

impl fmt::Display for ProjectionPredecessorPending {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an earlier turn event is still being projected")
    }
}

impl StdError for ProjectionPredecessorPending {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnEventProjectionFailureOutcome {
    LostClaim,
    Failed,
    Quarantined,
}

pub(crate) const TURN_EVENT_PROJECTION_MAX_ATTEMPTS: i64 = 10;

pub(crate) fn turn_event_projection_retry_delay_secs(attempt_count: i64) -> i64 {
    let exponent = attempt_count.clamp(0, 6) as u32;
    2_i64.saturating_pow(exponent).max(2)
}

impl CrudStore {
    pub(crate) fn append_and_project_turn_event_with_replay_in_transaction<'a>(
        &'a self,
        transaction: &'a DatabaseTransaction,
        prepared: PreparedProjectedTurnEvent,
        created_at: DateTimeWithTimeZone,
        claim_expires_at: DateTimeWithTimeZone,
        enqueue_optional_deliveries: bool,
        repair_compaction_projection: bool,
        allow_deferred_cli_mcp_projection: bool,
    ) -> futures_util::future::BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            let PreparedProjectedTurnEvent {
                event,
                mut projection,
                projection_context_json,
                claim_token,
                terminal_effect_activation,
            } = prepared;
            let timeline_item = projection.timeline_item.take();
            validate_turn_event_durable_owner(transaction, event.payload()).await?;
            if cli_mcp_observation_is_superseded(transaction, event.payload(), None).await?
                && !turn_event::prepared_event_already_exists(transaction, &event).await?
            {
                return Ok(false);
            }
            if terminal_turn_execution_status_for_event(event.payload()).is_some()
                && native_cancellation_context::has_accepted(transaction, event.payload().turn_id())
                    .await?
            {
                anyhow::ensure!(
                    turn_event::prepared_event_already_exists(transaction, &event).await?,
                    "turn event conflicts with accepted native cancellation"
                );
            }

            let appended_event =
                turn_event::append_prepared_event(transaction, event, created_at).await?;
            if !appended_event.was_inserted {
                if pioneer_crud_conversations::event_projection::is_projected(
                    transaction,
                    appended_event.id.as_str(),
                    appended_event.turn_id.as_str(),
                    appended_event.sequence,
                )
                .await?
                {
                    if repair_compaction_projection {
                        // A projected service event is idempotent, but its exact
                        // item may have been deleted/replayed. Restore projections
                        // atomically without appending or delivering it twice.
                        self.turn_projector
                            .project_prepared(transaction, &appended_event, projection)
                            .await?;
                        project_semantic_timeline_live_turn_event(
                            transaction,
                            &appended_event,
                            timeline_item,
                        )
                        .await?;
                    }
                    return Ok(repair_compaction_projection);
                }
                if allow_deferred_cli_mcp_projection {
                    return Ok(false);
                }
                anyhow::bail!(
                    "idempotent turn event `{}` exists but its authoritative projection is incomplete",
                    appended_event.id
                );
            }
            pioneer_crud_conversations::event_projection::insert_claimed(
                transaction,
                pioneer_crud_conversations::event_projection::NewTurnEventProjectionState {
                    event_id: appended_event.id.clone(),
                    thread_id: appended_event.thread_id.clone(),
                    turn_id: appended_event.turn_id.clone(),
                    sequence: appended_event.sequence,
                    projection_context_json,
                    claim_token: claim_token.clone(),
                    claim_expires_at,
                    created_at,
                },
            )
            .await?;
            projection_receipts::accept_terminal(transaction, &appended_event).await?;
            turn_event_delivery::insert_pending_for_event(
                transaction,
                &appended_event,
                created_at,
                enqueue_optional_deliveries,
            )
            .await?;
            if !pioneer_crud_conversations::event_projection::is_next_projection_sequence(
                transaction,
                appended_event.turn_id.as_str(),
                appended_event.sequence,
            )
            .await?
            {
                if allow_deferred_cli_mcp_projection {
                    // The canonical fact and its existing projection claim are
                    // durable. The ordinary replay lane will apply it after its
                    // predecessor; never discard a confirmed MCP result here.
                    return Ok(true);
                }
                anyhow::bail!(
                    "turn event projection `{}` is not the next sequence after the authoritative watermark for turn `{}`",
                    appended_event.id,
                    appended_event.turn_id
                );
            }

            self.turn_projector
                .project_prepared(transaction, &appended_event, projection)
                .await
                .context("failed to project turn event to read models")?;
            if let Some(status) = terminal_turn_execution_status_for_event(&appended_event.payload)
            {
                turn_execution::mark_terminal(
                    transaction,
                    appended_event.turn_id.as_str(),
                    status,
                    created_at,
                )
                .await?;
                let terminal_effect_activation = terminal_effect_activation.with_context(|| {
                format!(
                    "terminal Turn `{}` is missing its prepared native terminal-effect activation",
                    appended_event.turn_id
                )
            })?;
                native_terminal_effect_outbox::activate_prepared_for_terminal(
                    transaction,
                    terminal_effect_activation,
                    created_at,
                )
                .await
                .context("failed to atomically activate native terminal effects")?;
            }
            if matches!(
                &appended_event.payload,
                TurnEventPayload::TurnCompleted(_)
                    | TurnEventPayload::TurnFailed(_)
                    | TurnEventPayload::TurnBlocked(_)
            ) {
                execution_admission_lease::release_by_subject(
                    transaction,
                    "turn",
                    appended_event.turn_id.as_str(),
                    created_at,
                )
                .await?;
            }
            project_semantic_timeline_live_turn_event(transaction, &appended_event, timeline_item)
                .await
                .context("failed to project turn event to semantic timeline")?;

            if !projection_receipts::mark_projected_claimed(
                transaction,
                appended_event.id.as_str(),
                appended_event.turn_id.as_str(),
                appended_event.sequence,
                claim_token.as_str(),
                created_at,
            )
            .await?
            {
                anyhow::bail!(
                    "turn event projection `{}` is no longer claimed by this worker",
                    appended_event.id
                );
            }

            Ok(true)
        })
    }

    pub(crate) async fn append_claimed_turn_event_projection_once(
        &self,
        event: TurnEventPayload,
        created_at: DateTimeWithTimeZone,
        projection_context: TurnEventProjectionContext,
        claim_token: String,
        claim_expires_at: &(dyn Fn() -> DateTimeWithTimeZone + Send + Sync),
        execution_owner_id: Option<&str>,
        operation_guard: Option<sea_orm::Statement>,
    ) -> Result<Option<crate::events::AppendedTurnEvent>> {
        let control_wakes = event_wakes(std::slice::from_ref(&event));
        let cleanup_turns = vec![event.turn_id().to_owned()];
        let cleanup_change = terminal_cleanup::Change::events(&self.connection);
        let changes = ["gateway.control", "gateway.cli", "projection", "compaction"]
            .map(|domain| self.control_work_change(domain, &control_wakes));
        let result = async {
            let _release_wake = (terminal_turn_execution_status_for_event(&event).is_some())
                .then(|| self.execution_heartbeats.release_wake());
            let event = prepare_turn_event_for_permanent_storage(&self.connection, event).await?;
            let projection_context_json =
                serialize_turn_event_projection_context(&projection_context, event.id())?;
            let native_cancellation = projection_context.native_cancellation.clone();
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin turn event append transaction")?;

            pioneer_crud_conversations::turn_event::lock_stream(
                &transaction,
                event.payload().turn_id(),
            )
            .await?;
            validate_turn_event_durable_owner(&transaction, event.payload()).await?;
            validate_turn_event_execution_owner(&transaction, event.payload(), execution_owner_id)
                .await?;
            if cli_mcp_observation_is_superseded(&transaction, event.payload(), None).await?
                && !turn_event::prepared_event_already_exists(&transaction, &event).await?
            {
                transaction.rollback().await?;
                return Ok(None);
            }
            // Fence execution writes and competing terminal results. Independently
            // owned service lifecycle and user edits retain their existing guards.
            if (execution_owner_id.is_some()
                || terminal_turn_execution_status_for_event(event.payload()).is_some())
                && native_cancellation_context::has_accepted(
                    &transaction,
                    event.payload().turn_id(),
                )
                .await?
            {
                anyhow::ensure!(
                    turn_event::prepared_event_already_exists(&transaction, &event).await?,
                    "turn event conflicts with accepted native cancellation"
                );
            }

            if let Some(guard) = operation_guard {
                anyhow::ensure!(
                    transaction.query_one_raw(guard).await?.is_some(),
                    "compaction lifecycle owner or generation changed"
                );
            }

            if let Some(expected) = projection_context.unactivated_terminal.as_ref() {
                let current = turn_execution::find(&transaction, &expected.turn_id)
                    .await?
                    .context("unactivated terminal execution disappeared")?;
                anyhow::ensure!(
                    current.owner_id == expected.owner_id
                        && current.owner_generation == expected.owner_generation
                        && current.thread_id == expected.thread_id
                        && current.workspace_id == expected.workspace_id,
                    UnactivatedTurnPhaseChanged
                );
                if !turn_event::prepared_event_already_exists(&transaction, &event).await? {
                    anyhow::ensure!(
                        matches!(
                            current.status,
                            TurnExecutionStatus::Starting
                                | TurnExecutionStatus::Queued
                                | TurnExecutionStatus::Running
                        ) && (current.started_at.is_none()
                            || current.status == TurnExecutionStatus::Starting)
                            && !pioneer_crud_execution::native_cancellation_context::is_registered(
                                &transaction,
                                &expected.turn_id
                            )
                            .await?
                            && cli_runtime_binding::latest_turn_attempt(
                                &transaction,
                                &expected.turn_id
                            )
                            .await?
                            .is_none()
                            && !pioneer_crud_conversations::projection_receipts::has_accepted_terminal(
                                &transaction,
                                &expected.turn_id
                            )
                            .await?,
                        UnactivatedTurnPhaseChanged
                    );
                }
            }

            let cancellation_turn_id = if let Some(cancellation) = native_cancellation {
                if !turn_event::prepared_event_already_exists(&transaction, &event).await? {
                    native_cancellation_context::revalidate(
                        &transaction,
                        cancellation.revalidation,
                    )
                    .await?;
                    anyhow::ensure!(
                        !pioneer_crud_conversations::projection_receipts::has_accepted_terminal(
                            &transaction,
                            event.payload().turn_id()
                        )
                        .await?,
                        "native cancellation conflicts with accepted terminal result"
                    );
                    native_terminal_effect_outbox::prepare(
                        &transaction,
                        cancellation.effects,
                        created_at,
                    )
                    .await?;
                    Some(event.payload().turn_id().to_owned())
                } else {
                    None
                }
            } else {
                None
            };

            let appended_event =
                match turn_event::append_prepared_event(&transaction, event, created_at).await {
                    Ok(event) => event,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                };

            if let Some(turn_id) = cancellation_turn_id {
                native_cancellation_context::mark_accepted(
                    &transaction,
                    &turn_id,
                    &appended_event.id,
                )
                .await?;
            }

            if !appended_event.was_inserted {
                let projected = pioneer_crud_conversations::event_projection::is_projected(
                    &transaction,
                    appended_event.id.as_str(),
                    appended_event.turn_id.as_str(),
                    appended_event.sequence,
                )
                .await?;
                transaction
                    .rollback()
                    .await
                    .context("failed to rollback idempotent turn event lookup")?;
                if projected || projection_context.native_cancellation.is_some() {
                    return Ok(Some(appended_event));
                }
                // Another producer may have committed this exact canonical event
                // and still be projecting it. Its durable append is just as
                // authoritative as our own: callers must not roll back local
                // terminal state while the original projection is in flight.
                return Err(TurnEventProjectionAfterAppendError::pending(
                    appended_event.id,
                    appended_event.turn_id,
                    appended_event.sequence,
                    "idempotent canonical event is waiting for its authoritative projection"
                        .to_owned(),
                )
                .into());
            }

            if let Some(actor) = projection_context.cancellation_actor_id.as_deref()
                && let TurnEventPayload::TurnFailed(n) = &appended_event.payload
                && n.turn.status == pioneer_protocol::TurnStatus::Interrupted
            {
                pioneer_crud_tasks::task_cancellation_actor::record_turn(
                    &transaction,
                    actor,
                    &n.workspace_id,
                    &n.thread_id,
                    &n.turn.id,
                )
                .await?;
            }

            if let Err(error) = pioneer_crud_conversations::event_projection::insert_claimed(
                &transaction,
                pioneer_crud_conversations::event_projection::NewTurnEventProjectionState {
                    event_id: appended_event.id.clone(),
                    thread_id: appended_event.thread_id.clone(),
                    turn_id: appended_event.turn_id.clone(),
                    sequence: appended_event.sequence,
                    projection_context_json,
                    claim_token,
                    claim_expires_at: claim_expires_at(),
                    created_at,
                },
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            projection_receipts::accept_terminal(&transaction, &appended_event)
                .await?;
            if let Some(expected) = projection_context.unactivated_terminal.as_ref() {
                let status = terminal_turn_execution_status_for_event(&appended_event.payload)
                    .context("unactivated terminal event has no terminal execution status")?;
                let graph_status = match status {
                    TurnExecutionStatus::Interrupted => "cancelled",
                    TurnExecutionStatus::Blocked => "blocked",
                    TurnExecutionStatus::Failed => "failed",
                    _ => bail!("unsupported unactivated terminal event"),
                };
                turn_execution::mark_terminal(&transaction, &expected.turn_id, status, created_at)
                    .await?;
                if let Some(response) = pioneer_crud_execution::execution_reads::load_agent_turn_response(
                    &transaction,
                    &expected.turn_id,
                )
                .await?
                {
                    let execution = pioneer_crud_execution::agent_execution::load_agent_execution(
                        &transaction,
                        &response.execution_id,
                    )
                    .await?
                    .context("unactivated cancellation response execution disappeared")?;
                    // True Task occurrences retain their own candidate/run cleanup.
                    if execution.parent_task_id.is_none() {
                        pioneer_crud_tasks::terminal::finalize_agent_execution(
                            &transaction,
                            &response.execution_id,
                            graph_status,
                            created_at,
                        )
                        .await?;
                    }
                }
            }

            if let Err(error) = turn_event_delivery::insert_pending_for_event(
                &transaction,
                &appended_event,
                created_at,
                projection_context.enqueue_optional_deliveries,
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            transaction
                .commit()
                .await
                .context("failed to commit turn event append transaction")?;

            Ok(Some(appended_event))
        }
        .await;
        if let Ok(appended) = &result {
            let changed = appended.as_ref().is_some_and(|event| event.was_inserted);
            cleanup_change
                .complete(&self.connection, changed, &cleanup_turns)
                .await;
            for change in changes {
                change.completed(changed);
            }
        }
        result
    }

    pub(crate) async fn project_claimed_turn_event(
        &self,
        appended_event: crate::events::AppendedTurnEvent,
        projection_context: TurnEventProjectionContext,
        claim_token: String,
        projected_at: DateTimeWithTimeZone,
    ) -> Result<TurnEventProjectionOutcome> {
        // A predecessor can still be projecting or publish a new attempt after
        // reader preparation. Give an accepted native cancellation time to
        // complete both operations while retaining its original claim. A longer
        // predecessor backlog still goes through ordered recovery.
        let retry_pending_cancellation = projection_context.native_cancellation.is_some();
        retry_with_backoff(
            || async {
                let outcome = self
                    .project_claimed_turn_event_once(
                        appended_event.clone(),
                        projection_context.clone(),
                        claim_token.clone(),
                        projected_at,
                    )
                    .await?;
                if retry_pending_cancellation
                    && outcome == TurnEventProjectionOutcome::DeferredByPredecessor
                {
                    return Err(ProjectionPredecessorPending.into());
                }
                Ok(outcome)
            },
            |error: &anyhow::Error| {
                error.is::<turn_item_attempt::TerminalRunningAttemptsChanged>()
                    || error.is::<ProjectionPredecessorPending>()
                    || pioneer_crud_tasks::retry::is_retryable(error)
                    || pioneer_crud_artifacts::html_preview_io::is_conflict(error)
            },
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
        .or_else(|error| {
            if error.is::<ProjectionPredecessorPending>() {
                Ok(TurnEventProjectionOutcome::DeferredByPredecessor)
            } else {
                Err(error)
            }
        })
    }

    async fn project_claimed_turn_event_once(
        &self,
        appended_event: crate::events::AppendedTurnEvent,
        projection_context: TurnEventProjectionContext,
        claim_token: String,
        projected_at: DateTimeWithTimeZone,
    ) -> Result<TurnEventProjectionOutcome> {
        let control_wakes = event_wakes(std::slice::from_ref(&appended_event.payload));
        let cleanup_turns = vec![appended_event.turn_id.clone()];
        let html_turns = terminal_cleanup::html_lifetime_turn(&appended_event.payload)
            .map(str::to_owned)
            .into_iter()
            .collect::<Vec<_>>();
        let html_change = (!html_turns.is_empty())
            .then(|| self.connection.work_signal("maintenance.html").changing());
        let mut html_changed = false;
        let cleanup_change = terminal_cleanup::Change::events(&self.connection);
        let changes = ["gateway.control", "gateway.cli", "projection", "compaction"]
            .map(|domain| self.control_work_change(domain, &control_wakes));
        let result = async {
            let task_change = Some(
                pioneer_crud_tasks::work_change::TaskTurnChange::begin(
                    &self.connection,
                    vec![appended_event.turn_id.clone()],
                )
                .await?,
            );
            let result = async {
        let _release_wake = (terminal_turn_execution_status_for_event(&appended_event.payload)
            .is_some())
        .then(|| self.execution_heartbeats.release_wake());
        let mut projection = self
            .turn_projector
            .prepare(
                &self.connection,
                appended_event.id.as_str(),
                &appended_event.payload,
                appended_event.created_at,
            )
            .await?;
        let terminal_effect_activation =
            if terminal_turn_execution_status_for_event(&appended_event.payload).is_some() {
                Some(
                    native_terminal_effect_outbox::prepare_activation_for_terminal(
                        &self.connection,
                        appended_event.turn_id.as_str(),
                    )
                    .await?,
                )
            } else {
                None
            };
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin turn event projection transaction")?;

        let html_before = terminal_cleanup::html_turn_lifetimes(&transaction, &html_turns).await?;

        let is_next_projection_sequence =
            match pioneer_crud_conversations::event_projection::is_next_projection_sequence(
                &transaction,
                appended_event.turn_id.as_str(),
                appended_event.sequence,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };
        if !is_next_projection_sequence {
            let _ = transaction.rollback().await;
            return Ok(TurnEventProjectionOutcome::DeferredByPredecessor);
        }

        let timeline_item = projection.timeline_item.take();
        let superseded_observation = cli_mcp_observation_is_superseded(
            &transaction,
            &appended_event.payload,
            Some(appended_event.sequence),
        )
        .await?;

        if !superseded_observation
            && let Err(error) = self
                .turn_projector
                .project_prepared(&transaction, &appended_event, projection)
                .await
                .context("failed to project turn event to read models")
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }
        if let Some(status) = terminal_turn_execution_status_for_event(&appended_event.payload) {
            if let Err(error) = turn_execution::mark_terminal(
                &transaction,
                appended_event.turn_id.as_str(),
                status,
                projected_at,
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            let terminal_effect_activation = terminal_effect_activation.with_context(|| {
                format!(
                    "terminal Turn `{}` is missing its prepared native terminal-effect activation",
                    appended_event.turn_id
                )
            })?;
            if let Err(error) = native_terminal_effect_outbox::activate_prepared_for_terminal(
                &transaction,
                terminal_effect_activation,
                projected_at,
            )
            .await
            .context("failed to atomically activate native terminal effects")
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }
        if matches!(
            &appended_event.payload,
            TurnEventPayload::TurnCompleted(_)
                | TurnEventPayload::TurnFailed(_)
                | TurnEventPayload::TurnBlocked(_)
        ) && let Err(error) = execution_admission_lease::release_by_subject(
            &transaction,
            "turn",
            appended_event.turn_id.as_str(),
            projected_at,
        )
        .await
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }

        if !superseded_observation
            && let (TurnEventPayload::ItemStarted(notification), Some(deadlines)) = (
                &appended_event.payload,
                projection_context.item_started_deadlines,
            )
        {
            let configured = turn_item_attempt::configure_running_attempt_deadlines(
                &transaction,
                notification.turn_id.as_str(),
                notification.item.item_id(),
                appended_event.created_at,
                deadlines.lease_expires_at_unix.map(unix_to_datetime),
                deadlines.idle_deadline_at_unix.map(unix_to_datetime),
                deadlines.hard_deadline_at_unix.map(unix_to_datetime),
            )
            .await
            .context("failed to configure item attempt deadlines during item/started projection")?;
            if !configured {
                let _ = transaction.rollback().await;
                anyhow::bail!(
                    "item/started projection did not create a running attempt for item `{}`",
                    notification.item.item_id()
                );
            }
        }

        if !superseded_observation
            && let Err(error) = project_semantic_timeline_live_turn_event(&*transaction, &appended_event, timeline_item)
                .await
                .context("failed to project turn event to semantic timeline")
        {
            return Err(crate::store::connection::rollback_preserving_error(
                transaction,
                error,
            )
            .await);
        }

        let projected = match projection_receipts::mark_projected_claimed(
            &transaction,
            appended_event.id.as_str(),
            appended_event.turn_id.as_str(),
            appended_event.sequence,
            claim_token.as_str(),
            projected_at,
        )
        .await
        {
            Ok(projected) => projected,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        if !projected {
            let _ = transaction.rollback().await;
            anyhow::bail!(
                "turn event projection `{}` is no longer claimed by this worker",
                appended_event.id
            );
        }

        html_changed = html_before != terminal_cleanup::html_turn_lifetimes(&transaction, &html_turns).await?;

        transaction
            .commit()
            .await
            .context("failed to commit turn event projection transaction")?;

        Ok(TurnEventProjectionOutcome::Projected)
        }.await;
            if result.is_ok() {
                if let Some(change) = task_change {
                    change.completed(&self.connection).await?;
                }
            }
            result
        }
        .await;
        if let Ok(outcome) = &result {
            let changed = matches!(outcome, TurnEventProjectionOutcome::Projected);
            cleanup_change
                .complete(&self.connection, changed, &cleanup_turns)
                .await;
            if let Some(change) = html_change {
                change.completed(html_changed);
            }
            for change in changes {
                change.completed(changed);
            }
        }
        result
    }

    pub(crate) async fn mark_turn_event_projection_pending_once(
        &self,
        event_id: String,
        turn_id: String,
        claim_token: String,
        deferred_at_unix: i64,
    ) -> Result<bool> {
        let projection_changes = [
            self.connection
                .work_signal("projection")
                .changing()
                .with_keys(&[format!("turn:{turn_id}")]),
            self.connection.work_signal("compaction").changing(),
        ];
        let projection_result = async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin deferred turn event projection transaction")?;
            let deferred_at = unix_to_datetime(deferred_at_unix);
            let next_run_at = unix_to_datetime(
                deferred_at_unix.saturating_add(turn_event_projection_retry_delay_secs(0)),
            );
            let released =
                match pioneer_crud_conversations::event_projection::release_claim_as_pending(
                    &transaction,
                    event_id.as_str(),
                    claim_token.as_str(),
                    next_run_at,
                    deferred_at,
                )
                .await
                {
                    Ok(released) => released,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                };

            transaction
                .commit()
                .await
                .context("failed to commit deferred turn event projection transaction")?;

            Ok(released)
        }
        .await;
        if let Ok(value) = &projection_result {
            let changed = *value;
            for change in projection_changes {
                change.completed(changed);
            }
        }
        projection_result
    }

    pub(crate) async fn mark_turn_event_projection_failure_once(
        &self,
        event_id: String,
        thread_id: String,
        turn_id: String,
        claim_token: String,
        attempt_count: i64,
        error_message: String,
        failed_at_unix: i64,
        force_exhausted: bool,
    ) -> Result<TurnEventProjectionFailureOutcome> {
        let projection_changes = [
            self.connection
                .work_signal("projection")
                .changing()
                .with_keys(&[format!("turn:{turn_id}")]),
            self.connection.work_signal("compaction").changing(),
        ];
        let projection_result =
            async {
                let task_change = pioneer_crud_tasks::work_change::TaskTurnChange::begin(
                    &self.connection,
                    vec![turn_id.clone()],
                )
                .await?;
                let task_result =
                    async {
                        let transaction =
                            self.connection.begin().await.context(
                                "failed to begin turn event projection failure transaction",
                            )?;

                        let failed_at = unix_to_datetime(failed_at_unix);
                        let exhausted = force_exhausted
                            || attempt_count.saturating_add(1)
                                >= TURN_EVENT_PROJECTION_MAX_ATTEMPTS;
                        let marked = if exhausted {
                            pioneer_crud_conversations::event_projection::mark_exhausted_claimed(
                                &transaction,
                                event_id.as_str(),
                                claim_token.as_str(),
                                error_message.clone(),
                                failed_at,
                            )
                            .await
                        } else {
                            let next_run_at = unix_to_datetime(failed_at_unix.saturating_add(
                                turn_event_projection_retry_delay_secs(attempt_count),
                            ));
                            pioneer_crud_conversations::event_projection::mark_failed_claimed(
                                &transaction,
                                event_id.as_str(),
                                claim_token.as_str(),
                                error_message.clone(),
                                next_run_at,
                                failed_at,
                            )
                            .await
                        };

                        let marked = match marked {
                            Ok(marked) => marked,
                            Err(error) => {
                                let _ = transaction.rollback().await;
                                return Err(error);
                            }
                        };

                        if !marked {
                            transaction.commit().await.context(
                                "failed to commit lost turn event projection claim transaction",
                            )?;
                            return Ok(TurnEventProjectionFailureOutcome::LostClaim);
                        }

                        if exhausted
                            && let Err(error) = projection_receipts::quarantine(
                                &transaction,
                                thread_id.as_str(),
                                turn_id.as_str(),
                                event_id.as_str(),
                                error_message,
                                failed_at,
                            )
                            .await
                        {
                            let _ = transaction.rollback().await;
                            return Err(error);
                        }

                        transaction.commit().await.context(
                            "failed to commit turn event projection failure transaction",
                        )?;

                        Ok(if exhausted {
                            TurnEventProjectionFailureOutcome::Quarantined
                        } else {
                            TurnEventProjectionFailureOutcome::Failed
                        })
                    }
                    .await;
                if task_result.is_ok() {
                    task_change.completed(&self.connection).await?;
                }
                task_result
            }
            .await;
        if let Ok(value) = &projection_result {
            let changed = !matches!(value, TurnEventProjectionFailureOutcome::LostClaim);
            for change in projection_changes {
                change.completed(changed);
            }
        }
        projection_result
    }
}
