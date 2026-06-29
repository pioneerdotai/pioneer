use anyhow::{Context, Result};
use pioneer_protocol::{ThreadStatus, TurnItemAttemptStatus};
use sea_orm::ConnectionTrait;
use std::future::Future;
use std::pin::Pin;

use crate::convention::{TURN_ITEM_STATUS_IN_PROGRESS, turn_status_to_db};
use crate::events::{AppendedTurnEvent, TurnEventPayload, TurnStartedEventPayload};
use crate::repositories::{policy, thread, turn, turn_item_attempt};
use crate::turn_item_terminal::{
    TurnItemTerminalState, attempt_status_from_payload, terminal_turn_item_status_from_payload,
    terminalize_turn_item_payload,
};
use crate::util::unix_to_datetime;

type ProjectFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

// Keep each event branch out of the outer projector future; projection payloads are large.
fn project_future<'a, F>(future: F) -> ProjectFuture<'a>
where
    F: Future<Output = Result<()>> + Send + 'a,
{
    Box::pin(future)
}

#[derive(Clone, Default)]
pub struct TurnProjector;

impl TurnProjector {
    pub fn new() -> Self {
        Self
    }

    pub async fn project<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        event: &AppendedTurnEvent,
    ) -> Result<()> {
        let created_at = event.created_at;
        let future: ProjectFuture<'_> = match &event.payload {
            TurnEventPayload::TurnStarted(payload) => {
                project_future(self.project_turn_started(db, payload))
            }
            TurnEventPayload::ItemStarted(payload) => project_future(async move {
                turn::upsert_turn_item(
                    db,
                    payload.turn_id.as_str(),
                    &payload.item,
                    Some(TURN_ITEM_STATUS_IN_PROGRESS),
                    created_at,
                    created_at,
                )
                .await?;

                let payload_json = serde_json::to_string(&payload.item)
                    .context("failed to serialize item payload for attempt seed")?;

                turn_item_attempt::create_running_attempt(
                    db,
                    payload.turn_id.as_str(),
                    payload.item.item_id(),
                    payload.item.item_type(),
                    payload_json,
                    turn_item_attempt::AttemptDeadlines {
                        lease_expires_at: None,
                        idle_deadline_at: None,
                        hard_deadline_at: None,
                    },
                    created_at,
                )
                .await?;

                Ok(())
            }),
            TurnEventPayload::ItemCompleted(payload) => project_future(async move {
                let turn_item_status = terminal_turn_item_status_from_payload(&payload.item);

                turn::upsert_turn_item(
                    db,
                    payload.turn_id.as_str(),
                    &payload.item,
                    Some(turn_item_status),
                    created_at,
                    created_at,
                )
                .await?;

                let attempt_status = attempt_status_from_payload(&payload.item);

                let failure_reason = (attempt_status == TurnItemAttemptStatus::Failed).then(|| {
                    format!(
                        "item `{}` completed with failed status",
                        payload.item.item_id()
                    )
                });

                let _ = turn_item_attempt::finish_running_attempt(
                    db,
                    payload.turn_id.as_str(),
                    payload.item.item_id(),
                    attempt_status,
                    failure_reason,
                    created_at,
                )
                .await?;

                Ok(())
            }),
            TurnEventPayload::ItemUpdated(payload) => project_future(async move {
                let turn_item_status = terminal_turn_item_status_from_payload(&payload.item);

                turn::upsert_turn_item(
                    db,
                    payload.turn_id.as_str(),
                    &payload.item,
                    Some(turn_item_status),
                    created_at,
                    created_at,
                )
                .await
            }),
            TurnEventPayload::ItemTimeoutDetected(_)
            | TurnEventPayload::ItemRecoveryOpened(_)
            | TurnEventPayload::ItemRecoveryAttached(_)
            | TurnEventPayload::ItemRetryScheduled(_)
            | TurnEventPayload::ItemRetryAttemptStarted(_)
            | TurnEventPayload::ItemRecoverySucceeded(_)
            | TurnEventPayload::ItemRecoveryExhausted(_)
            | TurnEventPayload::ItemToolRetryScheduled(_)
            | TurnEventPayload::ItemToolRetryResolved(_)
            | TurnEventPayload::ItemToolRetryExhausted(_)
            | TurnEventPayload::TurnToolLoopBudgetExceeded(_)
            | TurnEventPayload::TurnExecutionWindowStarted(_)
            | TurnEventPayload::TurnExecutionWindowExhausted(_)
            | TurnEventPayload::TurnExecutionWindowCheckpointed(_)
            | TurnEventPayload::TurnExecutionWindowContinued(_)
            | TurnEventPayload::TurnExecutionWindowBlocked(_)
            | TurnEventPayload::TurnPermissionAudit(_) => project_future(async { Ok(()) }),
            TurnEventPayload::TurnCompleted(payload) => project_future(async move {
                self.close_running_attempts_for_terminal_turn(
                    db,
                    payload.turn.id.as_str(),
                    created_at,
                )
                .await?;
                self.project_turn_finished(
                    db,
                    payload.thread_id.as_str(),
                    &payload.turn,
                    created_at,
                )
                .await
            }),
            TurnEventPayload::TurnFailed(payload) => project_future(async move {
                self.close_running_attempts_for_terminal_turn(
                    db,
                    payload.turn.id.as_str(),
                    created_at,
                )
                .await?;
                self.project_turn_finished(
                    db,
                    payload.thread_id.as_str(),
                    &payload.turn,
                    created_at,
                )
                .await
            }),
            TurnEventPayload::TurnBlocked(payload) => project_future(async move {
                self.close_running_attempts_for_terminal_turn(
                    db,
                    payload.turn.id.as_str(),
                    created_at,
                )
                .await?;
                self.project_turn_finished(
                    db,
                    payload.thread_id.as_str(),
                    &payload.turn,
                    created_at,
                )
                .await
            }),
        };
        future.await
    }

    async fn project_turn_started<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
    ) -> Result<()> {
        let thread_created_at = unix_to_datetime(payload.thread.created_at);
        let thread_updated_at = unix_to_datetime(payload.thread.updated_at);

        thread::upsert_thread(db, &payload.thread, thread_created_at, thread_updated_at).await?;
        policy::upsert_thread_sandbox_policy(
            db,
            payload.thread.id.as_str(),
            payload.sandbox_mode,
            thread_created_at,
            thread_updated_at,
        )
        .await?;

        let existing_turn = turn::find_turn_by_id(db, payload.turn.id.as_str()).await?;
        let turn_status = turn_status_to_db(payload.turn.status).to_owned();
        let turn_error = payload.turn.error.clone();

        turn::upsert_turn(
            db,
            payload.turn.id.as_str(),
            payload.thread.id.as_str(),
            &payload.turn,
            None,
            payload.reasoning_effort.as_deref(),
            thread_updated_at,
            thread_updated_at,
        )
        .await?;

        turn::replace_turn_input(
            db,
            payload.turn.id.as_str(),
            payload.input.as_slice(),
            thread_updated_at,
        )
        .await?;

        let should_append_status = existing_turn
            .map(|existing| (existing.status, existing.error))
            .is_none_or(|existing| existing != (turn_status.clone(), turn_error.clone()));

        if should_append_status {
            turn::append_turn_status_history(
                db,
                payload.turn.id.as_str(),
                payload.turn.status,
                turn_error,
                thread_updated_at,
            )
            .await?;
        }

        Ok(())
    }

    async fn project_turn_finished<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        thread_id: &str,
        turn_model: &pioneer_protocol::Turn,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<()> {
        let existing_turn = turn::find_turn_by_id(db, turn_model.id.as_str()).await?;
        let turn_status = turn_status_to_db(turn_model.status).to_owned();
        let turn_error = turn_model.error.clone();

        turn::upsert_turn(
            db,
            turn_model.id.as_str(),
            thread_id,
            turn_model,
            None,
            None,
            updated_at,
            updated_at,
        )
        .await?;

        let should_append_status = existing_turn
            .map(|existing| (existing.status, existing.error))
            .is_none_or(|existing| existing != (turn_status.clone(), turn_error.clone()));

        if should_append_status {
            turn::append_turn_status_history(
                db,
                turn_model.id.as_str(),
                turn_model.status,
                turn_error,
                updated_at,
            )
            .await?;
        }

        thread::update_thread_status(db, thread_id, ThreadStatus::Idle, updated_at).await?;

        Ok(())
    }

    async fn close_running_attempts_for_terminal_turn<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        turn_id: &str,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<()> {
        let running_attempts =
            turn_item_attempt::list_running_attempts_for_turn(db, turn_id).await?;
        for attempt in running_attempts {
            let _ = turn_item_attempt::finish_running_attempt(
                db,
                turn_id,
                attempt.item_id.as_str(),
                TurnItemAttemptStatus::Interrupted,
                Some("turn_terminal_before_item_completed".to_owned()),
                updated_at,
            )
            .await?;

            let Some(item_row) =
                turn::find_turn_item(db, turn_id, attempt.item_id.as_str()).await?
            else {
                continue;
            };
            let mut item = serde_json::from_str::<pioneer_protocol::TurnItem>(item_row.payload.as_str())
                .with_context(|| {
                    format!(
                        "failed to decode running turn_item payload for terminal turn `{turn_id}` item `{}`",
                        attempt.item_id
                    )
                })?;
            let state = TurnItemTerminalState::Failed {
                reason: Some("turn_terminal_before_item_completed".to_owned()),
            };
            terminalize_turn_item_payload(&mut item, state.clone());

            turn::upsert_turn_item(
                db,
                turn_id,
                &item,
                Some(state.to_turn_item_status()),
                item_row.created_at,
                updated_at,
            )
            .await?;
        }
        Ok(())
    }
}
