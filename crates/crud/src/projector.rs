use anyhow::{Context, Result};
use pioneer_protocol::{
    PersistedActorRef, ThreadMode, ThreadStatus, TurnItemAttemptStatus, TurnKind,
    TurnMessageDeletedEvent, TurnMessageEditedEvent, TurnMessageRevisionChangeKind, TurnOrigin,
    TurnStatus, UserInput,
};
use sea_orm::ConnectionTrait;
use std::future::Future;
use std::pin::Pin;

use crate::convention::{TURN_ITEM_STATUS_IN_PROGRESS, turn_item_type_to_db, turn_status_to_db};
use crate::events::{AppendedTurnEvent, TurnEventPayload, TurnStartedEventPayload};
use crate::repositories::{
    identity, policy, self_improvement_source_turn, thread, turn, turn_item_attempt, turn_liveness,
};
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

fn message_thread_preview(input: &[UserInput]) -> String {
    input
        .iter()
        .find_map(|input| match input {
            UserInput::Text { text, .. } => Some(text.trim()),
            _ => None,
        })
        .filter(|text| !text.is_empty())
        .unwrap_or_default()
        .to_owned()
}

enum TurnMessageMutationProjection<'a> {
    Edit(&'a TurnMessageEditedEvent),
    Delete(&'a TurnMessageDeletedEvent),
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
        if let Some(observation) = liveness_observation_from_event(event) {
            turn_liveness::observe_activity(db, observation).await?;
        }
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
                    Some(event.sequence),
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

                self_improvement_source_turn::project_completed_collaborative_source_exchange(
                    db,
                    event.id.as_str(),
                    created_at,
                    payload,
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
            TurnEventPayload::TurnMessageEdited(payload) => {
                project_future(self.project_turn_message_mutation(
                    db,
                    TurnMessageMutationProjection::Edit(payload),
                    created_at,
                ))
            }
            TurnEventPayload::TurnMessageDeleted(payload) => {
                project_future(self.project_turn_message_mutation(
                    db,
                    TurnMessageMutationProjection::Delete(payload),
                    created_at,
                ))
            }
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
                .await?;
                self_improvement_source_turn::project_completed_source_turn(
                    db,
                    event.id.as_str(),
                    created_at,
                    payload,
                )
                .await?;
                Ok(())
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

    async fn project_turn_message_mutation<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        mutation: TurnMessageMutationProjection<'_>,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<()> {
        let (thread_id, event_turn, projected_input, changed_by, change_kind, deleted_by) =
            match mutation {
                TurnMessageMutationProjection::Edit(payload) => (
                    payload.thread_id.as_str(),
                    &payload.turn,
                    payload.input.as_slice(),
                    &payload.changed_by,
                    TurnMessageRevisionChangeKind::Edit,
                    None,
                ),
                TurnMessageMutationProjection::Delete(payload) => (
                    payload.thread_id.as_str(),
                    &payload.turn,
                    &[] as &[UserInput],
                    &payload.deleted_by,
                    TurnMessageRevisionChangeKind::Delete,
                    Some(&payload.deleted_by),
                ),
            };
        let is_delete = deleted_by.is_some();
        if event_turn.mode != ThreadMode::Message
            || event_turn.status != TurnStatus::Completed
            || event_turn.turn_kind != TurnKind::Conversation
            || event_turn.origin != TurnOrigin::User
            || event_turn.message_revision == 0
            || event_turn.message_deleted != is_delete
            || (is_delete && (!projected_input.is_empty() || !event_turn.mentions.is_empty()))
        {
            anyhow::bail!("invalid post-terminal Turn message mutation event");
        }

        let (turn_model, collaboration) =
            turn::find_turn_collaboration(db, thread_id, event_turn.id.as_str())
                .await?
                .context("message mutation event targets a missing Turn")?;

        if !collaboration.mode.message_mutation_eligible
            || turn_model.status != "completed"
            || turn_model.turn_kind != "conversation"
            || turn_model.origin != "user"
        {
            anyhow::bail!("message mutation event targets an immutable Turn");
        }

        let current_revision = collaboration.message_revision;
        if current_revision > event_turn.message_revision {
            return Ok(());
        }
        if current_revision < event_turn.message_revision
            && current_revision.checked_add(1) != Some(event_turn.message_revision)
        {
            anyhow::bail!("Turn message mutation replay has a revision gap");
        }

        let current_input = turn::find_turn_inputs(db, event_turn.id.as_str())
            .await?
            .into_iter()
            .map(|row| {
                serde_json::from_str::<UserInput>(row.payload.as_str())
                    .context("failed to decode current Turn input during mutation replay")
            })
            .collect::<Result<Vec<_>>>()?;
        let previous_input = if current_revision < event_turn.message_revision {
            current_input.clone()
        } else {
            let previous_revision = turn::list_turn_message_revisions(
                db,
                event_turn.id.as_str(),
                Some(event_turn.message_revision),
                1,
            )
            .await?
            .into_iter()
            .next()
            .map(turn::turn_message_revision_from_model)
            .transpose()?
            .filter(|revision| {
                revision.revision.checked_add(1) == Some(event_turn.message_revision)
            })
            .context("message mutation replay is missing its previous revision")?;
            previous_revision
                .input
                .context("message mutation replay previous revision has no input")?
        };
        let previous_preview = message_thread_preview(previous_input.as_slice());
        let replacement_preview = if is_delete {
            String::new()
        } else {
            message_thread_preview(projected_input)
        };
        thread::replace_thread_preview_if_matches(
            db,
            thread_id,
            previous_preview.as_str(),
            replacement_preview.as_str(),
            updated_at,
        )
        .await?;
        if current_revision == event_turn.message_revision
            && collaboration.mentions == event_turn.mentions
            && collaboration.message_deleted == event_turn.message_deleted
            && current_input == projected_input
        {
            return Ok(());
        }

        if current_revision < event_turn.message_revision {
            turn::insert_turn_message_revision_if_absent(
                db,
                turn::NewTurnMessageRevision {
                    turn_id: event_turn.id.as_str(),
                    revision: current_revision,
                    input: current_input.as_slice(),
                    mentions: collaboration.mentions.as_slice(),
                    changed_by,
                    change_kind,
                    created_at: updated_at,
                },
            )
            .await?;
        }
        if !turn::project_message_turn_mutation_state(
            db,
            turn_model.thread_id.as_str(),
            event_turn.id.as_str(),
            event_turn.message_revision,
            event_turn.mentions.as_slice(),
            deleted_by,
            updated_at,
        )
        .await?
        {
            anyhow::bail!("Turn message mutation replay lost its target");
        }
        turn::replace_turn_input(db, event_turn.id.as_str(), projected_input, updated_at).await
    }

    async fn project_turn_started<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
    ) -> Result<()> {
        match payload.actor.as_ref() {
            Some(actor) => {
                self.project_attributed_turn_started(db, payload, actor)
                    .await
            }
            None => self.project_legacy_turn_started(db, payload).await,
        }
    }

    async fn project_attributed_turn_started<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
        actor: &PersistedActorRef,
    ) -> Result<()> {
        self.project_turn_started_inner(db, payload, Some(actor))
            .await
    }

    /// Compatibility seam for append-only turn/start events written before actor attribution.
    /// Normal materialization rejects this shape before append.
    async fn project_legacy_turn_started<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
    ) -> Result<()> {
        self.project_turn_started_inner(db, payload, None).await?;
        if let Some(superuser_id) = legacy_backfill_superuser_id(db).await? {
            identity::backfill_legacy_actor_references(db, &superuser_id).await?;
        }
        Ok(())
    }

    async fn project_turn_started_inner<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
        actor: Option<&PersistedActorRef>,
    ) -> Result<()> {
        let thread_created_at = unix_to_datetime(payload.thread.created_at);
        let thread_updated_at = unix_to_datetime(payload.thread.updated_at);
        let is_task_run_occurrence = payload.turn.turn_kind == TurnKind::TaskRun;

        if is_task_run_occurrence {
            // A task-run occurrence is only a parent-timeline projection. The
            // parent thread and its foreground policy already exist, and a
            // stale task payload must not overwrite metadata or foreground
            // state written by a concurrent conversation turn.
            if thread::find_thread_by_id(db, payload.thread.id.as_str())
                .await?
                .is_none()
            {
                let mut parent = payload.thread.clone();
                parent.status = ThreadStatus::Idle;
                match actor {
                    Some(actor) => {
                        thread::upsert_thread_with_creator(
                            db,
                            &parent,
                            actor,
                            thread_created_at,
                            thread_updated_at,
                        )
                        .await?;
                    }
                    None => {
                        thread::upsert_thread(db, &parent, thread_created_at, thread_updated_at)
                            .await?;
                    }
                }
                policy::upsert_thread_sandbox_policy(
                    db,
                    parent.id.as_str(),
                    payload.sandbox_mode,
                    thread_created_at,
                    thread_updated_at,
                )
                .await?;
            }
        } else if payload.turn.mode == ThreadMode::Message
            && thread::find_thread_by_id(db, payload.thread.id.as_str())
                .await?
                .is_some()
        {
            // Message reuses the canonical TurnStarted envelope, but it must
            // not replay the envelope's full Thread snapshot over concurrent
            // execution or management changes. New threads still take the
            // normal insertion path below.
            let derived_preview = message_thread_preview(payload.input.as_slice());
            thread::touch_thread_for_completed_message(
                db,
                payload.thread.id.as_str(),
                derived_preview.as_str(),
                thread_updated_at,
            )
            .await?;
            if policy::find_thread_sandbox_mode(db, payload.thread.id.as_str())
                .await?
                .is_none()
            {
                // Atomic first-Message materialization inserts the user thread
                // before this event is projected. Seed its existing policy row
                // without replacing a policy changed by another operation.
                policy::upsert_thread_sandbox_policy(
                    db,
                    payload.thread.id.as_str(),
                    payload.sandbox_mode,
                    thread_created_at,
                    thread_updated_at,
                )
                .await?;
            }
        } else {
            match actor {
                Some(actor) => {
                    thread::upsert_thread_with_creator(
                        db,
                        &payload.thread,
                        actor,
                        thread_created_at,
                        thread_updated_at,
                    )
                    .await?;
                }
                None => {
                    thread::upsert_thread(
                        db,
                        &payload.thread,
                        thread_created_at,
                        thread_updated_at,
                    )
                    .await?;
                }
            }
            policy::upsert_thread_sandbox_policy(
                db,
                payload.thread.id.as_str(),
                payload.sandbox_mode,
                thread_created_at,
                thread_updated_at,
            )
            .await?;
        }

        let existing_turn = turn::find_turn_by_id(db, payload.turn.id.as_str()).await?;
        let turn_status = turn_status_to_db(payload.turn.status).to_owned();
        let turn_error = payload.turn.error.clone();

        match actor {
            Some(actor) => {
                turn::upsert_turn_with_initiator(
                    db,
                    payload.turn.id.as_str(),
                    payload.thread.id.as_str(),
                    &payload.turn,
                    None,
                    payload.reasoning_effort.as_deref(),
                    actor,
                    thread_updated_at,
                    thread_updated_at,
                )
                .await?;
            }
            None => {
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
            }
        }

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

        if is_task_run_occurrence {
            self.project_thread_foreground_status(
                db,
                payload.thread.id.as_str(),
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
        let Some(existing_turn) = turn::find_turn_by_id(db, turn_model.id.as_str()).await? else {
            anyhow::bail!(
                "terminal turn event for `{}` has no preceding turn/start projection",
                turn_model.id
            );
        };
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

        let should_append_status = (existing_turn.status, existing_turn.error)
            != (turn_status.clone(), turn_error.clone());

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

        self.project_thread_foreground_status(db, thread_id, updated_at)
            .await?;

        Ok(())
    }

    async fn project_thread_foreground_status<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        thread_id: &str,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<()> {
        let status = if turn::has_in_progress_conversation_turn(db, thread_id).await? {
            ThreadStatus::Active
        } else {
            ThreadStatus::Idle
        };
        thread::update_thread_status(db, thread_id, status, updated_at).await
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

async fn legacy_backfill_superuser_id<C: ConnectionTrait>(
    db: &C,
) -> Result<Option<pioneer_protocol::PrincipalId>> {
    let principals = identity::list_gateway_principals(db).await?;
    let gateway = identity::load_gateway_singleton(db).await?;
    let mut superusers = principals.iter().filter(|principal| {
        principal.kind == identity::principal_kind_to_db(pioneer_protocol::PrincipalKind::Superuser)
    });
    let superuser = superusers.next();
    match (gateway.as_ref(), superuser) {
        (None, None) if principals.is_empty() => return Ok(None),
        (None, _) => anyhow::bail!(
            "legacy replay found persisted principals without a Gateway identity singleton"
        ),
        (Some(_), None) => {
            anyhow::bail!("legacy replay found a Gateway identity without a Superuser principal")
        }
        (Some(_), Some(_)) => {}
    };
    if superusers.next().is_some() {
        anyhow::bail!("legacy replay found multiple persisted Superuser principals");
    }
    let gateway =
        gateway.context("Gateway identity disappeared during legacy replay validation")?;
    let superuser = superuser.context("Superuser disappeared during legacy replay validation")?;
    if superuser.gateway_id != gateway.id.as_ref()
        || superuser.role_key.is_some()
        || superuser.status
            != identity::principal_status_to_db(pioneer_protocol::PrincipalStatus::Active)
        || superuser.removed_at.is_some()
    {
        anyhow::bail!("legacy replay found an invalid persisted Superuser principal");
    }
    Ok(Some(
        pioneer_protocol::PrincipalId::new(superuser.id.clone())
            .context("legacy replay found an invalid persisted Superuser principal id")?,
    ))
}

fn liveness_observation_from_event(
    event: &AppendedTurnEvent,
) -> Option<turn_liveness::TurnLivenessObservation> {
    let mut item_id = None;
    let mut item_type = None;
    let meaningful = match &event.payload {
        TurnEventPayload::TurnStarted(payload) => payload.turn.mode != ThreadMode::Message,
        TurnEventPayload::ItemStarted(payload) => {
            item_id = Some(payload.item.item_id().to_owned());
            item_type = Some(turn_item_type_to_db(payload.item.item_type()).to_owned());
            true
        }
        TurnEventPayload::ItemCompleted(payload) => {
            item_id = Some(payload.item.item_id().to_owned());
            item_type = Some(turn_item_type_to_db(payload.item.item_type()).to_owned());
            true
        }
        TurnEventPayload::ItemUpdated(payload) => {
            item_id = Some(payload.item.item_id().to_owned());
            item_type = Some(turn_item_type_to_db(payload.item.item_type()).to_owned());
            true
        }
        TurnEventPayload::TurnExecutionWindowStarted(_)
        | TurnEventPayload::TurnExecutionWindowCheckpointed(_)
        | TurnEventPayload::TurnExecutionWindowContinued(_) => true,
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
        | TurnEventPayload::TurnExecutionWindowExhausted(_)
        | TurnEventPayload::TurnExecutionWindowBlocked(_)
        | TurnEventPayload::TurnPermissionAudit(_)
        | TurnEventPayload::TurnMessageEdited(_)
        | TurnEventPayload::TurnMessageDeleted(_)
        | TurnEventPayload::TurnCompleted(_)
        | TurnEventPayload::TurnFailed(_)
        | TurnEventPayload::TurnBlocked(_) => false,
    };

    meaningful.then(|| turn_liveness::TurnLivenessObservation {
        turn_id: event.turn_id.clone(),
        thread_id: event.thread_id.clone(),
        activity_sequence: event.sequence,
        activity_kind: event.payload.event_type().to_owned(),
        item_id,
        item_type,
        observed_at: event.created_at,
    })
}

#[cfg(test)]
mod epic6_tests {
    use super::*;
    use pioneer_protocol::{
        Turn, TurnMessageDeletedEvent, TurnMessageEditedEvent, TurnStatus,
        default_turn_permission_profile_snapshot,
    };

    fn completed_turn() -> Turn {
        Turn {
            id: "turn-message".to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: pioneer_protocol::ThreadMode::Message,
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 1,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: default_turn_permission_profile_snapshot(),
        }
    }

    fn event(payload: TurnEventPayload) -> AppendedTurnEvent {
        AppendedTurnEvent {
            id: "event-message-mutation".to_owned(),
            thread_id: "thread-message".to_owned(),
            turn_id: "turn-message".to_owned(),
            sequence: 9,
            payload,
            idempotency_key: None,
            was_inserted: true,
            created_at: unix_to_datetime(1_700_000_000),
        }
    }

    #[test]
    fn message_mutations_are_not_execution_liveness_events() {
        let edit = event(TurnEventPayload::TurnMessageEdited(
            TurnMessageEditedEvent {
                workspace_id: "workspace".to_owned(),
                thread_id: "thread-message".to_owned(),
                turn: completed_turn(),
                input: Vec::new(),
                changed_by: PersistedActorRef::System,
                changed_at: 1_700_000_000,
            },
        ));
        let mut deleted_turn = completed_turn();
        deleted_turn.message_deleted = true;
        let delete = event(TurnEventPayload::TurnMessageDeleted(
            TurnMessageDeletedEvent {
                workspace_id: "workspace".to_owned(),
                thread_id: "thread-message".to_owned(),
                turn: deleted_turn,
                deleted_by: PersistedActorRef::System,
                deleted_at: 1_700_000_001,
            },
        ));

        assert!(liveness_observation_from_event(&edit).is_none());
        assert!(liveness_observation_from_event(&delete).is_none());
    }

    #[test]
    fn message_start_is_not_an_execution_liveness_event() {
        let mut turn = completed_turn();
        turn.status = TurnStatus::InProgress;
        turn.message_revision = 0;
        let started = event(TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: pioneer_protocol::Thread {
                workspace_id: "workspace".to_owned(),
                id: "thread-message".to_owned(),
                name: None,
                preview: "hello".to_owned(),
                mode: ThreadMode::Message,
                model: "unused".to_owned(),
                model_provider: "unused".to_owned(),
                reasoning_effort: None,
                created_at: 1_700_000_000,
                updated_at: 1_700_000_000,
                status: ThreadStatus::Idle,
                origin_kind: pioneer_protocol::ThreadOriginKind::User,
                sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
                agent_nickname: None,
                agent_role: None,
                visibility: None,
                turns: vec![turn.clone()],
            },
            sandbox_mode: pioneer_protocol::SandboxMode::FullAccess,
            turn,
            input: vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            actor: Some(PersistedActorRef::System),
            reasoning_effort: None,
        }));

        assert!(liveness_observation_from_event(&started).is_none());
    }
}
