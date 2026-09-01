use anyhow::{Context, Result};
use pioneer_protocol::{
    PersistedActorRef, ThreadMode, ThreadStatus, TurnItemAttemptStatus, TurnKind,
    TurnMessageDeletedEvent, TurnMessageEditedEvent, TurnMessageRevisionChangeKind, TurnOrigin,
    TurnStatus, UserInput,
};
use sea_orm::ConnectionTrait;
use std::future::Future;
use std::pin::Pin;

use crate::convention::{TURN_ITEM_STATUS_IN_PROGRESS, turn_item_type_to_db};
use crate::events::{AppendedTurnEvent, TurnEventPayload, TurnStartedEventPayload};
use crate::repositories::{
    identity, policy, self_improvement_source_turn, thread, turn, turn_item_attempt, turn_liveness,
};
use crate::turn_item_terminal::{
    attempt_status_from_payload, terminal_turn_item_status_from_payload,
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

pub(crate) fn message_thread_preview(input: &[UserInput]) -> String {
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

#[derive(Clone, Debug)]
enum PreparedTurnMessageMutationProjection {
    Superseded {
        expected_turn: pioneer_entity::turn::Model,
    },
    Apply {
        expected_turn: pioneer_entity::turn::Model,
        expected_input_rows: Vec<pioneer_entity::turn_input::Model>,
        expected_previous_revision: Option<pioneer_entity::turn_message_revision::Model>,
        previous_preview: String,
        replacement_preview: String,
        preview_updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        already_projected: bool,
        revision: Option<turn::PreparedTurnMessageRevision>,
        mutation_state: turn::PreparedMessageTurnMutationState,
    },
}

#[derive(Clone, Default)]
pub struct TurnProjector;

#[derive(Clone, Debug, Default)]
pub(crate) struct PreparedTurnProjection {
    liveness: Option<turn_liveness::TurnLivenessObservation>,
    item: Option<turn::PreparedTurnItemProjection>,
    item_attempt_payload_json: Option<String>,
    item_attempt_id: Option<String>,
    item_attempt_failure_reason: Option<String>,
    input: Option<turn::PreparedTurnInputProjection>,
    terminal_running_attempts: Vec<turn_item_attempt::PreparedTerminalRunningAttempt>,
    message_mutation: Option<PreparedTurnMessageMutationProjection>,
    turn_upsert: Option<turn::PreparedTurnUpsert>,
    thread_preview_author_json: Option<Option<String>>,
    message_preview_author_json: Option<Option<String>>,
    message_thread_preview: Option<String>,
    task_run_parent_thread: Option<pioneer_protocol::Thread>,
    self_improvement_source:
        Option<self_improvement_source_turn::PreparedSelfImprovementSourceTurn>,
    legacy_superuser_id: Option<Option<pioneer_protocol::PrincipalId>>,
    status_history: Option<turn::PreparedTurnStatusHistory>,
    append_status_history: bool,
}

impl TurnProjector {
    pub fn new() -> Self {
        Self
    }

    pub(crate) async fn prepare<C: ConnectionTrait>(
        &self,
        db: &C,
        event_id: &str,
        payload: &TurnEventPayload,
        created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<PreparedTurnProjection> {
        let mut prepared = PreparedTurnProjection {
            liveness: liveness_observation_from_payload(payload, created_at),
            ..Default::default()
        };
        match payload {
            TurnEventPayload::TurnStarted(payload) => {
                let updated_at = unix_to_datetime(payload.thread.updated_at);
                prepared.thread_preview_author_json = Some(thread::prepare_preview_author_json(
                    payload.thread.preview_author.as_ref(),
                )?);
                prepared.message_preview_author_json = Some(thread::prepare_preview_author_json(
                    payload.turn.author.as_ref(),
                )?);
                prepared.message_thread_preview =
                    Some(message_thread_preview(payload.input.as_slice()));
                if payload.turn.turn_kind == TurnKind::TaskRun {
                    let mut parent = payload.thread.clone();
                    parent.status = ThreadStatus::Idle;
                    prepared.task_run_parent_thread = Some(parent);
                }
                if payload.actor.is_none() {
                    prepared.legacy_superuser_id = Some(legacy_backfill_superuser_id(db).await?);
                }
                prepared.status_history = Some(turn::prepare_turn_status_history(
                    payload.turn.id.as_str(),
                    payload.turn.status,
                    payload.turn.error.clone(),
                    updated_at,
                ));
                prepared.input = Some(turn::prepare_turn_input_projection(
                    payload.turn.id.as_str(),
                    payload.input.as_slice(),
                    created_at,
                )?);
                let turn_upsert = turn::prepare_projected_turn_upsert(
                    db,
                    payload.turn.id.as_str(),
                    payload.thread.id.as_str(),
                    &payload.turn,
                    None,
                    payload.reasoning_effort.as_deref(),
                    payload.actor.as_ref(),
                    payload.actor.is_some(),
                    false,
                    updated_at,
                    updated_at,
                )
                .await?;
                prepared.append_status_history = turn_upsert
                    .changes_status_history(payload.turn.status, payload.turn.error.as_deref());
                prepared.turn_upsert = Some(turn_upsert);
            }
            TurnEventPayload::ItemStarted(payload) => {
                let item = turn::prepare_turn_item_projection(
                    payload.turn_id.as_str(),
                    &payload.item,
                    Some(TURN_ITEM_STATUS_IN_PROGRESS),
                    created_at,
                    created_at,
                )?;
                prepared.item_attempt_payload_json = Some(item.payload_json().to_owned());
                prepared.item_attempt_id = Some(pioneer_protocol::generate_id(21));
                prepared.item = Some(item);
            }
            TurnEventPayload::ItemCompleted(payload) => {
                let status = terminal_turn_item_status_from_payload(&payload.item);
                if attempt_status_from_payload(&payload.item) == TurnItemAttemptStatus::Failed {
                    prepared.item_attempt_failure_reason = Some(format!(
                        "item `{}` completed with failed status",
                        payload.item.item_id()
                    ));
                }
                prepared.item = Some(turn::prepare_turn_item_projection(
                    payload.turn_id.as_str(),
                    &payload.item,
                    Some(status),
                    created_at,
                    created_at,
                )?);
                prepared.self_improvement_source =
                    self_improvement_source_turn::prepare_completed_collaborative_source_exchange(
                        db, event_id, created_at, payload,
                    )
                    .await?;
            }
            TurnEventPayload::ItemUpdated(payload) => {
                let status = terminal_turn_item_status_from_payload(&payload.item);
                prepared.item = Some(turn::prepare_turn_item_projection(
                    payload.turn_id.as_str(),
                    &payload.item,
                    Some(status),
                    created_at,
                    created_at,
                )?);
            }
            TurnEventPayload::TurnMessageEdited(payload) => {
                prepared.input = Some(turn::prepare_turn_input_projection(
                    payload.turn.id.as_str(),
                    payload.input.as_slice(),
                    created_at,
                )?);
                prepared.message_mutation = Some(
                    self.prepare_turn_message_mutation(
                        db,
                        TurnMessageMutationProjection::Edit(payload),
                        created_at,
                    )
                    .await?,
                );
            }
            TurnEventPayload::TurnMessageDeleted(payload) => {
                prepared.input = Some(turn::prepare_turn_input_projection(
                    payload.turn.id.as_str(),
                    &[],
                    created_at,
                )?);
                prepared.message_mutation = Some(
                    self.prepare_turn_message_mutation(
                        db,
                        TurnMessageMutationProjection::Delete(payload),
                        created_at,
                    )
                    .await?,
                );
            }
            TurnEventPayload::TurnCompleted(payload) => {
                prepared.status_history = Some(turn::prepare_turn_status_history(
                    payload.turn.id.as_str(),
                    payload.turn.status,
                    payload.turn.error.clone(),
                    created_at,
                ));
                prepared.self_improvement_source =
                    self_improvement_source_turn::prepare_completed_source_turn(
                        db, event_id, created_at, payload,
                    )
                    .await?;
                let turn_upsert = turn::prepare_projected_turn_upsert(
                    db,
                    payload.turn.id.as_str(),
                    payload.thread_id.as_str(),
                    &payload.turn,
                    None,
                    None,
                    None,
                    false,
                    true,
                    created_at,
                    created_at,
                )
                .await?;
                prepared.append_status_history = turn_upsert
                    .changes_status_history(payload.turn.status, payload.turn.error.as_deref());
                prepared.turn_upsert = Some(turn_upsert);
                prepared.terminal_running_attempts =
                    turn_item_attempt::prepare_terminal_running_attempts(
                        db,
                        payload.turn.id.as_str(),
                        created_at,
                    )
                    .await?;
            }
            TurnEventPayload::TurnFailed(payload) => {
                prepared.status_history = Some(turn::prepare_turn_status_history(
                    payload.turn.id.as_str(),
                    payload.turn.status,
                    payload.turn.error.clone(),
                    created_at,
                ));
                let turn_upsert = turn::prepare_projected_turn_upsert(
                    db,
                    payload.turn.id.as_str(),
                    payload.thread_id.as_str(),
                    &payload.turn,
                    None,
                    None,
                    None,
                    false,
                    true,
                    created_at,
                    created_at,
                )
                .await?;
                prepared.append_status_history = turn_upsert
                    .changes_status_history(payload.turn.status, payload.turn.error.as_deref());
                prepared.turn_upsert = Some(turn_upsert);
                prepared.terminal_running_attempts =
                    turn_item_attempt::prepare_terminal_running_attempts(
                        db,
                        payload.turn.id.as_str(),
                        created_at,
                    )
                    .await?;
            }
            TurnEventPayload::TurnBlocked(payload) => {
                prepared.status_history = Some(turn::prepare_turn_status_history(
                    payload.turn.id.as_str(),
                    payload.turn.status,
                    payload.turn.error.clone(),
                    created_at,
                ));
                let turn_upsert = turn::prepare_projected_turn_upsert(
                    db,
                    payload.turn.id.as_str(),
                    payload.thread_id.as_str(),
                    &payload.turn,
                    None,
                    None,
                    None,
                    false,
                    true,
                    created_at,
                    created_at,
                )
                .await?;
                prepared.append_status_history = turn_upsert
                    .changes_status_history(payload.turn.status, payload.turn.error.as_deref());
                prepared.turn_upsert = Some(turn_upsert);
                prepared.terminal_running_attempts =
                    turn_item_attempt::prepare_terminal_running_attempts(
                        db,
                        payload.turn.id.as_str(),
                        created_at,
                    )
                    .await?;
            }
            _ => {}
        }
        Ok(prepared)
    }

    pub async fn project_prepared<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        event: &AppendedTurnEvent,
        prepared: PreparedTurnProjection,
    ) -> Result<()> {
        let created_at = event.created_at;
        if let Some(mut observation) = prepared.liveness {
            observation.activity_sequence = event.sequence;
            turn_liveness::observe_activity(db, observation).await?;
        }
        let item_projection = prepared.item;
        let item_attempt_payload_json = prepared.item_attempt_payload_json;
        let item_attempt_id = prepared.item_attempt_id;
        let item_attempt_failure_reason = prepared.item_attempt_failure_reason;
        let input_projection = prepared.input;
        let terminal_running_attempts = prepared.terminal_running_attempts;
        let message_mutation = prepared.message_mutation;
        let turn_upsert = prepared.turn_upsert;
        let thread_preview_author_json = prepared.thread_preview_author_json;
        let message_preview_author_json = prepared.message_preview_author_json;
        let message_thread_preview = prepared.message_thread_preview;
        let task_run_parent_thread = prepared.task_run_parent_thread;
        let self_improvement_source = prepared.self_improvement_source;
        let legacy_superuser_id = prepared.legacy_superuser_id;
        let status_history = prepared.status_history;
        let append_status_history = prepared.append_status_history;
        let future: ProjectFuture<'_> = match &event.payload {
            TurnEventPayload::TurnStarted(payload) => project_future(
                self.project_turn_started(
                    db,
                    payload,
                    event.sequence,
                    input_projection.context("turn/start input projection was not prepared")?,
                    turn_upsert.context("turn/start Turn projection was not prepared")?,
                    thread_preview_author_json
                        .context("turn/start Thread projection was not prepared")?,
                    message_preview_author_json
                        .context("turn/start Message preview was not prepared")?,
                    message_thread_preview
                        .context("turn/start derived preview was not prepared")?,
                    task_run_parent_thread,
                    legacy_superuser_id,
                    status_history.context("turn/start status history was not prepared")?,
                ),
            ),
            TurnEventPayload::ItemStarted(payload) => project_future(async move {
                turn::upsert_prepared_turn_item(
                    db,
                    item_projection.context("item/start projection was not prepared")?,
                )
                .await?;

                turn_item_attempt::create_running_attempt(
                    db,
                    item_attempt_id.context("item/start attempt id was not prepared")?,
                    payload.turn_id.as_str(),
                    payload.item.item_id(),
                    payload.item.item_type(),
                    payload.item.execution_class(),
                    item_attempt_payload_json
                        .context("item/start attempt payload was not prepared")?,
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
                turn::upsert_prepared_turn_item(
                    db,
                    item_projection.context("item/completed projection was not prepared")?,
                )
                .await?;

                let attempt_status = attempt_status_from_payload(&payload.item);

                let _ = turn_item_attempt::finish_running_attempt(
                    db,
                    payload.turn_id.as_str(),
                    payload.item.item_id(),
                    attempt_status,
                    item_attempt_failure_reason,
                    created_at,
                )
                .await?;

                if let Some(source) = self_improvement_source {
                    self_improvement_source_turn::apply_prepared_source_turn(db, source).await?;
                }

                Ok(())
            }),
            TurnEventPayload::ItemUpdated(_payload) => project_future(async move {
                turn::upsert_prepared_turn_item(
                    db,
                    item_projection.context("item/updated projection was not prepared")?,
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
                    payload.thread_id.as_str(),
                    payload.turn.id.as_str(),
                    payload.turn.message_revision,
                    message_mutation.context("message edit projection was not prepared")?,
                    input_projection.context("message edit input projection was not prepared")?,
                ))
            }
            TurnEventPayload::TurnMessageDeleted(payload) => {
                project_future(self.project_turn_message_mutation(
                    db,
                    payload.thread_id.as_str(),
                    payload.turn.id.as_str(),
                    payload.turn.message_revision,
                    message_mutation.context("message delete projection was not prepared")?,
                    input_projection.context("message delete input projection was not prepared")?,
                ))
            }
            TurnEventPayload::TurnCompleted(payload) => project_future(async move {
                turn_item_attempt::close_prepared_terminal_running_attempts(
                    db,
                    payload.turn.id.as_str(),
                    terminal_running_attempts,
                )
                .await?;
                self.project_turn_finished(
                    db,
                    payload.thread_id.as_str(),
                    created_at,
                    turn_upsert.context("turn/completed Turn projection was not prepared")?,
                    status_history.context("turn/completed status history was not prepared")?,
                    append_status_history,
                )
                .await?;
                if let Some(source) = self_improvement_source {
                    self_improvement_source_turn::apply_prepared_source_turn(db, source).await?;
                }
                Ok(())
            }),
            TurnEventPayload::TurnFailed(payload) => project_future(async move {
                turn_item_attempt::close_prepared_terminal_running_attempts(
                    db,
                    payload.turn.id.as_str(),
                    terminal_running_attempts,
                )
                .await?;
                self.project_turn_finished(
                    db,
                    payload.thread_id.as_str(),
                    created_at,
                    turn_upsert.context("turn/failed Turn projection was not prepared")?,
                    status_history.context("turn/failed status history was not prepared")?,
                    append_status_history,
                )
                .await
            }),
            TurnEventPayload::TurnBlocked(payload) => project_future(async move {
                turn_item_attempt::close_prepared_terminal_running_attempts(
                    db,
                    payload.turn.id.as_str(),
                    terminal_running_attempts,
                )
                .await?;
                self.project_turn_finished(
                    db,
                    payload.thread_id.as_str(),
                    created_at,
                    turn_upsert.context("turn/blocked Turn projection was not prepared")?,
                    status_history.context("turn/blocked status history was not prepared")?,
                    append_status_history,
                )
                .await
            }),
        };
        future.await
    }

    #[cfg(test)]
    pub async fn project<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        event: &AppendedTurnEvent,
    ) -> Result<()> {
        let prepared = self
            .prepare(db, event.id.as_str(), &event.payload, event.created_at)
            .await?;
        self.project_prepared(db, event, prepared).await
    }

    async fn prepare_turn_message_mutation<C: ConnectionTrait>(
        &self,
        db: &C,
        mutation: TurnMessageMutationProjection<'_>,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<PreparedTurnMessageMutationProjection> {
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
            return Ok(PreparedTurnMessageMutationProjection::Superseded {
                expected_turn: turn_model,
            });
        }
        if current_revision < event_turn.message_revision
            && current_revision.checked_add(1) != Some(event_turn.message_revision)
        {
            anyhow::bail!("Turn message mutation replay has a revision gap");
        }

        let current_input_rows = turn::find_turn_inputs(db, event_turn.id.as_str()).await?;
        let current_input = current_input_rows
            .iter()
            .map(|row| {
                serde_json::from_str::<UserInput>(row.payload.as_str())
                    .context("failed to decode current Turn input during mutation replay")
            })
            .collect::<Result<Vec<_>>>()?;
        let (expected_previous_revision, previous_input) =
            if current_revision < event_turn.message_revision {
                (None, current_input.clone())
            } else {
                let previous_revision_model = turn::list_turn_message_revisions(
                    db,
                    event_turn.id.as_str(),
                    Some(event_turn.message_revision),
                    1,
                )
                .await?
                .into_iter()
                .next()
                .context("message mutation replay is missing its previous revision")?;
                let previous_revision =
                    turn::turn_message_revision_from_model(previous_revision_model.clone())?;
                if previous_revision.revision.checked_add(1) != Some(event_turn.message_revision) {
                    anyhow::bail!("message mutation replay is missing its previous revision");
                }
                let previous_input = previous_revision
                    .input
                    .context("message mutation replay previous revision has no input")?;
                (Some(previous_revision_model), previous_input)
            };
        let previous_preview = message_thread_preview(previous_input.as_slice());
        let replacement_preview = if is_delete {
            String::new()
        } else {
            message_thread_preview(projected_input)
        };
        let already_projected = current_revision == event_turn.message_revision
            && collaboration.mentions == event_turn.mentions
            && collaboration.message_deleted == event_turn.message_deleted
            && current_input == projected_input;

        let revision = if current_revision < event_turn.message_revision {
            Some(turn::prepare_turn_message_revision(
                turn::NewTurnMessageRevision {
                    turn_id: event_turn.id.as_str(),
                    revision: current_revision,
                    input: current_input.as_slice(),
                    mentions: collaboration.mentions.as_slice(),
                    changed_by,
                    change_kind,
                    created_at: updated_at,
                },
            )?)
        } else {
            None
        };
        let mutation_state = turn::prepare_message_turn_mutation_state(
            turn_model.thread_id.as_str(),
            event_turn.id.as_str(),
            event_turn.message_revision,
            event_turn.mentions.as_slice(),
            deleted_by,
            updated_at,
        )?;

        Ok(PreparedTurnMessageMutationProjection::Apply {
            expected_turn: turn_model,
            expected_input_rows: current_input_rows,
            expected_previous_revision,
            previous_preview,
            replacement_preview,
            preview_updated_at: updated_at,
            already_projected,
            revision,
            mutation_state,
        })
    }

    async fn project_turn_message_mutation<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        thread_id: &str,
        turn_id: &str,
        event_revision: u64,
        prepared: PreparedTurnMessageMutationProjection,
        input_projection: turn::PreparedTurnInputProjection,
    ) -> Result<()> {
        let expected_revision = i64::try_from(event_revision)
            .context("Turn message revision exceeds database integer range")?;
        match prepared {
            PreparedTurnMessageMutationProjection::Superseded { expected_turn } => {
                let current = turn::find_turn_by_thread_and_id(db, thread_id, turn_id)
                    .await?
                    .context("message mutation event targets a missing Turn")?;
                if current != expected_turn && current.message_revision <= expected_revision {
                    anyhow::bail!("Turn message mutation changed during projection preparation");
                }
                Ok(())
            }
            PreparedTurnMessageMutationProjection::Apply {
                expected_turn,
                expected_input_rows,
                expected_previous_revision,
                previous_preview,
                replacement_preview,
                preview_updated_at,
                already_projected,
                revision,
                mutation_state,
            } => {
                let current_turn = turn::find_turn_by_thread_and_id(db, thread_id, turn_id)
                    .await?
                    .context("message mutation event targets a missing Turn")?;
                if current_turn != expected_turn {
                    anyhow::bail!("Turn message mutation changed during projection preparation");
                }
                let current_input_rows = turn::find_turn_inputs(db, turn_id).await?;
                if current_input_rows != expected_input_rows {
                    anyhow::bail!("Turn message input changed during projection preparation");
                }
                if let Some(expected_previous_revision) = expected_previous_revision {
                    let current_previous_revision =
                        turn::list_turn_message_revisions(db, turn_id, Some(event_revision), 1)
                            .await?
                            .into_iter()
                            .next();
                    if current_previous_revision.as_ref() != Some(&expected_previous_revision) {
                        anyhow::bail!(
                            "Turn message revision changed during projection preparation"
                        );
                    }
                }
                thread::replace_thread_preview_if_matches(
                    db,
                    thread_id,
                    previous_preview.as_str(),
                    replacement_preview.as_str(),
                    preview_updated_at,
                )
                .await?;
                if already_projected {
                    return Ok(());
                }
                if let Some(revision) = revision {
                    turn::insert_prepared_turn_message_revision(db, revision, true).await?;
                }
                if !turn::project_prepared_message_turn_mutation_state(db, mutation_state).await? {
                    anyhow::bail!("Turn message mutation replay lost its target");
                }
                turn::replace_prepared_turn_input(db, input_projection).await
            }
        }
    }

    async fn project_turn_started<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
        event_sequence: i64,
        input_projection: turn::PreparedTurnInputProjection,
        turn_upsert: turn::PreparedTurnUpsert,
        thread_preview_author_json: Option<String>,
        message_preview_author_json: Option<String>,
        message_thread_preview: String,
        task_run_parent_thread: Option<pioneer_protocol::Thread>,
        legacy_superuser_id: Option<Option<pioneer_protocol::PrincipalId>>,
        status_history: turn::PreparedTurnStatusHistory,
    ) -> Result<()> {
        match payload.actor.as_ref() {
            Some(actor) => {
                self.project_attributed_turn_started(
                    db,
                    payload,
                    actor,
                    event_sequence,
                    turn_upsert,
                    thread_preview_author_json,
                    message_preview_author_json,
                    message_thread_preview,
                    task_run_parent_thread,
                )
                .await?;
            }
            None => {
                self.project_legacy_turn_started(
                    db,
                    payload,
                    event_sequence,
                    turn_upsert,
                    thread_preview_author_json,
                    message_preview_author_json,
                    message_thread_preview,
                    task_run_parent_thread,
                    legacy_superuser_id
                        .context("legacy turn/start Superuser projection was not prepared")?,
                )
                .await?;
            }
        }
        self.project_turn_started_input_and_status(
            db,
            payload,
            input_projection,
            unix_to_datetime(payload.thread.updated_at),
            payload.turn.turn_kind == TurnKind::TaskRun,
            status_history,
        )
        .await
    }

    async fn project_attributed_turn_started<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
        actor: &PersistedActorRef,
        event_sequence: i64,
        turn_upsert: turn::PreparedTurnUpsert,
        thread_preview_author_json: Option<String>,
        message_preview_author_json: Option<String>,
        message_thread_preview: String,
        task_run_parent_thread: Option<pioneer_protocol::Thread>,
    ) -> Result<()> {
        self.project_turn_started_inner(
            db,
            payload,
            Some(actor),
            event_sequence,
            turn_upsert,
            thread_preview_author_json,
            message_preview_author_json,
            message_thread_preview,
            task_run_parent_thread,
        )
        .await
    }

    /// Compatibility seam for append-only turn/start events written before actor attribution.
    /// Normal materialization rejects this shape before append.
    async fn project_legacy_turn_started<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
        event_sequence: i64,
        turn_upsert: turn::PreparedTurnUpsert,
        thread_preview_author_json: Option<String>,
        message_preview_author_json: Option<String>,
        message_thread_preview: String,
        task_run_parent_thread: Option<pioneer_protocol::Thread>,
        legacy_superuser_id: Option<pioneer_protocol::PrincipalId>,
    ) -> Result<()> {
        self.project_turn_started_inner(
            db,
            payload,
            None,
            event_sequence,
            turn_upsert,
            thread_preview_author_json,
            message_preview_author_json,
            message_thread_preview,
            task_run_parent_thread,
        )
        .await?;
        if let Some(superuser_id) = legacy_superuser_id {
            identity::backfill_legacy_actor_references_for_turn(
                db,
                payload.thread.id.as_str(),
                payload.turn.id.as_str(),
                &superuser_id,
            )
            .await?;
        }
        Ok(())
    }

    async fn project_turn_started_inner<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
        actor: Option<&PersistedActorRef>,
        event_sequence: i64,
        turn_upsert: turn::PreparedTurnUpsert,
        thread_preview_author_json: Option<String>,
        message_preview_author_json: Option<String>,
        message_thread_preview: String,
        task_run_parent_thread: Option<pioneer_protocol::Thread>,
    ) -> Result<()> {
        self.project_turn_started_identity(db, payload, event_sequence)
            .await?;

        let thread_created_at = unix_to_datetime(payload.thread.created_at);
        let thread_updated_at = unix_to_datetime(payload.thread.updated_at);
        let is_task_run_occurrence = payload.turn.turn_kind == TurnKind::TaskRun;

        self.project_turn_started_thread(
            db,
            payload,
            actor,
            thread_created_at,
            thread_updated_at,
            is_task_run_occurrence,
            thread_preview_author_json,
            message_preview_author_json,
            message_thread_preview,
            task_run_parent_thread,
        )
        .await?;
        turn::apply_prepared_turn_upsert(db, turn_upsert).await?;
        Ok(())
    }

    fn project_turn_started_identity<'a, C: ConnectionTrait + Sync>(
        &'a self,
        db: &'a C,
        payload: &'a TurnStartedEventPayload,
        event_sequence: i64,
    ) -> ProjectFuture<'a> {
        project_future(async move {
            // Sequence 1 may legitimately be replayed after projection-state loss,
            // and the explicit legacy seam may revisit an actorless in-progress
            // row to perform its idempotent actor backfill. A second TurnStarted is
            // necessarily later in the same canonical stream. Fence that identity
            // reuse before touching Thread/input projections. A terminal read model
            // is also monotonic even when imported legacy history has no event row.
            if let Some(existing) = turn::find_turn_by_id(db, payload.turn.id.as_str()).await?
                && (event_sequence != 1 || existing.status != "in_progress")
            {
                anyhow::bail!(
                    "turn `{}` already exists with status `{}`; TurnStarted identity reuse is forbidden",
                    payload.turn.id,
                    existing.status
                );
            }

            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn project_turn_started_thread<'a, C: ConnectionTrait + Sync>(
        &'a self,
        db: &'a C,
        payload: &'a TurnStartedEventPayload,
        actor: Option<&'a PersistedActorRef>,
        thread_created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        thread_updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        is_task_run_occurrence: bool,
        thread_preview_author_json: Option<String>,
        message_preview_author_json: Option<String>,
        message_thread_preview: String,
        task_run_parent_thread: Option<pioneer_protocol::Thread>,
    ) -> ProjectFuture<'a> {
        project_future(async move {
            if is_task_run_occurrence {
                // A task-run occurrence is only a parent-timeline projection. The
                // parent thread and its foreground policy already exist, and a
                // stale task payload must not overwrite metadata or foreground
                // state written by a concurrent conversation turn.
                if thread::find_thread_by_id(db, payload.thread.id.as_str())
                    .await?
                    .is_none()
                {
                    let parent = task_run_parent_thread
                        .context("task-run parent Thread projection was not prepared")?;
                    thread::upsert_projected_thread(
                        db,
                        &parent,
                        actor,
                        thread_preview_author_json,
                        thread_created_at,
                        thread_updated_at,
                    )
                    .await?;
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
                thread::touch_thread_for_completed_message_prepared(
                    db,
                    payload.thread.id.as_str(),
                    message_thread_preview.as_str(),
                    message_preview_author_json,
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
                let existing_thread = thread::find_thread_by_id(db, payload.thread.id.as_str())
                    .await?
                    .is_some();
                let creator = (!existing_thread).then_some(actor).flatten();
                thread::upsert_projected_thread(
                    db,
                    &payload.thread,
                    creator,
                    thread_preview_author_json,
                    thread_created_at,
                    thread_updated_at,
                )
                .await?;
                policy::upsert_thread_sandbox_policy(
                    db,
                    payload.thread.id.as_str(),
                    payload.sandbox_mode,
                    thread_created_at,
                    thread_updated_at,
                )
                .await?;
            }

            Ok(())
        })
    }

    fn project_turn_started_input_and_status<'a, C: ConnectionTrait + Sync>(
        &'a self,
        db: &'a C,
        payload: &'a TurnStartedEventPayload,
        input_projection: turn::PreparedTurnInputProjection,
        thread_updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        is_task_run_occurrence: bool,
        status_history: turn::PreparedTurnStatusHistory,
    ) -> ProjectFuture<'a> {
        project_future(async move {
            turn::replace_prepared_turn_input(db, input_projection).await?;

            turn::append_prepared_turn_status_history(db, status_history).await?;

            if is_task_run_occurrence {
                self.project_thread_foreground_status(
                    db,
                    payload.thread.id.as_str(),
                    thread_updated_at,
                )
                .await?;
            }

            Ok(())
        })
    }

    async fn project_turn_finished<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        thread_id: &str,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        turn_upsert: turn::PreparedTurnUpsert,
        status_history: turn::PreparedTurnStatusHistory,
        append_status_history: bool,
    ) -> Result<()> {
        turn::apply_prepared_turn_upsert(db, turn_upsert).await?;

        if append_status_history {
            turn::append_prepared_turn_status_history(db, status_history).await?;
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

fn liveness_observation_from_payload(
    payload: &TurnEventPayload,
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> Option<turn_liveness::TurnLivenessObservation> {
    let mut item_id = None;
    let mut item_type = None;
    let meaningful = match payload {
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
        turn_id: payload.turn_id().to_owned(),
        thread_id: payload.thread_id().to_owned(),
        activity_sequence: 0,
        activity_kind: payload.event_type().to_owned(),
        item_id,
        item_type,
        observed_at: created_at,
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

        assert!(liveness_observation_from_payload(&edit.payload, edit.created_at).is_none());
        assert!(liveness_observation_from_payload(&delete.payload, delete.created_at).is_none());
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
                preview_author: None,
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

        assert!(liveness_observation_from_payload(&started.payload, started.created_at).is_none());
    }
}
