use crate::events::{AppendedTurnEvent, TurnEventPayload, TurnStartedEventPayload};
use crate::turn_item_terminal::{
    attempt_status_from_payload, terminal_turn_item_status_from_payload,
};
use anyhow::{Context, Result};
use pioneer_crud_conversations::message_replay::{
    PreparedTurnMessageMutationProjection, TurnMessageMutationProjection, message_thread_preview,
    prepare_turn_message_mutation, project_turn_message_mutation,
};
use pioneer_crud_conversations::{turn_finalization, turn_liveness};
use pioneer_crud_execution::attempt_values::{
    TURN_ITEM_STATUS_CANCELLED, TURN_ITEM_STATUS_IN_PROGRESS,
};
use pioneer_crud_execution::event_liveness::liveness_observation_from_payload;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::turn_item_attempt;
use pioneer_crud_self_improvement::source_turn as self_improvement_source_turn;
use pioneer_protocol::{PersistedActorRef, ThreadStatus, TurnItemAttemptStatus, TurnKind};
use sea_orm::ConnectionTrait;
use std::future::Future;
use std::pin::Pin;

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

#[derive(Clone, Debug, Default)]
pub(crate) struct PreparedTurnProjection {
    liveness: Option<turn_liveness::TurnLivenessObservation>,
    item: Option<pioneer_crud_conversations::turn_item::PreparedTurnItemProjection>,
    pub(crate) timeline_item: Option<serde_json::Value>,
    item_attempt_payload_json: Option<String>,
    item_attempt_id: Option<String>,
    item_attempt_failure_reason: Option<String>,
    managed_cli_mcp_completion: bool,
    item_terminal_attempt_status: Option<TurnItemAttemptStatus>,
    input: Option<crate::store::conversations::turn_writes::PreparedTurnInputProjection>,
    terminal_running_attempts: Vec<turn_item_attempt::PreparedTerminalRunningAttempt>,
    message_mutation: Option<PreparedTurnMessageMutationProjection>,
    turn_upsert: Option<pioneer_crud_conversations::turn_writes::PreparedTurnUpsert>,
    thread_preview_author_json: Option<Option<String>>,
    message_preview_author_json: Option<Option<String>>,
    message_thread_preview: Option<String>,
    task_run_parent_thread: Option<pioneer_protocol::Thread>,
    self_improvement_source:
        Option<self_improvement_source_turn::PreparedSelfImprovementSourceTurn>,
    status_history: Option<pioneer_crud_conversations::turn_writes::PreparedTurnStatusHistory>,
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
                prepared.thread_preview_author_json = Some(
                    pioneer_crud_conversations::thread_metadata::prepare_preview_author_json(
                        payload.thread.preview_author.as_ref(),
                    )?,
                );
                prepared.message_preview_author_json = Some(
                    pioneer_crud_conversations::thread_metadata::prepare_preview_author_json(
                        payload.turn.author.as_ref(),
                    )?,
                );
                prepared.message_thread_preview =
                    Some(message_thread_preview(payload.input.as_slice()));
                if payload.turn.turn_kind == TurnKind::TaskRun {
                    let mut parent = payload.thread.clone();
                    parent.status = ThreadStatus::Idle;
                    prepared.task_run_parent_thread = Some(parent);
                }
                anyhow::ensure!(payload.actor.is_some(), "turn/start requires an actor");
                prepared.status_history = Some(
                    pioneer_crud_conversations::turn_writes::prepare_turn_status_history(
                        payload.turn.id.as_str(),
                        payload.turn.status,
                        payload.turn.error.clone(),
                        updated_at,
                    ),
                );
                prepared.input = Some(
                    crate::store::conversations::turn_writes::prepare_turn_input_projection(
                        payload.turn.id.as_str(),
                        payload.input.as_slice(),
                        created_at,
                    )?,
                );
                let turn_upsert =
                    pioneer_crud_conversations::turn_writes::prepare_projected_turn_upsert(
                        db,
                        payload.turn.id.as_str(),
                        payload.thread.id.as_str(),
                        &payload.turn,
                        None,
                        payload.reasoning_effort.as_deref(),
                        Some(payload.work_owner),
                        payload.actor.as_ref(),
                        payload.actor.is_some(),
                        updated_at,
                        updated_at,
                    )
                    .await?;
                prepared.append_status_history = turn_upsert
                    .changes_status_history(payload.turn.status, payload.turn.error.as_deref());
                prepared.turn_upsert = Some(turn_upsert);
            }
            TurnEventPayload::ItemStarted(payload) => {
                let item = pioneer_crud_conversations::turn_item::prepare_turn_item_projection(
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
                prepared.managed_cli_mcp_completion = crate::is_managed_cli_mcp_item(&payload.item);
                let mut status = terminal_turn_item_status_from_payload(&payload.item);
                let mut attempt_status = attempt_status_from_payload(&payload.item);
                if prepared.managed_cli_mcp_completion
                    && let pioneer_protocol::TurnItem::DynamicToolCall {
                        outcome: Some(outcome),
                        ..
                    } = &payload.item
                    && outcome.error_class == Some(pioneer_protocol::ToolErrorClass::Cancelled)
                {
                    status = TURN_ITEM_STATUS_CANCELLED;
                    attempt_status = TurnItemAttemptStatus::Cancelled;
                }
                prepared.item_terminal_attempt_status = Some(attempt_status);
                if attempt_status_from_payload(&payload.item) == TurnItemAttemptStatus::Failed {
                    prepared.item_attempt_failure_reason = Some(format!(
                        "item `{}` completed with failed status",
                        payload.item.item_id()
                    ));
                    if prepared.managed_cli_mcp_completion
                        && let pioneer_protocol::TurnItem::DynamicToolCall {
                            outcome: Some(outcome),
                            ..
                        } = &payload.item
                        && outcome.incomplete_reason.is_some()
                    {
                        prepared.item_attempt_failure_reason = outcome.incomplete_reason.clone();
                    }
                }
                prepared.item = Some(
                    pioneer_crud_conversations::turn_item::prepare_turn_item_projection(
                        payload.turn_id.as_str(),
                        &payload.item,
                        Some(status),
                        created_at,
                        created_at,
                    )?,
                );
                prepared.self_improvement_source =
                    self_improvement_source_turn::prepare_completed_collaborative_source_exchange(
                        db, event_id, created_at, payload,
                    )
                    .await?;
            }
            TurnEventPayload::ItemUpdated(payload) => {
                let status = terminal_turn_item_status_from_payload(&payload.item);
                prepared.item = Some(
                    pioneer_crud_conversations::turn_item::prepare_turn_item_projection(
                        payload.turn_id.as_str(),
                        &payload.item,
                        Some(status),
                        created_at,
                        created_at,
                    )?,
                );
            }
            TurnEventPayload::TurnMessageEdited(payload) => {
                prepared.input = Some(
                    crate::store::conversations::turn_writes::prepare_turn_input_projection(
                        payload.turn.id.as_str(),
                        payload.input.as_slice(),
                        created_at,
                    )?,
                );
                prepared.message_mutation = Some(
                    prepare_turn_message_mutation(
                        db,
                        TurnMessageMutationProjection::Edit(payload),
                        created_at,
                    )
                    .await?,
                );
            }
            TurnEventPayload::TurnMessageDeleted(payload) => {
                prepared.input = Some(
                    crate::store::conversations::turn_writes::prepare_turn_input_projection(
                        payload.turn.id.as_str(),
                        &[],
                        created_at,
                    )?,
                );
                prepared.message_mutation = Some(
                    prepare_turn_message_mutation(
                        db,
                        TurnMessageMutationProjection::Delete(payload),
                        created_at,
                    )
                    .await?,
                );
            }
            TurnEventPayload::TurnCompleted(payload) => {
                prepared.status_history = Some(
                    pioneer_crud_conversations::turn_writes::prepare_turn_status_history(
                        payload.turn.id.as_str(),
                        payload.turn.status,
                        payload.turn.error.clone(),
                        created_at,
                    ),
                );
                prepared.self_improvement_source =
                    self_improvement_source_turn::prepare_completed_source_turn(
                        db, event_id, created_at, payload,
                    )
                    .await?;
                let turn_upsert =
                    pioneer_crud_conversations::turn_writes::prepare_projected_turn_upsert(
                        db,
                        payload.turn.id.as_str(),
                        payload.thread_id.as_str(),
                        &payload.turn,
                        None,
                        None,
                        None,
                        None,
                        false,
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
                prepared.status_history = Some(
                    pioneer_crud_conversations::turn_writes::prepare_turn_status_history(
                        payload.turn.id.as_str(),
                        payload.turn.status,
                        payload.turn.error.clone(),
                        created_at,
                    ),
                );
                let turn_upsert =
                    pioneer_crud_conversations::turn_writes::prepare_projected_turn_upsert(
                        db,
                        payload.turn.id.as_str(),
                        payload.thread_id.as_str(),
                        &payload.turn,
                        None,
                        None,
                        None,
                        None,
                        false,
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
                prepared.status_history = Some(
                    pioneer_crud_conversations::turn_writes::prepare_turn_status_history(
                        payload.turn.id.as_str(),
                        payload.turn.status,
                        payload.turn.error.clone(),
                        created_at,
                    ),
                );
                let turn_upsert =
                    pioneer_crud_conversations::turn_writes::prepare_projected_turn_upsert(
                        db,
                        payload.turn.id.as_str(),
                        payload.thread_id.as_str(),
                        &payload.turn,
                        None,
                        None,
                        None,
                        None,
                        false,
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
        prepared.timeline_item = prepared
            .item
            .as_ref()
            .map(|item| serde_json::from_str(item.payload_json()))
            .transpose()?;
        Ok(prepared)
    }

    pub fn project_prepared<'a>(
        &'a self,
        db: &'a sea_orm::DatabaseTransaction,
        event: &'a AppendedTurnEvent,
        prepared: PreparedTurnProjection,
    ) -> ProjectFuture<'a> {
        Box::pin(async move {
            let created_at = event.created_at;
            if pioneer_crud_execution::cli_mcp_completion::cli_mcp_observation_is_superseded(
                db,
                &event.payload,
                Some(event.sequence),
            )
            .await?
            {
                return Ok(());
            }
            if let Some(mut observation) = prepared.liveness {
                observation.activity_sequence = event.sequence;
                turn_liveness::observe_activity(db, observation).await?;
            }
            let item_projection = prepared.item;
            let item_attempt_payload_json = prepared.item_attempt_payload_json;
            let item_attempt_id = prepared.item_attempt_id;
            let item_attempt_failure_reason = prepared.item_attempt_failure_reason;
            let managed_cli_mcp_completion = prepared.managed_cli_mcp_completion;
            let item_terminal_attempt_status = prepared.item_terminal_attempt_status;
            let input_projection = prepared.input;
            let terminal_running_attempts = prepared.terminal_running_attempts;
            let message_mutation = prepared.message_mutation;
            let turn_upsert = prepared.turn_upsert;
            let thread_preview_author_json = prepared.thread_preview_author_json;
            let message_preview_author_json = prepared.message_preview_author_json;
            let message_thread_preview = prepared.message_thread_preview;
            let task_run_parent_thread = prepared.task_run_parent_thread;
            let self_improvement_source = prepared.self_improvement_source;
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
                        status_history.context("turn/start status history was not prepared")?,
                    ),
                ),
                TurnEventPayload::ItemStarted(payload) => project_future(async move {
                    pioneer_crud_conversations::turn_item::upsert_prepared_turn_item(
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
                    let attempt_status = item_terminal_attempt_status
                        .context("item/completed attempt status was not prepared")?;
                    if managed_cli_mcp_completion {
                        pioneer_crud_execution::cli_mcp_completion::fence_projected_completion_attempt(
                            db,
                            &payload.turn_id,
                            payload.item.item_id(),
                            attempt_status,
                            item_attempt_failure_reason.clone(),
                            event.sequence,
                            created_at,
                        )
                        .await?;
                    }
                    pioneer_crud_conversations::turn_item::upsert_prepared_turn_item(
                        db,
                        item_projection.context("item/completed projection was not prepared")?,
                    )
                    .await?;

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
                        self_improvement_source_turn::apply_prepared_source_turn(db, source)
                            .await?;
                    }

                    Ok(())
                }),
                TurnEventPayload::ItemUpdated(_payload) => project_future(async move {
                    let mut item =
                        item_projection.context("item/updated projection was not prepared")?;
                    item.preserve_uncommitted_source_status(db).await?;
                    pioneer_crud_conversations::turn_item::upsert_prepared_turn_item(db, item).await
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
                    project_future(project_turn_message_mutation(
                        db,
                        payload.thread_id.as_str(),
                        payload.turn.id.as_str(),
                        payload.turn.message_revision,
                        message_mutation.context("message edit projection was not prepared")?,
                        {
                            let input = input_projection
                                .context("message edit input projection was not prepared")?;
                            move || {
                                crate::store::conversations::turn_writes::replace_prepared_turn_input(db, input)
                            }
                        },
                    ))
                }
                TurnEventPayload::TurnMessageDeleted(payload) => {
                    project_future(project_turn_message_mutation(
                        db,
                        payload.thread_id.as_str(),
                        payload.turn.id.as_str(),
                        payload.turn.message_revision,
                        message_mutation.context("message delete projection was not prepared")?,
                        {
                            let input = input_projection
                                .context("message delete input projection was not prepared")?;
                            move || {
                                crate::store::conversations::turn_writes::replace_prepared_turn_input(db, input)
                            }
                        },
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
                        self_improvement_source_turn::apply_prepared_source_turn(db, source)
                            .await?;
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
                    .await?;
                    turn_finalization::delete_prepared_by_turn_id(db, payload.turn.id.as_str())
                        .await?;
                    Ok(())
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
                    .await?;
                    turn_finalization::delete_prepared_by_turn_id(db, payload.turn.id.as_str())
                        .await?;
                    Ok(())
                }),
            };
            future.await
        })
    }

    #[cfg(test)]
    pub async fn project(
        &self,
        db: impl Into<pioneer_postgres_pools::PostgresDatabase>,
        event: &AppendedTurnEvent,
    ) -> Result<()> {
        use sea_orm::TransactionTrait;

        let db = db.into();
        let prepared = self
            .prepare(&db, event.id.as_str(), &event.payload, event.created_at)
            .await?;
        let tx = db.begin().await?;
        self.project_prepared(&tx, event, prepared).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn project_turn_started<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
        event_sequence: i64,
        input_projection: crate::store::conversations::turn_writes::PreparedTurnInputProjection,
        turn_upsert: pioneer_crud_conversations::turn_writes::PreparedTurnUpsert,
        thread_preview_author_json: Option<String>,
        message_preview_author_json: Option<String>,
        message_thread_preview: String,
        task_run_parent_thread: Option<pioneer_protocol::Thread>,
        status_history: pioneer_crud_conversations::turn_writes::PreparedTurnStatusHistory,
    ) -> Result<()> {
        let actor = payload
            .actor
            .as_ref()
            .context("turn/start requires an actor")?;
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
        .await?;
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

    async fn project_turn_started_inner<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        payload: &TurnStartedEventPayload,
        actor: Option<&PersistedActorRef>,
        event_sequence: i64,
        turn_upsert: pioneer_crud_conversations::turn_writes::PreparedTurnUpsert,
        thread_preview_author_json: Option<String>,
        message_preview_author_json: Option<String>,
        message_thread_preview: String,
        task_run_parent_thread: Option<pioneer_protocol::Thread>,
    ) -> Result<()> {
        pioneer_crud_conversations::turn_projection::project_turn_started_identity(
            db,
            payload,
            event_sequence,
        )
        .await?;

        let thread_created_at = unix_to_datetime(payload.thread.created_at);
        let thread_updated_at = unix_to_datetime(payload.thread.updated_at);
        let is_task_run_occurrence = payload.turn.turn_kind == TurnKind::TaskRun;

        pioneer_crud_conversations::turn_projection::project_turn_started_thread(
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
            |thread_id| async move {
                pioneer_crud_tasks::task_child_producers::thread(db, &thread_id).await
            },
        )
        .await?;
        crate::store::conversations::turn_writes::apply_prepared_turn_upsert(db, turn_upsert)
            .await?;
        Ok(())
    }

    fn project_turn_started_input_and_status<'a, C: ConnectionTrait + Sync>(
        &'a self,
        db: &'a C,
        payload: &'a TurnStartedEventPayload,
        input_projection: crate::store::conversations::turn_writes::PreparedTurnInputProjection,
        thread_updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        is_task_run_occurrence: bool,
        status_history: pioneer_crud_conversations::turn_writes::PreparedTurnStatusHistory,
    ) -> ProjectFuture<'a> {
        project_future(async move {
            crate::store::conversations::turn_writes::replace_prepared_turn_input(
                db,
                input_projection,
            )
            .await?;

            pioneer_crud_conversations::turn_writes::append_prepared_turn_status_history(
                db,
                status_history,
            )
            .await?;

            if is_task_run_occurrence {
                pioneer_crud_conversations::turn_projection::project_thread_foreground_status(
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
        turn_upsert: pioneer_crud_conversations::turn_writes::PreparedTurnUpsert,
        status_history: pioneer_crud_conversations::turn_writes::PreparedTurnStatusHistory,
        append_status_history: bool,
    ) -> Result<()> {
        crate::store::conversations::turn_writes::apply_prepared_turn_upsert(db, turn_upsert)
            .await?;

        if append_status_history {
            pioneer_crud_conversations::turn_writes::append_prepared_turn_status_history(
                db,
                status_history,
            )
            .await?;
        }

        pioneer_crud_conversations::turn_projection::project_thread_foreground_status(
            db, thread_id, updated_at,
        )
        .await?;

        Ok(())
    }
}
