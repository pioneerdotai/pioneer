use crate::events::TurnEventPayload;
use crate::store::events::event_preparation::{
    TURN_EVENT_PROJECTION_LEASE_SECS, prepare_projected_turn_event_for_permanent_storage,
};
use crate::{
    CrudStore, GatewayControlWake, PrepareTurnFinalizationOutcome, TurnWorkOwner, terminal_cleanup,
};
use anyhow::{Context, Result};
use pioneer_crud_access::turn_execution::{
    turn_kind_from_db, turn_origin_from_db, turn_status_from_db,
};
use pioneer_crud_conversations::protocol_projection::{
    parse_turn_permission_profile, parse_turn_prompt_manifest,
};
use pioneer_crud_conversations::thread_metadata;
use pioneer_crud_conversations::turn_finalization;
use pioneer_crud_conversations::turn_item_events::{
    is_agent_diff_updated_item, latest_agent_diff_raw_payload_for_item,
};
use pioneer_crud_conversations::turn_reads;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::turn_execution_window;
use pioneer_crud_tasks::finalization_revision::task_finalization_revision_for_connection;
use pioneer_protocol::{Turn, TurnStatus};
use sea_orm::TransactionTrait;
use std::fmt;

/// Safe operation boundary used when reporting PostgreSQL storage failures.
#[derive(Debug, Clone, Copy)]
pub enum NativeFinalizationPhase {
    Scan,
    Preparation,
    WriterBegin,
    WriterApply,
    WriterCommit,
}

impl NativeFinalizationPhase {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Scan => "finalization_scan",
            Self::Preparation => "finalization_preparation",
            Self::WriterBegin => "finalization_writer_begin",
            Self::WriterApply => "finalization_writer_apply",
            Self::WriterCommit => "finalization_writer_commit",
        }
    }
}

impl fmt::Display for NativeFinalizationPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommittedTurnFinalization {
    pub final_item: pioneer_protocol::ItemCompletedNotification,
    pub turn_completed: pioneer_protocol::TurnCompletedNotification,
    pub already_committed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TurnFinalizationCommitOutcome {
    Committed(CommittedTurnFinalization),
    DeferredByPredecessor,
    SupersededByTerminalStatus(TurnStatus),
}

#[derive(Debug, Clone, Default)]
pub struct TurnFinalizationReconciliationSummary {
    /// Selected intents which failed; successful commits are still returned.
    pub failures: Vec<std::sync::Arc<anyhow::Error>>,
    pub pass_complete: bool,
    pub pass_failed: bool,
    pub scanned: u64,
    pub committed: Vec<CommittedTurnFinalization>,
    pub deferred_by_predecessor: u64,
    pub superseded_by_terminal_status: u64,
}

// Preserve value comparison for callers of the existing summary API. Error
// chains have no native PartialEq; compare their contextual display values.
impl PartialEq for TurnFinalizationReconciliationSummary {
    fn eq(&self, other: &Self) -> bool {
        self.scanned == other.scanned
            && self.committed == other.committed
            && self.deferred_by_predecessor == other.deferred_by_predecessor
            && self.superseded_by_terminal_status == other.superseded_by_terminal_status
            && self.pass_complete == other.pass_complete
            && self.pass_failed == other.pass_failed
            && self.failures.len() == other.failures.len()
            && self
                .failures
                .iter()
                .zip(&other.failures)
                .all(|(left, right)| format!("{left:#}") == format!("{right:#}"))
    }
}

impl TurnFinalizationReconciliationSummary {
    pub fn committed_count(&self) -> u64 {
        u64::try_from(self.committed.len()).unwrap_or(u64::MAX)
    }

    pub fn newly_committed_count(&self) -> u64 {
        u64::try_from(
            self.committed
                .iter()
                .filter(|committed| !committed.already_committed)
                .count(),
        )
        .unwrap_or(u64::MAX)
    }
}

impl CrudStore {
    /// Captures one accepted final provider response before the execution owner
    /// may report success. Exact replay is idempotent; a different response for
    /// the same Turn/generation is rejected.
    pub async fn prepare_turn_finalization(
        &self,
        notification: &pioneer_protocol::ItemCompletedNotification,
        generation: i64,
        expected_task_finalization_revision: Option<&str>,
        event_timestamp_secs: i64,
    ) -> Result<PrepareTurnFinalizationOutcome> {
        let intent = turn_finalization::PreparedIntent::new(notification, generation)?;
        let cleanup_turns = vec![notification.turn_id.clone()];
        let cleanup_change = terminal_cleanup::Change::events(&self.connection);
        let changes = ["projection", "compaction"].map(|domain| {
            let change = self.connection.work_signal(domain).changing();
            if domain == "projection" {
                change.with_keys(
                    &cleanup_turns
                        .iter()
                        .map(|id| format!("turn:{id}"))
                        .collect::<Vec<_>>(),
                )
            } else {
                change
            }
        });
        let notification = notification.clone();
        let expected_task_finalization_revision =
            expected_task_finalization_revision.map(str::to_owned);
        let result = self.retry_write_on_conflict(|| async {
            let tx = self
                .connection
                .begin()
                .await
                .context("failed to begin turn finalization preparation transaction")?;
            let result = async {
                pioneer_crud_conversations::turn_event::lock_stream(&tx, &notification.turn_id)
                    .await?;
                let turn = turn_reads::find_turn_by_thread_and_id(
                    &tx,
                    notification.thread_id.as_str(),
                    notification.turn_id.as_str(),
                )
                .await?
                .with_context(|| {
                    format!(
                        "turn `{}` does not exist for finalization",
                        notification.turn_id
                    )
                })?;
                let status = turn_status_from_db(turn.status.as_str())
                    .context("turn finalization target has an unknown status")?;
                if !matches!(status, TurnStatus::InProgress | TurnStatus::Completed) {
                    anyhow::bail!(
                        "turn `{}` cannot prepare finalization from status `{:?}`",
                        notification.turn_id,
                        status
                    );
                }

                let existing = turn_finalization::find_by_turn_id(
                    &tx,
                    notification.turn_id.as_str(),
                )
                .await?;
                if status == TurnStatus::Completed && existing.is_none() {
                    anyhow::bail!(
                        "completed turn `{}` has no existing finalization intent",
                        notification.turn_id
                    );
                }

                let thread = thread_metadata::find_thread_by_id(&tx, notification.thread_id.as_str())
                    .await?
                    .with_context(|| {
                        format!(
                            "thread `{}` does not exist for finalization",
                            notification.thread_id
                        )
                    })?;
                if thread.workspace_id != notification.workspace_id {
                    anyhow::bail!(
                        "turn finalization workspace does not match authoritative thread"
                    );
                }

                // An exact replay of an already prepared intent has already
                // crossed the task gate. Let the repository verify the item
                // identity without rejecting it because task projections
                // legitimately advanced after that barrier.
                if existing.is_none() {
                    let current = task_finalization_revision_for_connection(
                        &tx,
                        notification.thread_id.as_str(),
                        notification.turn_id.as_str(),
                    )
                    .await?;
                    match expected_task_finalization_revision.as_deref() {
                        Some(expected) if expected == current.token => {}
                        Some(expected) => anyhow::bail!(
                            "attached-task finalization revision changed before commit: expected `{expected}`, current `{}`",
                            current.token
                        ),
                        None if current.attached_task_count == 0 => {}
                        None => anyhow::bail!(
                            "turn `{}` owns {} attached tasks but finalization has no task revision fence",
                            notification.turn_id,
                            current.attached_task_count
                        ),
                    }
                }

                turn_finalization::prepare(&tx, &intent, unix_to_datetime(event_timestamp_secs))
                .await
            }
            .await;

            match result {
                Ok(outcome) => {
                    tx.commit()
                        .await
                        .context("failed to commit turn finalization preparation transaction")?;
                    Ok(outcome)
                }
                Err(error) => {
                    let _ = tx.rollback().await;
                    Err(error)
                }
            }
        })
        .await;
        if let Ok(value) = &result {
            let changed = matches!(value, PrepareTurnFinalizationOutcome::Prepared);
            cleanup_change
                .complete(&self.connection, changed, &cleanup_turns)
                .await;
            for change in changes {
                change.completed(changed);
            }
        }
        result
    }

    pub async fn list_prepared_turn_finalization_ids(&self, limit: u64) -> Result<Vec<String>> {
        turn_finalization::list_prepared_ids(&self.connection, limit).await
    }

    /// Returns whether this Turn participates in the atomic execution-success
    /// finalization protocol. Native providers and CLI runtimes prepare an
    /// intent before publishing success; only older workers retain the legacy
    /// terminal event path during expand/migrate/contract rollout.
    pub async fn has_turn_finalization_intent(&self, turn_id: &str) -> Result<bool> {
        turn_finalization::exists(&self.connection, turn_id).await
    }

    pub async fn reconcile_prepared_turn_finalizations(
        &self,
        limit: u64,
        event_timestamp_secs: i64,
    ) -> Result<TurnFinalizationReconciliationSummary> {
        if limit == 0 {
            return Ok(TurnFinalizationReconciliationSummary::default());
        }
        // Serialize only this domain owner's cursor, never database capacity.
        // One bounded eligible page per call; failures advance too, then retry
        // after the pass wraps. Discovery errors leave the cursor unchanged.
        let mut pass = self.finalization_cursor.lock().await;
        if pass.upper.is_none() {
            pass.upper = turn_finalization::prepared_upper(&self.connection)
                .await
                .context(NativeFinalizationPhase::Scan)?;
        }
        let Some(upper) = pass.upper.as_ref() else {
            return Ok(TurnFinalizationReconciliationSummary {
                pass_complete: true,
                ..Default::default()
            });
        };
        let page = turn_finalization::list_eligible_prepared(
            &self.connection,
            pass.after.as_ref(),
            upper,
            limit,
        )
        .await
        .context(NativeFinalizationPhase::Scan)?;
        let complete = (page.len() as u64) < limit;
        let last = page.last().cloned();
        let mut summary = self
            .reconcile_finalization_candidates(page, event_timestamp_secs)
            .await;
        pass.failed |= !summary.failures.is_empty();
        summary.pass_complete = complete;
        summary.pass_failed = pass.failed;
        if complete {
            *pass = turn_finalization::PreparedPass::default();
        } else {
            pass.after = last;
        }
        Ok(summary)
    }

    /// No global cursor is moved by a keyed wake. Eligibility is advisory;
    /// commit revalidates intent, Turn state and predecessor in its transaction.
    pub async fn reconcile_prepared_turn_finalizations_for_turns(
        &self,
        turn_ids: &[String],
        event_timestamp_secs: i64,
    ) -> Result<TurnFinalizationReconciliationSummary> {
        anyhow::ensure!(
            turn_ids.len() <= 64,
            "finalization frontier exceeds 64 Turns"
        );
        let page = turn_finalization::list_eligible_prepared_for_turns(&self.connection, turn_ids)
            .await
            .context(NativeFinalizationPhase::Scan)?;
        let mut summary = self
            .reconcile_finalization_candidates(page, event_timestamp_secs)
            .await;
        summary.pass_complete = true;
        summary.pass_failed = !summary.failures.is_empty();
        Ok(summary)
    }

    async fn reconcile_finalization_candidates(
        &self,
        page: Vec<turn_finalization::PreparedCursor>,
        event_timestamp_secs: i64,
    ) -> TurnFinalizationReconciliationSummary {
        let mut summary = TurnFinalizationReconciliationSummary::default();
        for candidate in page {
            summary.scanned = summary.scanned.saturating_add(1);
            match self
                .commit_prepared_turn_finalization(candidate.turn_id.as_str(), event_timestamp_secs)
                .await
            {
                Ok(TurnFinalizationCommitOutcome::Committed(committed)) => {
                    summary.committed.push(committed);
                }
                Ok(TurnFinalizationCommitOutcome::DeferredByPredecessor) => {
                    summary.deferred_by_predecessor =
                        summary.deferred_by_predecessor.saturating_add(1);
                }
                Ok(TurnFinalizationCommitOutcome::SupersededByTerminalStatus(_)) => {
                    summary.superseded_by_terminal_status =
                        summary.superseded_by_terminal_status.saturating_add(1);
                }
                Err(error) => summary.failures.push(std::sync::Arc::new(error)),
            }
        }
        summary
    }

    /// Atomically publishes the prepared final AgentMessage and immutable
    /// Completed Turn state. This is the sole successful native finalization
    /// commit boundary.
    pub async fn commit_prepared_turn_finalization(
        &self,
        turn_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<TurnFinalizationCommitOutcome> {
        let payload_change = terminal_cleanup::Change::payload(&self.connection);
        let control_wakes = vec![
            GatewayControlWake::Turn(turn_id.into()),
            GatewayControlWake::Resources,
            GatewayControlWake::Ledger,
            GatewayControlWake::NativeRetention,
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let receipt_keys = vec![turn_id.to_owned()];
        let receipt_change = self
            .connection
            .work_signal("maintenance.receipts")
            .changing();
        let changes = [
            "gateway.control",
            "gateway.cli",
            "projection",
            "compaction",
            "maintenance.native",
            "maintenance.html",
        ]
        .map(|domain| self.control_work_change(domain, &control_wakes));
        let task_change = pioneer_crud_tasks::work_change::TaskTurnChange::begin(
            &self.connection,
            vec![turn_id.to_owned()],
        )
        .await?;
        let result = async {
            let _release_wake = self.execution_heartbeats.release_wake();
            let turn_id = turn_id.to_owned();
            self.retry_write_on_conflict(|| async {
                // Decode the prepared item and materialize both canonical event
                // payloads through the read contour. The writer transaction below
                // revalidates their durable identities before committing them.
                let prepared_intent =
                    turn_finalization::find_by_turn_id(&self.connection, turn_id.as_str())
                        .await
                        .context(NativeFinalizationPhase::Preparation)?
                        .with_context(|| format!("turn `{turn_id}` has no finalization intent"))?;
                let prepared_turn = turn_reads::find_turn_by_thread_and_id(
                    &self.connection,
                    prepared_intent.thread_id.as_str(),
                    turn_id.as_str(),
                )
                .await
                .context(NativeFinalizationPhase::Preparation)?
                .with_context(|| format!("turn `{turn_id}` disappeared during finalization"))?;
                // Advisory only: do not decode/render the final answer or prepare
                // two events for a prefix which cannot yet commit. Terminal Turns
                // still reach authoritative supersession/idempotent reconciliation.
                // The writer below repeats the prefix proof: a concurrent append
                // after this read must not allow finalization to overtake it.
                if prepared_intent.status == turn_finalization::STATUS_PREPARED
                    && prepared_turn.status == "in_progress"
                    && pioneer_crud_conversations::event_projection::has_unprojected_event(
                        &self.connection,
                        turn_id.as_str(),
                    )
                    .await
                    .context(NativeFinalizationPhase::Preparation)?
                {
                    return Ok((TurnFinalizationCommitOutcome::DeferredByPredecessor, false));
                }
                let final_item = turn_finalization::notification_from_model(&prepared_intent)?;
                let final_item_payload = pioneer_crud_conversations::turn_payload::PreparedItemPayload::new(prepared_intent.item_json.clone());
                let prepared_thread =
                    thread_metadata::find_thread_by_id(&self.connection, prepared_intent.thread_id.as_str())
                        .await
                        .context(NativeFinalizationPhase::Preparation)?
                        .with_context(|| {
                            format!(
                                "thread `{}` disappeared during finalization",
                                prepared_intent.thread_id
                            )
                        })?;
                if prepared_thread.workspace_id != prepared_intent.workspace_id {
                    anyhow::bail!("turn finalization workspace identity changed");
                }
                let prompt_manifest = parse_turn_prompt_manifest(&prepared_turn)?;
                let permission_profile = parse_turn_permission_profile(&prepared_turn)?;
                let collaboration = pioneer_crud_conversations::collaboration::collaboration_from_model(&prepared_turn)?;
                let completed_turn = Turn {
                    work_owner: TurnWorkOwner::from_db_str(&prepared_turn.work_owner)
                        .map_err(anyhow::Error::msg)?,
                    id: prepared_turn.id.clone(),
                    status: TurnStatus::Completed,
                    turn_kind: turn_kind_from_db(prepared_turn.turn_kind.as_str())
                        .unwrap_or_default(),
                    origin: turn_origin_from_db(prepared_turn.origin.as_str()).unwrap_or_default(),
                    mode: collaboration.mode.effective_mode,
                    author: collaboration.author,
                    reply_to_turn_id: collaboration.reply_to_turn_id,
                    mentions: collaboration.mentions,
                    message_revision: collaboration.message_revision,
                    message_deleted: collaboration.message_deleted,
                    error: None,
                    prompt_manifest,
                    permission_profile,
                };
                let turn_completed = pioneer_protocol::TurnCompletedNotification {
                    workspace_id: prepared_intent.workspace_id.clone(),
                    thread_id: prepared_intent.thread_id.clone(),
                    turn: completed_turn,
                };
                let prepared_item_event = prepare_projected_turn_event_for_permanent_storage(
                    &self.connection,
                    TurnEventPayload::ItemCompleted(final_item.clone()),
                    unix_to_datetime(event_timestamp_secs),
                )
                .await
                .context(NativeFinalizationPhase::Preparation)?;
                let prepared_completed_event = prepare_projected_turn_event_for_permanent_storage(
                    &self.connection,
                    TurnEventPayload::TurnCompleted(turn_completed.clone()),
                    unix_to_datetime(event_timestamp_secs),
                )
                .await
                .context(NativeFinalizationPhase::Preparation)?;

                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context(NativeFinalizationPhase::WriterBegin)?;
                let result = async {
                // Live completion and background recovery can publish the
                // same intent. Use the canonical stream lock before reading
                // the authoritative Turn or intent, just as event append does.
                pioneer_crud_conversations::turn_event::lock_stream(&transaction, &turn_id)
                    .await?;
                let intent = turn_finalization::find_by_turn_id(&transaction, turn_id.as_str())
                    .await?
                    .with_context(|| format!("turn `{turn_id}` has no finalization intent"))?;
                if intent.generation != prepared_intent.generation
                    || intent.item_id != prepared_intent.item_id
                    || intent.item_digest != prepared_intent.item_digest
                    || intent.thread_id != prepared_intent.thread_id
                    || intent.workspace_id != prepared_intent.workspace_id
                {
                    anyhow::bail!("turn finalization intent changed during commit preparation");
                }
                let turn_model = turn_reads::find_turn_by_thread_and_id(
                    &transaction,
                    intent.thread_id.as_str(),
                    turn_id.as_str(),
                )
                .await?
                .with_context(|| format!("turn `{turn_id}` disappeared during finalization"))?;
                let authoritative_status = turn_status_from_db(turn_model.status.as_str())
                    .context("turn finalization target has an unknown status")?;
                let thread_model =
                    thread_metadata::find_thread_by_id(&transaction, intent.thread_id.as_str())
                        .await?
                        .with_context(|| {
                            format!(
                                "thread `{}` disappeared during finalization",
                                intent.thread_id
                            )
                        })?;
                if thread_model.workspace_id != intent.workspace_id {
                    anyhow::bail!("turn finalization workspace identity changed");
                }
                if turn_model.thread_id != prepared_turn.thread_id
                    || turn_model.turn_kind != prepared_turn.turn_kind
                    || turn_model.origin != prepared_turn.origin
                    || thread_model.id != prepared_thread.id
                {
                    anyhow::bail!("turn finalization target changed during commit preparation");
                }

                let already_committed = authoritative_status == TurnStatus::Completed;
                if already_committed {
                    if intent.status != turn_finalization::STATUS_COMMITTED {
                        let existing_item = pioneer_crud_conversations::turn_item::find_turn_item_metadata(
                            &transaction,
                            turn_id.as_str(),
                            intent.item_id.as_str(),
                        )
                        .await?
                        .context("completed Turn is missing its final AgentMessage")?;
                        if !final_item_payload.matches(&transaction, &existing_item).await? {
                            anyhow::bail!(
                                "completed Turn final item conflicts with its finalization intent"
                            );
                        }
                        if !turn_finalization::mark_committed(
                            &transaction,
                            turn_id.as_str(),
                            intent.generation,
                            unix_to_datetime(event_timestamp_secs),
                        )
                        .await?
                        {
                            anyhow::bail!("failed to reconcile committed finalization intent");
                        }
                    }
                    let closed_windows = turn_execution_window::close_active_execution_windows_for_terminal_turn(
                        &transaction,
                        turn_id.as_str(),
                        unix_to_datetime(event_timestamp_secs),
                    )
                    .await?;
                    return Ok((TurnFinalizationCommitOutcome::Committed(
                        CommittedTurnFinalization {
                            final_item,
                            turn_completed,
                            already_committed: true,
                        },
                    ), intent.status != turn_finalization::STATUS_COMMITTED || closed_windows > 0));
                }
                if authoritative_status != TurnStatus::InProgress {
                    if intent.status != turn_finalization::STATUS_PREPARED {
                        anyhow::bail!(
                            "terminal Turn `{turn_id}` has non-prepared finalization state `{}`",
                            intent.status
                        );
                    }
                    if !turn_finalization::delete_prepared_by_turn_id(
                        &transaction,
                        turn_id.as_str(),
                    )
                    .await?
                    {
                        anyhow::bail!(
                            "failed to discard finalization superseded by terminal Turn `{turn_id}`"
                        );
                    }
                    return Ok((TurnFinalizationCommitOutcome::SupersededByTerminalStatus(
                        authoritative_status,
                    ), true));
                }
                if intent.status != turn_finalization::STATUS_PREPARED {
                    anyhow::bail!(
                        "in-progress Turn `{turn_id}` has non-prepared finalization state `{}`",
                        intent.status
                    );
                }

                // Existing durable events necessarily precede the two events
                // this transaction is about to append. The per-Turn watermark
                // is the authority for whether that prefix is fully projected.
                if pioneer_crud_conversations::event_projection::has_unprojected_event(
                    &transaction,
                    turn_id.as_str(),
                )
                .await?
                {
                    return Ok((TurnFinalizationCommitOutcome::DeferredByPredecessor, false));
                }

                let created_at = unix_to_datetime(event_timestamp_secs);
                let claim_expires_at = unix_to_datetime(
                    event_timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS),
                );
                self.append_and_project_turn_event_in_transaction(
                    &transaction,
                    prepared_item_event,
                    created_at,
                    claim_expires_at,
                    true,
                )
                .await?;
                self.append_and_project_turn_event_in_transaction(
                    &transaction,
                    prepared_completed_event,
                    created_at,
                    claim_expires_at,
                    true,
                )
                .await?;
                if !turn_finalization::mark_committed(
                    &transaction,
                    turn_id.as_str(),
                    intent.generation,
                    created_at,
                )
                .await?
                {
                    anyhow::bail!("turn finalization compare-and-set was lost");
                }
                // Both live completion and background reconciliation use this
                // transaction. Terminal projection and window closure must
                // commit together; a superseded live publisher may never run
                // its post-completion cleanup.
                turn_execution_window::close_active_execution_windows_for_terminal_turn(
                    &transaction,
                    turn_id.as_str(),
                    created_at,
                )
                .await?;
                Ok((TurnFinalizationCommitOutcome::Committed(
                    CommittedTurnFinalization {
                        final_item,
                        turn_completed,
                        already_committed: false,
                    },
                ), true))
            }
            .await
            .context(NativeFinalizationPhase::WriterApply);
                match result {
                    Ok((TurnFinalizationCommitOutcome::DeferredByPredecessor, _)) => {
                        let _ = transaction.rollback().await;
                        Ok((TurnFinalizationCommitOutcome::DeferredByPredecessor, false))
                    }
                    Ok(outcome) => {
                        transaction
                            .commit()
                            .await
                            .context(NativeFinalizationPhase::WriterCommit)?;
                        Ok(outcome)
                    }
                    Err(error) => Err(crate::store::connection::rollback_preserving_error(
                        transaction,
                        error,
                    )
                    .await),
                }
            })
            .await
        }
        .await;
        if let Ok((outcome, changed)) = &result {
            // Terminal status and its projection were committed atomically.
            // Mandatory consumers must observe that commit before any further
            // reads; payload eligibility needs no advisory post-commit SELECT.
            receipt_change.completed_keys(if *changed { &receipt_keys } else { &[] });
            for change in changes {
                change.completed(*changed);
            }
            let payload_changed = *changed
                && matches!(
                    outcome,
                    TurnFinalizationCommitOutcome::Committed(_)
                        | TurnFinalizationCommitOutcome::SupersededByTerminalStatus(
                            TurnStatus::Failed | TurnStatus::Interrupted
                        )
                );
            payload_change.completed_terminal(payload_changed, &receipt_keys);
            task_change.completed(&self.connection).await?;
        }
        result.map(|(outcome, _)| outcome)
    }

    pub async fn materialize_agent_diff_final_snapshot_if_changed(
        &self,
        notification: pioneer_protocol::ItemCompletedNotification,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        if !is_agent_diff_updated_item(&notification.item) {
            anyhow::bail!(
                "final diff snapshot expected agent_diff_updated item, got `{}`",
                notification.item.item_id()
            );
        }

        let item_payload_json = serde_json::to_string(&notification.item)
            .context("failed to serialize final agent diff snapshot payload")?;
        let latest_raw_payload = latest_agent_diff_raw_payload_for_item(
            &self.connection,
            notification.thread_id.as_str(),
            notification.turn_id.as_str(),
            notification.item.item_id(),
        )
        .await?;
        if latest_raw_payload.as_deref() == Some(item_payload_json.as_str()) {
            return Ok(false);
        }

        self.materialize_item_completed(notification, event_timestamp_secs)
            .await?;
        Ok(true)
    }

    pub async fn materialize_item_updated(
        &self,
        notification: pioneer_protocol::ItemUpdatedNotification,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.materialize_turn_event(
            TurnEventPayload::ItemUpdated(notification),
            event_timestamp_secs,
        )
        .await
    }
}
