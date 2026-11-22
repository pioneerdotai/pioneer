use crate::store::events::event_preparation::{
    PreparedProjectedTurnEvent, TURN_EVENT_PROJECTION_LEASE_SECS,
    prepare_projected_turn_event_for_permanent_storage,
};
use crate::store::execution::recovery_settlement;
use crate::{
    ClaimedRecoveryTerminalizationRecord, CrudStore, GatewayControlWake,
    RecoveryTerminalCleanupPlan, RecoveryTerminalizationApplyOutcome,
    RecoveryTerminalizationClaimDisposition, terminal_cleanup,
};
use anyhow::{Context, Result};
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_conversations::turn_reads;
use pioneer_crud_execution::recovery_terminalization_outbox::{
    self, PermanentRecoveryTerminalizationError,
};
use pioneer_crud_execution::recovery_terminalization_plan::PreparedRecoveryTerminalizationOutcome;
use pioneer_crud_execution::recovery_values::recovery_job_status_from_db;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::{native_terminal_effect_outbox, recovery_job};
use pioneer_protocol::TurnStatus;
use sea_orm::TransactionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

impl CrudStore {
    pub async fn claim_due_recovery_terminalizations(
        &self,
        now_unix: i64,
        claim_lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<ClaimedRecoveryTerminalizationRecord>> {
        let control_wakes = vec![GatewayControlWake::Terminalization];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let native_change = self.connection.work_signal("maintenance.native").changing();
        let child_change = self.connection.work_signal("tasks.lifecycle").changing();
        let result = pioneer_crud_execution::recovery_terminalization_outbox::claim_records(
            &self.connection,
            now_unix,
            claim_lease_secs,
            limit,
            &recovery_settlement::ReleaseShellControls,
        )
        .await;
        if let Ok((_, released)) = &result {
            control_change.completed(false);
            native_change.completed(*released);
            child_change.completed(*released);
        }
        result.map(|(valid, _)| valid)
    }

    pub async fn fail_recovery_terminalization_claim(
        &self,
        record: &ClaimedRecoveryTerminalizationRecord,
        error: String,
        retry_at_unix: i64,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![GatewayControlWake::Terminalization];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let control_result = async {
            self.retry_write_on_conflict(|| async {
                recovery_terminalization_outbox::mark_failed(
                    &self.connection,
                    record.recovery_job_id.as_str(),
                    record.claim_token.as_str(),
                    error.clone(),
                    unix_to_datetime(retry_at_unix),
                    unix_to_datetime(now_unix),
                )
                .await
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    /// Classifies an exact claimed row before the Gateway performs any
    /// recovery-specific metadata reconstruction. Supersession and quarantine
    /// are committed under the same claim-token fence; the two non-terminal
    /// dispositions are revalidated by the later atomic apply transaction.
    pub async fn classify_claimed_recovery_terminalization(
        &self,
        record: &ClaimedRecoveryTerminalizationRecord,
        now_unix: i64,
    ) -> Result<RecoveryTerminalizationClaimDisposition> {
        let native_change = self.connection.work_signal("maintenance.native").changing();
        let child_change = self.connection.work_signal("tasks.lifecycle").changing();
        let result = pioneer_crud_execution::recovery_terminalization_outbox::classify_claim(
            &self.connection,
            record,
            now_unix,
            &recovery_settlement::ReleaseShellControls,
        )
        .await;
        if let Ok(value) = &result {
            let released = matches!(
                value,
                RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(_)
                    | RecoveryTerminalizationClaimDisposition::Quarantined { .. }
            );
            native_change.completed(released);
            child_change.completed(released);
        }
        result
    }

    /// Set-based form used by the recovery worker. A full 64-row stale batch
    /// is classified with one joined read and resolved with two fenced writes,
    /// rather than reconstructing metadata and issuing queries per row.
    pub async fn classify_claimed_recovery_terminalization_batch(
        &self,
        records: &[ClaimedRecoveryTerminalizationRecord],
        now_unix: i64,
    ) -> Result<
        Vec<(
            ClaimedRecoveryTerminalizationRecord,
            RecoveryTerminalizationClaimDisposition,
        )>,
    > {
        let native_change = self.connection.work_signal("maintenance.native").changing();
        let child_change = self.connection.work_signal("tasks.lifecycle").changing();
        let result = pioneer_crud_execution::recovery_terminalization_outbox::classify_claim_batch(
            &self.connection,
            records,
            now_unix,
            &recovery_settlement::ReleaseShellControls,
        )
        .await;
        if let Ok(value) = &result {
            let released = value.iter().any(|(_, disposition)| {
                matches!(
                    disposition,
                    RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(_)
                        | RecoveryTerminalizationClaimDisposition::Quarantined { .. }
                )
            });
            native_change.completed(released);
            child_change.completed(released);
        }
        result
    }

    async fn prepare_claimed_recovery_terminalization_commit(
        &self,
        record: &ClaimedRecoveryTerminalizationRecord,
        resume: Option<&pioneer_protocol::TurnBlockedResumeMetadata>,
        cleanup_plan: &RecoveryTerminalCleanupPlan,
        event_created_at: DateTimeWithTimeZone,
    ) -> Result<PreparedRecoveryTerminalizationOutcome<PreparedProjectedTurnEvent>> {
        pioneer_crud_execution::recovery_terminalization_plan::prepare_claimed_recovery_terminalization_commit(
            &self.connection,
            record,
            resume,
            cleanup_plan,
            event_created_at,
            |event, at| prepare_projected_turn_event_for_permanent_storage(&self.connection, event, at),
        ).await
    }

    /// Applies item and Turn terminal state and consumes the recovery outbox
    /// claim in the same transaction. A stale/resumed job cannot close a Turn.
    pub async fn apply_claimed_recovery_terminalization(
        &self,
        record: &ClaimedRecoveryTerminalizationRecord,
        resume: Option<pioneer_protocol::TurnBlockedResumeMetadata>,
        cleanup_plan: RecoveryTerminalCleanupPlan,
        event_timestamp_secs: i64,
    ) -> Result<RecoveryTerminalizationApplyOutcome> {
        let payload_change = terminal_cleanup::Change::payload(&self.connection);
        let control_wakes = vec![
            GatewayControlWake::Turn(record.turn_id.clone()),
            GatewayControlWake::Resources,
            GatewayControlWake::Actions,
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
            GatewayControlWake::Ledger,
            GatewayControlWake::NativeRetention,
        ];
        let receipt_keys = vec![record.turn_id.clone()];
        let receipt_change = self
            .connection
            .work_signal("maintenance.receipts")
            .changing();
        let changes = [
            "gateway.control",
            "gateway.cli",
            "projection",
            "compaction",
            "maintenance.html",
            "tasks.lifecycle",
        ]
        .map(|domain| self.control_work_change(domain, &control_wakes));
        let native_change = self.connection.work_signal("maintenance.native").changing();
        let child_change = self.connection.work_signal("tasks.lifecycle").changing();
        let control_result = async {
        let _release_wake = self.execution_heartbeats.release_wake();
        let record = record.clone();
        self.retry_write_on_conflict(|| async {
            let created_at = unix_to_datetime(event_timestamp_secs);
            let prepared = match self
                .prepare_claimed_recovery_terminalization_commit(
                    &record,
                    resume.as_ref(),
                    &cleanup_plan,
                    created_at,
                )
                .await
            {
                Ok(prepared) => prepared,
                Err(error) => {
                    let Some(permanent) =
                        error.downcast_ref::<PermanentRecoveryTerminalizationError>()
                    else {
                        return Err(error);
                    };
                    let reason_code = permanent.reason_code.to_owned();
                    let message = permanent.message.chars().take(4_096).collect::<String>();
                    let transaction = self.connection.begin().await.context(
                        "failed to begin recovery terminalization quarantine transaction",
                    )?;
                    let quarantined = recovery_settlement::quarantine_claim(
                        &transaction,
                        record.recovery_job_id.as_str(),
                        record.claim_token.as_str(),
                        reason_code.as_str(),
                        message.as_str(),
                        created_at,
                    )
                    .await;
                    match quarantined {
                        Ok(true) => {
                            transaction.commit().await.context(
                                "failed to commit recovery terminalization quarantine",
                            )?;
                            return Ok(RecoveryTerminalizationApplyOutcome::Quarantined {
                                reason_code,
                            });
                        }
                        Ok(false) => {
                            let _ = transaction.rollback().await;
                            anyhow::bail!(
                                "recovery terminalization claim was lost before quarantine"
                            );
                        }
                        Err(quarantine_error) => {
                            let _ = transaction.rollback().await;
                            return Err(quarantine_error.context(format!(
                                "failed to quarantine permanent recovery terminalization error: {error:#}"
                            )));
                        }
                    }
                }
            };
            let mut prepared = match prepared {
                PreparedRecoveryTerminalizationOutcome::Commit(prepared) => prepared,
                PreparedRecoveryTerminalizationOutcome::Superseded {
                    outbox_updated_at,
                    job_updated_at,
                    turn_updated_at,
                    current_status,
                } => {
                    let transaction = self.connection.begin().await.context(
                        "failed to begin superseded recovery terminalization transaction",
                    )?;
                    let result = async {
                        recovery_terminalization_outbox::require_claim_revision(
                            &transaction,
                            &record.recovery_job_id,
                            &record.claim_token,
                            outbox_updated_at,
                        )
                        .await?;
                        let job = recovery_job::find_job_by_id(
                            &transaction,
                            record.recovery_job_id.as_str(),
                        )
                        .await?
                        .context("recovery terminalization job disappeared")?;
                        if job.updated_at != job_updated_at {
                            anyhow::bail!(
                                "recovery terminalization is stale: job identity/status changed"
                            );
                        }
                        let turn_model =
                            turn_reads::find_turn_by_id(&transaction, record.turn_id.as_str())
                                .await?
                                .context("recovery terminalization Turn is missing")?;
                        if turn_model.updated_at != turn_updated_at
                            || turn_status_from_db(turn_model.status.as_str())
                                != Some(current_status)
                        {
                            anyhow::bail!(
                                "recovery terminalization target changed before supersession"
                            );
                        }
                        let reason = format!(
                            "superseded by authoritative terminal Turn status `{:?}`",
                            current_status
                        );
                        if !recovery_settlement::cancel_claim(
                            &transaction,
                            record.recovery_job_id.as_str(),
                            record.claim_token.as_str(),
                            reason.as_str(),
                            created_at,
                        )
                        .await?
                        {
                            anyhow::bail!(
                                "recovery terminalization claim was lost before supersession"
                            );
                        }
                        Ok::<(), anyhow::Error>(())
                    }
                    .await;
                    match result {
                        Ok(()) => {
                            transaction
                                .commit()
                                .await
                                .context("failed to commit superseded recovery terminalization")?;
                            return Ok(
                                RecoveryTerminalizationApplyOutcome::SupersededByTerminalStatus(
                                    current_status,
                                ),
                            );
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(error);
                        }
                    }
                }
            };
            // Supplemental cleanup rows are non-runnable until the canonical
            // terminal event activates them. Persist that bounded preparation
            // before the atomic terminal transaction, then rebuild the exact
            // activation fence from the resulting reader-visible state.
            let preparation_tx = self.connection.begin().await
                .context("failed to begin recovery terminal cleanup preparation")?;
            native_terminal_effect_outbox::prepare_supplemental(
                &preparation_tx,
                prepared.terminal_effects.clone(),
                created_at,
            )
            .await
            .context("failed to prepare recovery terminal cleanup")?;
            preparation_tx.commit().await
                .context("failed to commit recovery terminal cleanup preparation")?;
            if let Some(terminal_event) = prepared.terminal_event.as_mut() {
                terminal_event.terminal_effect_activation = Some(
                    native_terminal_effect_outbox::prepare_activation_for_terminal(
                        &self.connection,
                        record.turn_id.as_str(),
                    )
                    .await
                    .context("failed to refresh recovery terminal cleanup activation")?,
                );
            }
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin recovery terminalization transaction")?;
            let result = async {
                pioneer_crud_conversations::turn_event::lock_stream(&transaction, &record.turn_id).await?;
                recovery_terminalization_outbox::require_claim_revision(
                    &transaction,
                    &record.recovery_job_id,
                    &record.claim_token,
                    prepared.outbox_updated_at,
                )
                .await?;
                let job =
                    recovery_job::find_job_by_id(&transaction, record.recovery_job_id.as_str())
                        .await?
                        .context("recovery terminalization job disappeared")?;
                if job.updated_at != prepared.job_updated_at
                    || recovery_job_status_from_db(&job.status) != Some(record.recovery_status)
                {
                    anyhow::bail!("recovery terminalization is stale: job identity/status changed");
                }
                let turn_model = turn_reads::find_turn_by_id(&transaction, record.turn_id.as_str())
                    .await?
                    .context("recovery terminalization Turn is missing")?;
                let thread_model =
                    pioneer_crud_conversations::thread_metadata::find_thread_by_id(&transaction, turn_model.thread_id.as_str())
                        .await?
                        .context("recovery terminalization Thread is missing")?;
                if turn_model.updated_at != prepared.turn_updated_at
                    || thread_model.id != prepared.thread_id
                    || thread_model.workspace_id != prepared.workspace_id
                    || (prepared.terminal_event.is_some()
                        && turn_status_from_db(&turn_model.status) != Some(TurnStatus::InProgress))
                {
                    anyhow::bail!(
                        "recovery terminalization target changed during commit preparation"
                    );
                }
                let claim_expires_at = unix_to_datetime(
                    event_timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS),
                );
                // Compatibility workers from an older Gateway may have
                // committed the Turn failure while losing the synthetic item
                // completion. Repair that half-state even when the desired
                // Turn status is already present, then consume the outbox in
                // the same transaction.
                if prepared.revalidate_item {
                    let current_item = pioneer_crud_conversations::turn_item::find_turn_item_metadata(
                        &transaction,
                        record.turn_id.as_str(),
                        record.item_id.as_str(),
                    )
                    .await?;
                    if current_item.as_ref().map(|item| item.updated_at) != prepared.item_updated_at
                    {
                        anyhow::bail!("recovery terminalization item changed during preparation");
                    }
                }
                if let Some(item_event) = prepared.item_event {
                    self.append_and_project_turn_event_in_transaction(
                        &transaction,
                        item_event,
                        created_at,
                        claim_expires_at,
                        true,
                    )
                    .await?;
                }
                if let Some(terminal_event) = prepared.terminal_event {
                    if prepared.stop_native_compaction {
                        pioneer_crud_compaction::stop_execution::stop_native_operations_for_no_progress(
                            &transaction,
                            &prepared.workspace_id,
                            &prepared.thread_id,
                            &record.turn_id,
                        )
                        .await?;
                    }
                    self.append_and_project_turn_event_in_transaction(
                        &transaction,
                        terminal_event,
                        created_at,
                        claim_expires_at,
                        true,
                    )
                    .await?;
                }
                if !recovery_settlement::mark_delivered(
                    &transaction,
                    record.recovery_job_id.as_str(),
                    record.claim_token.as_str(),
                    unix_to_datetime(event_timestamp_secs),
                )
                .await?
                {
                    anyhow::bail!("recovery terminalization claim was lost before commit");
                }
                Ok(prepared.applied)
            }
            .await;
            match result {
                Ok(applied) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit recovery terminalization transaction")?;
                    Ok(RecoveryTerminalizationApplyOutcome::Applied(applied))
                }
                Err(error) => {
                    Err(crate::store::connection::rollback_preserving_error(transaction, error).await)
                }
            }
        })
        .await
        }.await;
        if let Ok(value) = &control_result {
            let published = matches!(value, RecoveryTerminalizationApplyOutcome::Applied(applied) if applied.newly_terminal_turn.is_some() || applied.final_item.is_some());
            payload_change
                .complete(&self.connection, published, &receipt_keys)
                .await;
            receipt_change.completed_keys(if published { &receipt_keys } else { &[] });
            for change in changes {
                change.completed(published);
            }
            // Every successful disposition resolves the recovery episode,
            // which can release a durable native-event cleanup dependency.
            native_change.completed(true);
            child_change.completed(true);
        }
        control_result
    }
}
