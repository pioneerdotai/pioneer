//! Scoped execution recovery, diagnostic reads, and terminalization obligations.
use crate::attempt_supervision::TurnLivenessRecord;
use crate::cli_runtime_store::{
    CliRuntimeStore, ack_cli_runtime_recovery_outcome, cli_runtime_recovery_transition_allowed,
    native_recovery_start_transition_allowed,
};
use crate::gateway_control::{GatewayControlWake, control_work_change};
use crate::recovery_job::{self, RecoveryJobRecord, recovery_job_record_from_model};
use crate::recovery_terminalization_outbox;
use crate::recovery_values::{recovery_action_to_db, recovery_job_status_from_db};
use crate::retry::run_serialized_write;
use crate::time::unix_to_datetime;
use crate::turn_execution::{self, TurnExecutionRecord};
use crate::turn_item_attempt;
use anyhow::{Context, Result};
use pioneer_crud_conversations::{events::CanonicalTurnEventPayload, turn_liveness};
use pioneer_protocol::{
    ProviderFailureClass, ProviderFailureStage, RecoveryAction, RecoveryJobStatus, RecoveryTrigger,
    TurnItemType, TurnStatus,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait, prelude::DateTimeWithTimeZone,
};
use std::sync::Arc;
use tokio::sync::Notify;

/// A retained hint for the existing recovery_job consumer. The guard is
/// created before DB admission and dropped after the operation releases its
/// transaction/capacity, also on cancellation or an unknown commit outcome.
/// It does not acknowledge work or replace the durable fallback poll.
pub struct RecoveryChange(pub Arc<Notify>);
impl Drop for RecoveryChange {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[derive(Debug, Clone)]
pub enum AtomicRecoveryJobEnqueueOutcome {
    Created(RecoveryJobRecord),
    Reused(RecoveryJobRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimedRecoveryActivation {
    Activated,
    BlockedByActiveRecovery,
    ClaimNotFound,
}

pub struct RecoveryStore<'a> {
    connection: &'a SqliteDatabase,
    recovery_work: &'a Arc<Notify>,
    #[cfg(test)]
    watchdog_preparation_gate: std::sync::Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
}

impl<'a> RecoveryStore<'a> {
    pub fn new(connection: &'a SqliteDatabase, recovery_work: &'a Arc<Notify>) -> Self {
        Self {
            connection,
            recovery_work,
            #[cfg(test)]
            watchdog_preparation_gate: Default::default(),
        }
    }

    /// short writer transaction; CLI ownership proof remains independent.
    pub async fn apply_recovery_watchdog_failure(
        &self,
        expected: &RecoveryJobRecord,
        status: RecoveryJobStatus,
        next_run_at_unix: Option<i64>,
        last_error: Option<String>,
        diagnostic: Option<pioneer_protocol::RecoveryDiagnostic>,
        now_unix: i64,
        observed_liveness: Option<&TurnLivenessRecord>,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let changes = ["gateway.control", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let result = async {
            let Some(raw) =
                crate::recovery_watchdog::raw_policy(self.connection, &expected.id, None).await?
            else {
                return Ok(false);
            };
            // Interpret legacy values only outside DB capacity. Raw strings retain
            // unknown non-NULL values and formatting for exact commit revalidation.
            if !raw.agrees_with(expected) {
                return Ok(false);
            }
            // Keep exact persisted timestamps for the fence. Public watchdog records
            // expose seconds, while activity and legacy episode timestamps may have
            // fractions; reconstructing their SQL values would reject unchanged rows.
            let raw_liveness =
                turn_liveness::find_by_turn_id(self.connection, &expected.turn_id).await?;
            let agrees = match (observed_liveness, raw_liveness.as_ref()) {
                (None, None) => true,
                (Some(observed), Some(raw)) => {
                    observed.turn_id == raw.turn_id
                        && observed.thread_id == raw.thread_id
                        && observed.last_activity_sequence == raw.last_activity_sequence
                        && observed.last_activity_kind == raw.last_activity_kind
                        && observed.last_activity_item_id == raw.last_activity_item_id
                        && observed.last_activity_item_type == raw.last_activity_item_type
                        && observed.last_activity_at_unix == raw.last_activity_at.timestamp()
                }
                _ => false,
            };
            if !agrees {
                return Ok(false);
            }
            let diagnostic_json = diagnostic.as_ref().map(serde_json::to_string).transpose()?;
            let token = pioneer_protocol::generate_id(21);
            let cli_source = self
                .prepare_cli_runtime_recovery_outcome_source(&expected.id)
                .await?;
            #[cfg(test)]
            {
                let gate = self.watchdog_preparation_gate.lock().unwrap().take();
                if let Some((entered, release)) = gate {
                    entered.notify_one();
                    release.notified().await;
                }
            }
            run_serialized_write(|| async {
                let tx = self.connection.begin().await?;
                if !cli_runtime_recovery_transition_allowed(
                    &tx,
                    &expected.id,
                    cli_source.as_ref(),
                    None,
                )
                .await?
                    || !crate::recovery_watchdog::fence(
                        &tx,
                        expected,
                        &raw,
                        raw_liveness.as_ref(),
                        &token,
                    )
                    .await?
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                // SQL TEXT equality may use a case-insensitive collation. The
                // successful fence owns the job's write lock; a current locked read
                // now verifies bytes before any domain mutation. No JSON parsing.
                if crate::recovery_watchdog::raw_policy(&tx, &expected.id, Some(&token))
                    .await?
                    .as_ref()
                    != Some(&raw)
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                let applied = if let Some(attempt) = &expected.active_attempt_id {
                    if let Some(next) = next_run_at_unix {
                        recovery_job::mark_job_retrying(
                            &tx,
                            &expected.id,
                            attempt,
                            unix_to_datetime(next),
                            None,
                            last_error.clone(),
                            diagnostic_json.clone(),
                            unix_to_datetime(now_unix),
                        )
                        .await?
                    } else {
                        recovery_job::mark_job_terminal_after_attempt(
                            &tx,
                            &expected.id,
                            attempt,
                            status,
                            last_error.clone(),
                            unix_to_datetime(now_unix),
                        )
                        .await?
                    }
                } else {
                    recovery_job::mark_active_without_attempt_terminal(
                        &tx,
                        &expected.id,
                        status,
                        last_error.clone(),
                        unix_to_datetime(now_unix),
                    )
                    .await?
                };
                if !applied {
                    tx.rollback().await?;
                    return Ok(false);
                }
                if next_run_at_unix.is_none() {
                    if let Some(value) = &diagnostic_json {
                        recovery_job::set_diagnostic(&tx, &expected.id, value.clone()).await?;
                    }
                    enqueue_recovery_terminalization_if_required(
                        &tx,
                        &expected.id,
                        unix_to_datetime(now_unix),
                    )
                    .await?;
                }
                tx.commit().await?;
                Ok(true)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn count_recovery_jobs_for_turn(&self, turn_id: &str) -> Result<u64> {
        recovery_job::count_jobs_for_turn(self.connection, turn_id).await
    }

    pub async fn enqueue_recovery_job(
        &self,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        source_attempt_id: Option<String>,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        reason: Option<String>,
        error_class: Option<ProviderFailureClass>,
        transport_stage: Option<ProviderFailureStage>,
        retry_after_ms: Option<i64>,
        provider_attempt_number: i64,
        max_attempts: i64,
        policy_json: serde_json::Value,
        policy_snapshot: serde_json::Value,
        now_unix: i64,
    ) -> Result<RecoveryJobRecord> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let changes = ["gateway.control", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let result = async {
            run_serialized_write(|| async {
                let now = unix_to_datetime(now_unix);
                let retry_delay_secs = retry_after_ms
                    .map(|retry_after_ms| retry_after_ms.max(0).saturating_add(999) / 1_000)
                    .unwrap_or(0);
                let scheduled_at = unix_to_datetime(now_unix.saturating_add(retry_delay_secs));
                let policy_json = serde_json::to_string(&policy_json)
                    .context("failed to serialize recovery policy json")?;
                let policy_snapshot_json = serde_json::to_string(&policy_snapshot)
                    .context("failed to serialize recovery policy snapshot json")?;
                let row = recovery_job::enqueue_recovery_job(
                    self.connection,
                    recovery_job::NewRecoveryJob {
                        diagnostic_json: None,
                        turn_id: turn_id.clone(),
                        item_id: item_id.clone(),
                        item_type,
                        source_attempt_id: source_attempt_id.clone(),
                        trigger,
                        action,
                        reason: reason.clone(),
                        policy_json,
                        error_class,
                        transport_stage,
                        retry_after_ms,
                        provider_attempt_number,
                        policy_snapshot_json,
                        max_attempts,
                        created_at: now,
                        scheduled_at,
                        next_run_at: scheduled_at,
                    },
                )
                .await?;
                Ok(recovery_job_record_from_model(row))
            })
            .await
        }
        .await;
        if result.is_ok() {
            for change in changes {
                change.completed(true);
            }
        }
        result
    }

    pub async fn enqueue_recovery_job_if_no_unresolved(
        &self,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        source_attempt_id: Option<String>,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        reason: Option<String>,
        error_class: Option<ProviderFailureClass>,
        transport_stage: Option<ProviderFailureStage>,
        retry_after_ms: Option<i64>,
        provider_attempt_number: i64,
        max_attempts: i64,
        policy_json: serde_json::Value,
        policy_snapshot: serde_json::Value,
        now_unix: i64,
    ) -> Result<AtomicRecoveryJobEnqueueOutcome> {
        self.enqueue_recovery_job_with_diagnostic_if_no_unresolved(
            turn_id,
            item_id,
            item_type,
            source_attempt_id,
            trigger,
            action,
            reason,
            error_class,
            transport_stage,
            retry_after_ms,
            provider_attempt_number,
            max_attempts,
            policy_json,
            policy_snapshot,
            None,
            now_unix,
        )
        .await
    }

    pub async fn enqueue_recovery_job_with_diagnostic_if_no_unresolved(
        &self,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        source_attempt_id: Option<String>,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        reason: Option<String>,
        error_class: Option<ProviderFailureClass>,
        transport_stage: Option<ProviderFailureStage>,
        retry_after_ms: Option<i64>,
        provider_attempt_number: i64,
        max_attempts: i64,
        policy_json: serde_json::Value,
        policy_snapshot: serde_json::Value,
        diagnostic: Option<pioneer_protocol::RecoveryDiagnostic>,
        now_unix: i64,
    ) -> Result<AtomicRecoveryJobEnqueueOutcome> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let changes = ["gateway.control", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let result = async {
            let diagnostic_json = diagnostic.as_ref().map(serde_json::to_string).transpose()?;
            run_serialized_write(|| async {
                let tx = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin atomic recovery enqueue transaction")?;
                let result = async {
                    if let Some(existing) =
                        recovery_job::find_unresolved_job_by_turn(&tx, turn_id.as_str()).await?
                    {
                        return Ok(AtomicRecoveryJobEnqueueOutcome::Reused(
                            recovery_job_record_from_model(existing),
                        ));
                    }

                    let now = unix_to_datetime(now_unix);
                    let retry_delay_secs = retry_after_ms
                        .map(|retry_after_ms| retry_after_ms.max(0).saturating_add(999) / 1_000)
                        .unwrap_or(0);
                    let scheduled_at = unix_to_datetime(now_unix.saturating_add(retry_delay_secs));
                    let policy_json = serde_json::to_string(&policy_json)
                        .context("failed to serialize recovery policy json")?;
                    let policy_snapshot_json = serde_json::to_string(&policy_snapshot)
                        .context("failed to serialize recovery policy snapshot json")?;
                    let row = recovery_job::enqueue_recovery_job(
                        &tx,
                        recovery_job::NewRecoveryJob {
                            diagnostic_json: diagnostic_json.clone(),
                            turn_id: turn_id.clone(),
                            item_id: item_id.clone(),
                            item_type,
                            source_attempt_id: source_attempt_id.clone(),
                            trigger,
                            action,
                            reason: reason.clone(),
                            policy_json,
                            error_class,
                            transport_stage,
                            retry_after_ms,
                            provider_attempt_number,
                            policy_snapshot_json,
                            max_attempts,
                            created_at: now,
                            scheduled_at,
                            next_run_at: scheduled_at,
                        },
                    )
                    .await?;
                    Ok(AtomicRecoveryJobEnqueueOutcome::Created(
                        recovery_job_record_from_model(row),
                    ))
                }
                .await;
                match result {
                    Ok(outcome) => {
                        tx.commit()
                            .await
                            .context("failed to commit atomic recovery enqueue transaction")?;
                        Ok(outcome)
                    }
                    Err(error) => {
                        let _ = tx.rollback().await;
                        Err(error)
                    }
                }
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(matches!(
                    changed,
                    AtomicRecoveryJobEnqueueOutcome::Created(_)
                ));
            }
        }
        result
    }

    pub async fn mark_attempt_recovery_action(
        &self,
        attempt_id: &str,
        action: RecoveryAction,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            run_serialized_write(|| async {
                turn_item_attempt::mark_recovery_action(
                    self.connection,
                    attempt_id,
                    recovery_action_to_db(action),
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

    pub async fn claim_due_recovery_jobs(
        &self,
        now_unix: i64,
        claim_lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        let now = unix_to_datetime(now_unix);
        // Bounded advisory identities only; release the reader before writer
        // admission. A reschedule/lease change is revalidated at the UPDATE.
        let candidates = recovery_job::list_due_job_ids(self.connection, now, limit).await?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let claim_expires_at = i64::try_from(claim_lease_secs)
            .ok()
            .and_then(|secs| now_unix.checked_add(secs))
            .map(unix_to_datetime)
            .unwrap_or(now);
        let jobs = run_serialized_write(|| async {
            recovery_job::claim_due_job_ids(self.connection, &candidates, now, claim_expires_at)
                .await
        })
        .await?;
        Ok(jobs
            .into_iter()
            .map(recovery_job_record_from_model)
            .collect())
    }

    pub async fn next_recovery_job_due_at(&self) -> Result<Option<i64>> {
        recovery_job::next_due_at(self.connection).await
    }

    pub async fn list_due_pending_recovery_jobs_by_action(
        &self,
        action: RecoveryAction,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        let jobs = recovery_job::list_due_pending_jobs_by_action(
            self.connection,
            action,
            unix_to_datetime(now_unix),
            limit,
        )
        .await?;
        Ok(jobs
            .into_iter()
            .map(recovery_job_record_from_model)
            .collect())
    }

    pub async fn get_failed_turn_recovery_diagnostic(
        &self,
        turn_id: &str,
    ) -> Result<Option<pioneer_protocol::RecoveryDiagnostic>> {
        let Some((diagnostic, error)) =
            crate::recovery_diagnostic::failed(self.connection, turn_id).await?
        else {
            return Ok(None);
        };
        let diagnostic = diagnostic.as_deref().and_then(|value| {
            serde_json::from_str::<pioneer_protocol::RecoveryDiagnostic>(value).ok()
        });
        Ok(diagnostic.filter(|value| {
            value.stop_reason.is_some() && error.as_deref() == Some(value.public_message().as_str())
        }))
    }

    pub async fn get_blocked_turn_recovery_diagnostic(
        &self,
        turn_id: &str,
    ) -> Result<Option<pioneer_protocol::RecoveryDiagnostic>> {
        // Select the same latest transition before JSON work. The second
        // relational read revalidates its exact incarnation/revision and the
        // latest-transition fence, so no mixed snapshot can authorize a job.
        let source = pioneer_crud_conversations::turn_payload::events(
            self.connection,
            pioneer_entity::turn_event::Entity::find()
                .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id))
                .filter(pioneer_entity::turn_event::Column::EventType.is_in([
                    "turn/started",
                    "turn/blocked",
                    "turn/failed",
                    "turn/completed",
                ]))
                .order_by_desc(pioneer_entity::turn_event::Column::Sequence)
                .limit(1),
        )
        .await?
        .pop();
        let event = if let Some(source) = source.as_ref().filter(|s| s.event_type == "turn/blocked")
        {
            let payload = source
                .payload
                .as_ref()
                .context("missing turn event payload")?;
            match serde::Deserialize::deserialize(payload) {
                Ok(CanonicalTurnEventPayload::TurnBlocked(event)) => Some(event),
                _ => None,
            }
        } else {
            None
        };
        let job_id = event
            .as_ref()
            .and_then(|event| event.resume.as_ref())
            .and_then(|resume| resume.blocked_recovery_job_id.clone());
        let source_version = source.as_ref().map(|row| {
            (
                row.id.as_str(),
                row.portable_identity.as_str(),
                row.portable_revision,
            )
        });
        let Some((diagnostic, error)) = crate::recovery_diagnostic::blocked(
            self.connection,
            turn_id,
            job_id.as_deref(),
            source_version,
        )
        .await?
        else {
            return Ok(None);
        };
        let (Some(event), Some(job_id)) = (event, job_id) else {
            return Ok(None);
        };
        let diagnostic = diagnostic.as_deref().and_then(|value| {
            serde_json::from_str::<pioneer_protocol::RecoveryDiagnostic>(value).ok()
        });
        Ok(diagnostic.filter(|value| {
            value.stop_reason == Some(pioneer_protocol::RecoveryStopReason::NoProgress)
                && value.last_failure.is_some()
                && event.turn.id == turn_id
                && event.turn.status == TurnStatus::Blocked
                && event.turn.error == error
                && error.as_deref()
                    == Some(format!("{} (recovery job {job_id})", value.public_message()).as_str())
        }))
    }

    pub async fn get_recovery_job(&self, job_id: &str) -> Result<Option<RecoveryJobRecord>> {
        run_serialized_write(|| async {
            Ok(recovery_job::find_job_by_id(self.connection, job_id)
                .await?
                .map(recovery_job_record_from_model))
        })
        .await
    }

    pub async fn prepare_cli_runtime_recovery_outcome_source(
        &self,
        job_id: &str,
    ) -> Result<Option<pioneer_protocol::CliRuntimeBlockedTurnGuard>> {
        let Some(job) = recovery_job::find_job_by_id(self.connection, job_id).await? else {
            return Ok(None);
        };
        let source = CliRuntimeStore::new(self.connection.clone())
            .cli_runtime_turn_terminal_control_guard_by_id(&job.turn_id)
            .await?
            .and_then(|snapshot| snapshot.terminal_event_source());
        Ok(source.map(|mut source| {
            source.terminal_delivery_id = Some(source.terminal_delivery_id());
            source
        }))
    }

    pub async fn mark_recovery_job_retrying(
        &self,
        job_id: &str,
        active_attempt_id: &str,
        next_run_at_unix: i64,
        budget_origin_after_cooldown_unix: Option<i64>,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.mark_recovery_job_retrying_with_diagnostic(
            job_id,
            active_attempt_id,
            next_run_at_unix,
            budget_origin_after_cooldown_unix,
            last_error,
            None,
            now_unix,
            None,
        )
        .await
    }

    pub async fn mark_recovery_job_retrying_with_diagnostic(
        &self,
        job_id: &str,
        active_attempt_id: &str,
        next_run_at_unix: i64,
        budget_origin_after_cooldown_unix: Option<i64>,
        last_error: Option<String>,
        diagnostic: Option<pioneer_protocol::RecoveryDiagnostic>,
        now_unix: i64,
        accepted_cli_outcome: Option<&pioneer_protocol::CliRuntimeBlockedTurnGuard>,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let changes = ["gateway.control", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let result = async {
            let diagnostic_json = diagnostic.as_ref().map(serde_json::to_string).transpose()?;
            let last_error_value = last_error.clone();
            let cli_source = self
                .prepare_cli_runtime_recovery_outcome_source(job_id)
                .await?;
            run_serialized_write(|| async {
                let tx = self.connection.begin().await?;
                if !cli_runtime_recovery_transition_allowed(
                    &tx,
                    job_id,
                    cli_source.as_ref(),
                    accepted_cli_outcome,
                )
                .await?
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                let affected = recovery_job::mark_job_retrying(
                    &tx,
                    job_id,
                    active_attempt_id,
                    unix_to_datetime(next_run_at_unix),
                    budget_origin_after_cooldown_unix.map(unix_to_datetime),
                    last_error_value.clone(),
                    diagnostic_json.clone(),
                    unix_to_datetime(now_unix),
                )
                .await?;
                if affected {
                    ack_cli_runtime_recovery_outcome(&tx, accepted_cli_outcome).await?;
                }
                tx.commit().await?;
                Ok(affected)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn defer_active_recovery_job(
        &self,
        job_id: &str,
        active_attempt_id: &str,
        next_run_at_unix: i64,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let changes = ["gateway.control", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let result = async {
            let last_error_value = last_error.clone();
            let cli_source = self
                .prepare_cli_runtime_recovery_outcome_source(job_id)
                .await?;
            run_serialized_write(|| async {
                let tx = self.connection.begin().await?;
                if !cli_runtime_recovery_transition_allowed(&tx, job_id, cli_source.as_ref(), None)
                    .await?
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                let affected = recovery_job::defer_active_job(
                    &tx,
                    job_id,
                    active_attempt_id,
                    unix_to_datetime(next_run_at_unix),
                    last_error_value.clone(),
                    unix_to_datetime(now_unix),
                )
                .await?;
                tx.commit().await?;
                Ok(affected)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn mark_claimed_recovery_job_retrying(
        &self,
        job_id: &str,
        claim_token: &str,
        next_run_at_unix: i64,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let changes = ["gateway.control", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let result = async {
            let last_error_value = last_error.clone();
            run_serialized_write(|| async {
                recovery_job::mark_claimed_job_retrying(
                    self.connection,
                    job_id,
                    claim_token,
                    unix_to_datetime(next_run_at_unix),
                    last_error_value.clone(),
                    unix_to_datetime(now_unix),
                )
                .await
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn mark_claimed_recovery_job_active(
        &self,
        job_id: &str,
        claim_token: &str,
        active_attempt_id: &str,
        now_unix: i64,
    ) -> Result<ClaimedRecoveryActivation> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            run_serialized_write(|| async {
                let outcome = recovery_job::mark_claimed_job_active(
                    self.connection,
                    job_id,
                    claim_token,
                    active_attempt_id,
                    unix_to_datetime(now_unix),
                )
                .await?;
                Ok(match outcome {
                    recovery_job::ClaimedJobActivation::Activated => {
                        ClaimedRecoveryActivation::Activated
                    }
                    recovery_job::ClaimedJobActivation::BlockedByActiveRecovery => {
                        ClaimedRecoveryActivation::BlockedByActiveRecovery
                    }
                    recovery_job::ClaimedJobActivation::ClaimNotFound => {
                        ClaimedRecoveryActivation::ClaimNotFound
                    }
                })
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(matches!(value, ClaimedRecoveryActivation::Activated));
        }
        control_result
    }

    pub async fn release_claimed_recovery_job(
        &self,
        job_id: &str,
        claim_token: &str,
        next_run_at_unix: i64,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let last_error_value = last_error.clone();
            run_serialized_write(|| async {
                recovery_job::release_claimed_job(
                    self.connection,
                    job_id,
                    claim_token,
                    unix_to_datetime(next_run_at_unix),
                    last_error_value.clone(),
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

    pub async fn mark_due_pending_recovery_job_terminal_if_turn_idle(
        &self,
        job_id: &str,
        action: RecoveryAction,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.mark_due_pending_recovery_job_terminal_if_turn_idle_with_diagnostic(
            job_id, action, status, last_error, None, now_unix,
        )
        .await
    }

    pub async fn mark_due_pending_recovery_job_terminal_if_turn_idle_with_diagnostic(
        &self,
        job_id: &str,
        action: RecoveryAction,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        diagnostic: Option<pioneer_protocol::RecoveryDiagnostic>,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let changes = ["gateway.control", "maintenance.native", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let result = async {
            let diagnostic_json = diagnostic.as_ref().map(serde_json::to_string).transpose()?;
            let last_error_value = last_error.clone();
            run_serialized_write(|| async {
                let tx = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin due pending recovery terminal transaction")?;
                let affected = match recovery_job::mark_due_pending_job_terminal_if_turn_idle(
                    &tx,
                    job_id,
                    action,
                    status,
                    last_error_value.clone(),
                    unix_to_datetime(now_unix),
                )
                .await
                {
                    Ok(affected) => affected,
                    Err(error) => {
                        let _ = tx.rollback().await;
                        return Err(error);
                    }
                };
                if affected {
                    if let Some(value) = &diagnostic_json {
                        recovery_job::set_diagnostic(&tx, job_id, value.clone()).await?;
                    }
                }
                if affected
                    && let Err(error) = enqueue_recovery_terminalization_if_required(
                        &tx,
                        job_id,
                        unix_to_datetime(now_unix),
                    )
                    .await
                {
                    let _ = tx.rollback().await;
                    return Err(error);
                }
                tx.commit()
                    .await
                    .context("failed to commit due pending recovery terminal transaction")?;
                Ok(affected)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn mark_claimed_recovery_job_terminal(
        &self,
        job_id: &str,
        claim_token: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.mark_claimed_recovery_job_terminal_with_diagnostic(
            job_id,
            claim_token,
            status,
            last_error,
            None,
            now_unix,
        )
        .await
    }

    pub async fn mark_claimed_recovery_job_terminal_with_diagnostic(
        &self,
        job_id: &str,
        claim_token: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        diagnostic: Option<pioneer_protocol::RecoveryDiagnostic>,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let changes = ["gateway.control", "maintenance.native", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let result = async {
            let diagnostic_json = diagnostic.as_ref().map(serde_json::to_string).transpose()?;
            let last_error_value = last_error.clone();
            let cli_source = self
                .prepare_cli_runtime_recovery_outcome_source(job_id)
                .await?;
            run_serialized_write(|| async {
                let tx = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin claimed recovery terminal transaction")?;
                if matches!(
                    status,
                    RecoveryJobStatus::Failed
                        | RecoveryJobStatus::Exhausted
                        | RecoveryJobStatus::Blocked
                ) && !cli_runtime_recovery_transition_allowed(
                    &tx,
                    job_id,
                    cli_source.as_ref(),
                    None,
                )
                .await?
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                let affected = recovery_job::mark_claimed_job_terminal(
                    &tx,
                    job_id,
                    claim_token,
                    status,
                    last_error_value.clone(),
                    unix_to_datetime(now_unix),
                )
                .await?;
                if affected {
                    if let Some(value) = &diagnostic_json {
                        recovery_job::set_diagnostic(&tx, job_id, value.clone()).await?;
                    } else if status == RecoveryJobStatus::Blocked {
                        recovery_job::clear_diagnostic_stop_reason(&tx, job_id).await?;
                    }
                    enqueue_recovery_terminalization_if_required(
                        &tx,
                        job_id,
                        unix_to_datetime(now_unix),
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .context("failed to commit claimed recovery terminal transaction")?;
                Ok(affected)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn mark_recovery_job_terminal(
        &self,
        job_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let changes = ["gateway.control", "maintenance.native", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let result = async {
            let last_error_value = last_error.clone();
            let cli_source = self
                .prepare_cli_runtime_recovery_outcome_source(job_id)
                .await?;
            run_serialized_write(|| async {
                let tx = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin recovery terminal transaction")?;
                if matches!(
                    status,
                    RecoveryJobStatus::Failed
                        | RecoveryJobStatus::Exhausted
                        | RecoveryJobStatus::Blocked
                ) && !cli_runtime_recovery_transition_allowed(
                    &tx,
                    job_id,
                    cli_source.as_ref(),
                    None,
                )
                .await?
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                let affected = recovery_job::mark_job_terminal(
                    &tx,
                    job_id,
                    status,
                    last_error_value.clone(),
                    unix_to_datetime(now_unix),
                )
                .await?;
                if affected {
                    if status == RecoveryJobStatus::Blocked {
                        recovery_job::clear_diagnostic_stop_reason(&tx, job_id).await?;
                    }
                    enqueue_recovery_terminalization_if_required(
                        &tx,
                        job_id,
                        unix_to_datetime(now_unix),
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .context("failed to commit recovery terminal transaction")?;
                Ok(affected)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn mark_malformed_active_recovery_job_terminal(
        &self,
        job_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.mark_malformed_active_recovery_job_terminal_with_diagnostic(
            job_id, status, last_error, None, now_unix,
        )
        .await
    }

    pub async fn mark_malformed_active_recovery_job_terminal_with_diagnostic(
        &self,
        job_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        diagnostic: Option<pioneer_protocol::RecoveryDiagnostic>,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let changes = ["gateway.control", "maintenance.native", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let result = async {
            let diagnostic_json = diagnostic.as_ref().map(serde_json::to_string).transpose()?;
            let last_error_value = last_error.clone();
            let cli_source = self
                .prepare_cli_runtime_recovery_outcome_source(job_id)
                .await?;
            run_serialized_write(|| async {
                let tx = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin malformed recovery terminal transaction")?;
                if matches!(
                    status,
                    RecoveryJobStatus::Failed
                        | RecoveryJobStatus::Exhausted
                        | RecoveryJobStatus::Blocked
                ) && !cli_runtime_recovery_transition_allowed(
                    &tx,
                    job_id,
                    cli_source.as_ref(),
                    None,
                )
                .await?
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                let affected = recovery_job::mark_active_without_attempt_terminal(
                    &tx,
                    job_id,
                    status,
                    last_error_value.clone(),
                    unix_to_datetime(now_unix),
                )
                .await?;
                if affected {
                    if let Some(value) = &diagnostic_json {
                        recovery_job::set_diagnostic(&tx, job_id, value.clone()).await?;
                    } else if status == RecoveryJobStatus::Blocked {
                        recovery_job::clear_diagnostic_stop_reason(&tx, job_id).await?;
                    }
                    enqueue_recovery_terminalization_if_required(
                        &tx,
                        job_id,
                        unix_to_datetime(now_unix),
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .context("failed to commit malformed recovery terminal transaction")?;
                Ok(affected)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn mark_recovery_job_terminal_after_attempt(
        &self,
        job_id: &str,
        active_attempt_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.mark_recovery_job_terminal_after_attempt_with_diagnostic(
            job_id,
            active_attempt_id,
            status,
            last_error,
            None,
            now_unix,
            None,
        )
        .await
    }

    pub async fn mark_recovery_job_terminal_after_attempt_with_diagnostic(
        &self,
        job_id: &str,
        active_attempt_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        diagnostic: Option<pioneer_protocol::RecoveryDiagnostic>,
        now_unix: i64,
        accepted_cli_outcome: Option<&pioneer_protocol::CliRuntimeBlockedTurnGuard>,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let changes = ["gateway.control", "maintenance.native", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let result = async {
            let diagnostic_json = diagnostic.as_ref().map(serde_json::to_string).transpose()?;
            let last_error_value = last_error.clone();
            let cli_source = self
                .prepare_cli_runtime_recovery_outcome_source(job_id)
                .await?;
            run_serialized_write(|| async {
                let tx = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin recovery-after-attempt terminal transaction")?;
                if matches!(
                    status,
                    RecoveryJobStatus::Failed
                        | RecoveryJobStatus::Exhausted
                        | RecoveryJobStatus::Blocked
                ) && !cli_runtime_recovery_transition_allowed(
                    &tx,
                    job_id,
                    cli_source.as_ref(),
                    accepted_cli_outcome,
                )
                .await?
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                let affected = recovery_job::mark_job_terminal_after_attempt(
                    &tx,
                    job_id,
                    active_attempt_id,
                    status,
                    last_error_value.clone(),
                    unix_to_datetime(now_unix),
                )
                .await?;
                if affected {
                    if let Some(value) = &diagnostic_json {
                        recovery_job::set_diagnostic(&tx, job_id, value.clone()).await?;
                    } else if status == RecoveryJobStatus::Blocked {
                        recovery_job::clear_diagnostic_stop_reason(&tx, job_id).await?;
                    }
                    enqueue_recovery_terminalization_if_required(
                        &tx,
                        job_id,
                        unix_to_datetime(now_unix),
                    )
                    .await?;
                }
                if affected {
                    ack_cli_runtime_recovery_outcome(&tx, accepted_cli_outcome).await?;
                }
                tx.commit()
                    .await
                    .context("failed to commit recovery-after-attempt terminal transaction")?;
                Ok(affected)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn mark_native_recovery_start_running_owned(
        &self,
        execution: &TurnExecutionRecord,
        recovery: &pioneer_protocol::RecoveryAttemptContext,
        now_unix: i64,
        lease_until_unix: i64,
    ) -> Result<bool> {
        run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            if !native_recovery_start_transition_allowed(&tx, execution, recovery, now_unix).await?
            {
                tx.rollback().await?;
                return Ok(false);
            }
            let applied = turn_execution::mark_running_owned(
                &tx,
                &execution.turn_id,
                &execution.owner_id,
                unix_to_datetime(now_unix),
                unix_to_datetime(lease_until_unix),
            )
            .await?;
            tx.commit().await?;
            Ok(applied)
        })
        .await
    }

    pub async fn record_native_recovery_start_failure_owned(
        &self,
        execution: &TurnExecutionRecord,
        recovery: &pioneer_protocol::RecoveryAttemptContext,
        status: RecoveryJobStatus,
        next_run_at_unix: Option<i64>,
        last_error: Option<String>,
        diagnostic: Option<pioneer_protocol::RecoveryDiagnostic>,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let changes = ["gateway.control", "recovery.due"]
            .map(|domain| control_work_change(self.connection, domain, &control_wakes));
        let result = async {
            anyhow::ensure!(
                matches!(
                    status,
                    RecoveryJobStatus::Pending | RecoveryJobStatus::Exhausted
                ),
                "unexpected native pre-start recovery outcome"
            );
            let diagnostic_json = diagnostic.as_ref().map(serde_json::to_string).transpose()?;
            run_serialized_write(|| async {
                let tx = self.connection.begin().await?;
                if !native_recovery_start_transition_allowed(&tx, execution, recovery, now_unix)
                    .await?
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                let now = unix_to_datetime(now_unix);
                let applied = if status == RecoveryJobStatus::Pending {
                    let applied = recovery_job::mark_job_retrying(
                        &tx,
                        &recovery.job_id,
                        &recovery.attempt_id,
                        unix_to_datetime(
                            next_run_at_unix.context("native retry deadline is missing")?,
                        ),
                        None,
                        last_error.clone(),
                        diagnostic_json.clone(),
                        now,
                    )
                    .await?;
                    if applied {
                        anyhow::ensure!(
                            turn_execution::mark_recovering_owned(
                                &tx,
                                &execution.turn_id,
                                &execution.owner_id,
                                now
                            )
                            .await?,
                            "native recovery execution changed during refusal"
                        );
                    }
                    applied
                } else {
                    let applied = recovery_job::mark_job_terminal_after_attempt(
                        &tx,
                        &recovery.job_id,
                        &recovery.attempt_id,
                        status,
                        last_error.clone(),
                        now,
                    )
                    .await?;
                    if applied {
                        if let Some(value) = &diagnostic_json {
                            recovery_job::set_diagnostic(&tx, &recovery.job_id, value.clone())
                                .await?;
                        }
                        enqueue_recovery_terminalization_if_required(&tx, &recovery.job_id, now)
                            .await?;
                    }
                    applied
                };
                tx.commit().await?;
                Ok(applied)
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            for change in changes {
                change.completed(*changed);
            }
        }
        result
    }

    pub async fn list_active_recovery_job_page(
        &self,
        after: Option<&str>,
        upper: &str,
        limit: u64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        Ok(
            crate::recovery_watchdog::active_page(self.connection, after, upper, limit)
                .await?
                .into_iter()
                .map(recovery_job_record_from_model)
                .collect(),
        )
    }

    pub async fn list_active_recovery_jobs(&self, limit: u64) -> Result<Vec<RecoveryJobRecord>> {
        let jobs = recovery_job::list_active_jobs(self.connection, limit).await?;
        Ok(jobs
            .into_iter()
            .map(recovery_job_record_from_model)
            .collect())
    }

    pub async fn find_open_recovery_jobs_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Vec<RecoveryJobRecord>> {
        run_serialized_write(|| async {
            let jobs = recovery_job::find_open_jobs_by_turn(self.connection, turn_id).await?;
            Ok(jobs
                .into_iter()
                .map(recovery_job_record_from_model)
                .collect::<Vec<_>>())
        })
        .await
    }

    pub async fn find_unresolved_recovery_job_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Option<RecoveryJobRecord>> {
        run_serialized_write(|| async {
            Ok(
                recovery_job::find_unresolved_job_by_turn(self.connection, turn_id)
                    .await?
                    .map(recovery_job_record_from_model),
            )
        })
        .await
    }
}

pub async fn enqueue_recovery_terminalization_if_required<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    let job = recovery_job::find_job_by_id(db, job_id)
        .await?
        .with_context(|| format!("terminal recovery job `{job_id}` disappeared"))?;
    let status = recovery_job_status_from_db(job.status.as_str())
        .context("terminal recovery job has an unknown status")?;
    if matches!(
        status,
        RecoveryJobStatus::Failed | RecoveryJobStatus::Exhausted | RecoveryJobStatus::Blocked
    ) {
        recovery_terminalization_outbox::enqueue_for_terminal_job(db, &job, now).await?;
    }
    Ok(())
}

impl RecoveryStore<'_> {
    pub async fn active_recovery_job_upper_id(&self) -> Result<Option<String>> {
        crate::recovery_watchdog::active_upper(self.connection).await
    }
}

#[cfg(test)]
#[path = "recovery_store_tests.rs"]
mod tests;

use crate::native_terminal_effect_outbox::NativeTerminalEffectStats;
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeLifecycleDurableHealthSnapshot {
    pub active_turns: u64,
    pub stale_running_turns: u64,
    pub active_recovery_jobs: u64,
    pub oldest_recovery_age_secs: Option<u64>,
    pub terminal_effects: NativeTerminalEffectStats,
    pub oldest_terminal_effect_age_secs: Option<u64>,
}

impl RecoveryStore<'_> {
    pub async fn native_lifecycle_durable_health_snapshot(
        &self,
        now_unix: i64,
        stale_after_secs: u64,
    ) -> Result<NativeLifecycleDurableHealthSnapshot> {
        let stale_cutoff = unix_to_datetime(
            now_unix.saturating_sub(i64::try_from(stale_after_secs).unwrap_or(i64::MAX)),
        );
        let (active_turns, stale_running_turns) =
            crate::recovery_reads::active_native_turn_counts(self.connection, stale_cutoff).await?;
        let active_recovery_jobs = self.list_active_recovery_jobs(101).await?;
        let oldest_recovery_age_secs = active_recovery_jobs
            .iter()
            .map(|job| job.scheduled_at_unix)
            .min()
            .map(|oldest| u64::try_from(now_unix.saturating_sub(oldest)).unwrap_or(0));
        let terminal_effects =
            crate::native_terminal_effect_outbox::load_stats(self.connection).await?;
        let oldest_terminal_effect = pioneer_entity::native_terminal_effect_outbox::Entity::find()
            .filter(
                pioneer_entity::native_terminal_effect_outbox::Column::Status.is_in([
                    "prepared",
                    "waiting_acceptance",
                    "ready",
                    "running",
                    "retry_wait",
                    "unresolved",
                ]),
            )
            .order_by_asc(pioneer_entity::native_terminal_effect_outbox::Column::PreparedAt)
            .one(self.connection)
            .await
            .context("failed to load oldest native terminal effect")?;
        let oldest_terminal_effect_age_secs = oldest_terminal_effect.map(|effect| {
            u64::try_from(now_unix.saturating_sub(effect.prepared_at.timestamp())).unwrap_or(0)
        });
        Ok(NativeLifecycleDurableHealthSnapshot {
            active_turns,
            stale_running_turns,
            active_recovery_jobs: u64::try_from(active_recovery_jobs.len()).unwrap_or(u64::MAX),
            oldest_recovery_age_secs,
            terminal_effects,
            oldest_terminal_effect_age_secs,
        })
    }
}
