use crate::{
    AtomicRecoveryJobEnqueueOutcome, ClaimedRecoveryActivation, CrudStore, GatewayControlWake,
    RecoveryJobRecord, TaskOwnedTurnResumeOutcome, TurnExecutionRecord, TurnLivenessRecord,
};
use anyhow::{Context, Result};
pub use pioneer_crud_execution::blocked_recovery_resume::BlockedTurnRecoveryResumeOutcome;
use pioneer_crud_execution::blocked_recovery_resume::{
    BlockedResumeAdmission, admit_blocked_recovery_resume,
};
use pioneer_crud_execution::recovery_job::recovery_job_record_from_model;
use pioneer_crud_execution::recovery_store::RecoveryChange;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_execution::{recovery_job, recovery_terminalization_outbox, turn_execution};
use pioneer_protocol::{
    ProviderFailureClass, ProviderFailureStage, RecoveryAction, RecoveryJobStatus, RecoveryTrigger,
    TurnItemType, TurnStatus,
};
use sea_orm::TransactionTrait;

impl CrudStore {
    /// Scoped clones share this retained postcommit recovery hint.
    pub fn recovery_job_notification(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.recovery_work.clone()
    }

    pub(crate) fn recovery_store(
        &self,
    ) -> pioneer_crud_execution::recovery_store::RecoveryStore<'_> {
        pioneer_crud_execution::recovery_store::RecoveryStore::new(
            &self.connection,
            &self.recovery_work,
        )
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
        self.recovery_store()
            .enqueue_recovery_job(
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
                now_unix,
            )
            .await
    }

    /// Atomically creates one recovery episode or reuses its unresolved owner.
    #[allow(clippy::too_many_arguments)]
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
        self.recovery_store()
            .enqueue_recovery_job_if_no_unresolved(
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
        self.recovery_store()
            .enqueue_recovery_job_with_diagnostic_if_no_unresolved(
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
                diagnostic,
                now_unix,
            )
            .await
    }

    pub async fn mark_attempt_recovery_action(
        &self,
        attempt_id: &str,
        action: RecoveryAction,
        now_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .mark_attempt_recovery_action(attempt_id, action, now_unix)
            .await
    }

    pub async fn claim_due_recovery_jobs(
        &self,
        now_unix: i64,
        claim_lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        self.recovery_store()
            .claim_due_recovery_jobs(now_unix, claim_lease_secs, limit)
            .await
    }

    pub async fn next_recovery_job_due_at(&self) -> Result<Option<i64>> {
        self.recovery_store().next_recovery_job_due_at().await
    }

    pub async fn list_due_pending_recovery_jobs_by_action(
        &self,
        action: RecoveryAction,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        self.recovery_store()
            .list_due_pending_recovery_jobs_by_action(action, now_unix, limit)
            .await
    }

    /// Only the diagnostic that actually produced this failed Turn may cross into Task.
    /// A pending/resumed job, unrelated failure or legacy raw text cannot qualify.
    pub async fn get_failed_turn_recovery_diagnostic(
        &self,
        turn_id: &str,
    ) -> Result<Option<pioneer_protocol::RecoveryDiagnostic>> {
        self.recovery_store()
            .get_failed_turn_recovery_diagnostic(turn_id)
            .await
    }

    /// The latest canonical blocked transition owns this diagnostic. Never select a
    /// job merely because it is the newest failure for the same Turn.
    pub async fn get_blocked_turn_recovery_diagnostic(
        &self,
        turn_id: &str,
    ) -> Result<Option<pioneer_protocol::RecoveryDiagnostic>> {
        self.recovery_store()
            .get_blocked_turn_recovery_diagnostic(turn_id)
            .await
    }

    pub async fn get_recovery_job(&self, job_id: &str) -> Result<Option<RecoveryJobRecord>> {
        self.recovery_store().get_recovery_job(job_id).await
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
        self.recovery_store()
            .mark_recovery_job_retrying(
                job_id,
                active_attempt_id,
                next_run_at_unix,
                budget_origin_after_cooldown_unix,
                last_error,
                now_unix,
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
        self.recovery_store()
            .mark_recovery_job_retrying_with_diagnostic(
                job_id,
                active_attempt_id,
                next_run_at_unix,
                budget_origin_after_cooldown_unix,
                last_error,
                diagnostic,
                now_unix,
                accepted_cli_outcome,
            )
            .await
    }

    pub async fn defer_active_recovery_job(
        &self,
        job_id: &str,
        active_attempt_id: &str,
        next_run_at_unix: i64,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .defer_active_recovery_job(
                job_id,
                active_attempt_id,
                next_run_at_unix,
                last_error,
                now_unix,
            )
            .await
    }

    pub async fn mark_claimed_recovery_job_retrying(
        &self,
        job_id: &str,
        claim_token: &str,
        next_run_at_unix: i64,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .mark_claimed_recovery_job_retrying(
                job_id,
                claim_token,
                next_run_at_unix,
                last_error,
                now_unix,
            )
            .await
    }

    pub async fn mark_claimed_recovery_job_active(
        &self,
        job_id: &str,
        claim_token: &str,
        active_attempt_id: &str,
        now_unix: i64,
    ) -> Result<ClaimedRecoveryActivation> {
        self.recovery_store()
            .mark_claimed_recovery_job_active(job_id, claim_token, active_attempt_id, now_unix)
            .await
    }

    pub async fn release_claimed_recovery_job(
        &self,
        job_id: &str,
        claim_token: &str,
        next_run_at_unix: i64,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .release_claimed_recovery_job(
                job_id,
                claim_token,
                next_run_at_unix,
                last_error,
                now_unix,
            )
            .await
    }

    pub async fn mark_due_pending_recovery_job_terminal_if_turn_idle(
        &self,
        job_id: &str,
        action: RecoveryAction,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .mark_due_pending_recovery_job_terminal_if_turn_idle(
                job_id, action, status, last_error, now_unix,
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
        self.recovery_store()
            .mark_due_pending_recovery_job_terminal_if_turn_idle_with_diagnostic(
                job_id, action, status, last_error, diagnostic, now_unix,
            )
            .await
    }

    pub async fn mark_claimed_recovery_job_terminal(
        &self,
        job_id: &str,
        claim_token: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .mark_claimed_recovery_job_terminal(job_id, claim_token, status, last_error, now_unix)
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
        self.recovery_store()
            .mark_claimed_recovery_job_terminal_with_diagnostic(
                job_id,
                claim_token,
                status,
                last_error,
                diagnostic,
                now_unix,
            )
            .await
    }

    pub async fn mark_recovery_job_terminal(
        &self,
        job_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .mark_recovery_job_terminal(job_id, status, last_error, now_unix)
            .await
    }

    pub async fn mark_malformed_active_recovery_job_terminal(
        &self,
        job_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .mark_malformed_active_recovery_job_terminal(job_id, status, last_error, now_unix)
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
        self.recovery_store()
            .mark_malformed_active_recovery_job_terminal_with_diagnostic(
                job_id, status, last_error, diagnostic, now_unix,
            )
            .await
    }

    pub async fn mark_recovery_job_terminal_after_attempt(
        &self,
        job_id: &str,
        active_attempt_id: &str,
        status: RecoveryJobStatus,
        last_error: Option<String>,
        now_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .mark_recovery_job_terminal_after_attempt(
                job_id,
                active_attempt_id,
                status,
                last_error,
                now_unix,
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
        self.recovery_store()
            .mark_recovery_job_terminal_after_attempt_with_diagnostic(
                job_id,
                active_attempt_id,
                status,
                last_error,
                diagnostic,
                now_unix,
                accepted_cli_outcome,
            )
            .await
    }

    /// Admission and a late pre-start failure share the exact attempt/owner fence.
    pub async fn mark_native_recovery_start_running_owned(
        &self,
        execution: &TurnExecutionRecord,
        recovery: &pioneer_protocol::RecoveryAttemptContext,
        now_unix: i64,
        lease_until_unix: i64,
    ) -> Result<bool> {
        self.recovery_store()
            .mark_native_recovery_start_running_owned(
                execution,
                recovery,
                now_unix,
                lease_until_unix,
            )
            .await
    }

    /// Policy is computed on owned data before writer admission. Revalidate its
    /// exact source in the transaction, so cancellation, terminal acceptance,
    /// ownership takeover and a newer attempt all win over a late refusal.
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
        self.recovery_store()
            .record_native_recovery_start_failure_owned(
                execution,
                recovery,
                status,
                next_run_at_unix,
                last_error,
                diagnostic,
                now_unix,
            )
            .await
    }

    pub async fn find_recovery_jobs_by_turn_and_status(
        &self,
        turn_id: &str,
        status: RecoveryJobStatus,
    ) -> Result<Vec<RecoveryJobRecord>> {
        self.retry_write_on_conflict(|| async {
            let jobs =
                recovery_job::find_jobs_by_turn_and_status(&self.connection, turn_id, status)
                    .await?;
            Ok(jobs
                .into_iter()
                .map(recovery_job_record_from_model)
                .collect::<Vec<_>>())
        })
        .await
    }

    pub async fn resume_blocked_turn_recovery(
        &self,
        thread_id: &str,
        turn_id: &str,
        recovery_job_id: Option<&str>,
        now_unix: i64,
        execution_owner_id: &str,
        execution_lease_until_unix: i64,
    ) -> Result<BlockedTurnRecoveryResumeOutcome> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let changes = ["gateway.control", "recovery.due"]
            .map(|domain| self.control_work_change(domain, &control_wakes));
        let result = async {
            let _ownership_change = self.execution_heartbeats.changing(execution_owner_id);
            self.retry_write_on_conflict(|| async {
                let now = unix_to_datetime(now_unix);
                let tx = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin blocked turn resume transaction")?;

                let (turn_model, job) =
                    match admit_blocked_recovery_resume(&tx, thread_id, turn_id, recovery_job_id)
                        .await?
                    {
                        BlockedResumeAdmission::Ready { turn, job } => (turn, job),
                        BlockedResumeAdmission::Unchanged {
                            outcome,
                            rollback_context,
                        } => {
                            tx.rollback().await.context(rollback_context)?;
                            return Ok(outcome);
                        }
                    };

                let max_attempts = job.max_attempts.max(job.run_count.saturating_add(1));
                let resumed = recovery_job::resume_blocked_job(
                    &tx,
                    &job,
                    RecoveryAction::RestartTurn,
                    max_attempts,
                    now,
                )
                .await?;
                if !resumed {
                    tx.rollback()
                        .await
                        .context("failed to rollback stale blocked recovery resume transaction")?;
                    return Ok(BlockedTurnRecoveryResumeOutcome::NotFound);
                }

                recovery_terminalization_outbox::cancel_undelivered_for_job(
                    &tx,
                    job.id.as_str(),
                    "superseded by explicit blocked Turn resume",
                    now,
                )
                .await?;

                pioneer_crud_tasks::turn_sources::clear_confirmed_blocked_for_resume(
                    &tx,
                    thread_id,
                    turn_id,
                    now,
                    &turn_model,
                )
                .await?;
                let updated = pioneer_crud_tasks::turn_sources::update_turn_status(
                    &tx,
                    thread_id,
                    turn_id,
                    TurnStatus::InProgress,
                    None,
                    now,
                )
                .await?;
                if !updated {
                    tx.rollback()
                        .await
                        .context("failed to rollback blocked turn resume after status miss")?;
                    return Ok(BlockedTurnRecoveryResumeOutcome::NotFound);
                }
                pioneer_crud_conversations::turn_writes::append_turn_status_history(
                    &tx,
                    turn_id,
                    TurnStatus::InProgress,
                    Some("blocked turn resumed by explicit request".to_owned()),
                    now,
                )
                .await?;
                if !turn_execution::reacquire_blocked(
                    &tx,
                    turn_id,
                    execution_owner_id,
                    now,
                    unix_to_datetime(execution_lease_until_unix),
                )
                .await?
                {
                    tx.rollback().await.context(
                        "failed to rollback blocked turn resume after execution ownership conflict",
                    )?;
                    return Ok(BlockedTurnRecoveryResumeOutcome::NotFound);
                }

                let acquired_execution = turn_execution::find(&tx, turn_id).await?;
                tx.commit()
                    .await
                    .context("failed to commit blocked turn resume transaction")?;
                if let Some(execution) = acquired_execution.as_ref() {
                    _ownership_change.register(execution);
                }

                Ok(
                    recovery_job::find_job_by_id(&self.connection, job.id.as_str())
                        .await?
                        .map(recovery_job_record_from_model)
                        .map(BlockedTurnRecoveryResumeOutcome::Resumed)
                        .unwrap_or(BlockedTurnRecoveryResumeOutcome::NotFound),
                )
            })
            .await
        }
        .await;
        if let Ok(outcome) = &result {
            for change in changes {
                change.completed(matches!(
                    outcome,
                    BlockedTurnRecoveryResumeOutcome::Resumed(_)
                ));
            }
        }
        result
    }

    /// Atomically reopens every durable owner of a blocked task child turn.
    ///
    /// Generic Turn recovery is intentionally not used here.  A task child is
    /// owned by a TaskRun aggregate; reopening only the Turn would allow the
    /// actor to perform side effects while its task/run/execution remained
    /// terminal.  The transaction also fences successor runs and active locks
    /// so a resumed child cannot race a newer occurrence.
    pub async fn resume_task_owned_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        recovery_job_id: Option<&str>,
        now_unix: i64,
        execution_owner_id: &str,
        execution_lease_until_unix: i64,
    ) -> Result<Option<TaskOwnedTurnResumeOutcome>> {
        self.task_event_store()
            .resume_task_owned_turn(
                &self.execution_heartbeats,
                &self.recovery_work,
                thread_id,
                turn_id,
                recovery_job_id,
                now_unix,
                execution_owner_id,
                execution_lease_until_unix,
            )
            .await
    }

    pub async fn retry_task_revision_execution(
        &self,
        thread_id: &str,
        turn_id: &str,
        automatic: bool,
        admission: &crate::NewTaskExecutionAdmission,
        now_unix: i64,
        action: Option<&crate::AgentCommitInput>,
    ) -> Result<Option<TaskOwnedTurnResumeOutcome>> {
        self.task_event_store()
            .retry_task_revision_execution(
                &self.execution_heartbeats,
                &self.recovery_work,
                thread_id,
                turn_id,
                automatic,
                admission,
                now_unix,
                |tx| {
                    let action = action.cloned();
                    Box::pin(async move {
                        if let Some(action) = action {
                            super::agent_transactions::commit_agent_action(tx, &action).await?;
                        }
                        Ok(())
                    })
                },
            )
            .await
    }

    pub async fn task_revision_retry_blocker(
        &self,
        task: &pioneer_protocol::Task,
        run: &pioneer_protocol::TaskRun,
        revision: &pioneer_protocol::TaskRunTurn,
    ) -> Result<Option<pioneer_protocol::TaskRevisionRetryBlocker>> {
        self.task_event_store()
            .task_revision_retry_blocker(task, run, revision)
            .await
    }

    pub async fn stop_pending_task_revision_retry(
        &self,
        revision: &pioneer_protocol::TaskRunTurn,
        blocker: pioneer_protocol::TaskRevisionRetryBlocker,
        now: i64,
    ) -> Result<bool> {
        self.task_event_store()
            .stop_pending_task_revision_retry(revision, blocker, now)
            .await
    }

    /// Compact reader probe; errors remain unknown rather than proving empty.
    pub async fn active_recovery_job_upper_id(&self) -> Result<Option<String>> {
        self.recovery_store().active_recovery_job_upper_id().await
    }

    pub async fn list_active_recovery_job_page(
        &self,
        after: Option<&str>,
        upper: &str,
        limit: u64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        self.recovery_store()
            .list_active_recovery_job_page(after, upper, limit)
            .await
    }

    /// Prepared watchdog decisions commit only if current policy, episode and
    /// causal progress still permit them. The guard and transition are one
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
        self.recovery_store()
            .apply_recovery_watchdog_failure(
                expected,
                status,
                next_run_at_unix,
                last_error,
                diagnostic,
                now_unix,
                observed_liveness,
            )
            .await
    }

    pub async fn list_active_recovery_jobs(&self, limit: u64) -> Result<Vec<RecoveryJobRecord>> {
        self.recovery_store().list_active_recovery_jobs(limit).await
    }

    pub async fn find_open_recovery_jobs_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Vec<RecoveryJobRecord>> {
        self.recovery_store()
            .find_open_recovery_jobs_for_turn(turn_id)
            .await
    }

    pub async fn find_unresolved_recovery_job_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Option<RecoveryJobRecord>> {
        self.recovery_store()
            .find_unresolved_recovery_job_for_turn(turn_id)
            .await
    }

    pub async fn cancel_open_recovery_jobs_for_turn(
        &self,
        turn_id: &str,
        exclude_job_id: Option<&str>,
        reason: Option<String>,
        now_unix: i64,
    ) -> Result<Vec<RecoveryJobRecord>> {
        let control_wakes = vec![
            GatewayControlWake::Supervision,
            GatewayControlWake::Terminalization,
        ];
        let changes = [
            "gateway.control",
            "maintenance.native",
            "recovery.due",
            "tasks.lifecycle",
        ]
        .map(|domain| self.control_work_change(domain, &control_wakes));
        let _recovery_change = RecoveryChange(self.recovery_work.clone());
        let result = async {
            let reason_value = reason.clone();
            self.retry_write_on_conflict(|| async {
                let tx = self.connection.begin().await?;
                let jobs = recovery_job::cancel_open_jobs_for_turn(
                    &tx,
                    turn_id,
                    exclude_job_id,
                    reason_value.clone(),
                    unix_to_datetime(now_unix),
                )
                .await?;
                if let Some(job) = jobs.first() {
                    crate::store::conversations::context_writes::settle_shell_controls_for_recovery_jobs(
                        &tx,
                        &[job.id.clone()],
                    )
                    .await?;
                }
                tx.commit().await?;
                Ok(jobs
                    .into_iter()
                    .map(recovery_job_record_from_model)
                    .collect::<Vec<_>>())
            })
            .await
        }
        .await;
        if let Ok(jobs) = &result {
            for change in changes {
                change.completed(!jobs.is_empty());
            }
        }
        result
    }
}
