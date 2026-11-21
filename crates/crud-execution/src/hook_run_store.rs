//! Scoped hook execution, attempt recovery, and audit records.
use crate::hook_run::{
    self, HookAuditEventRecord, HookRunAttemptCompletionRecord, HookRunAttemptRecord,
    HookRunCompletionRecord, HookRunRecord, NewHookAuditEventRecord, NewHookRunAttemptRecord,
    NewHookRunRecord, RecoverableHookRunRecord,
};
use crate::retry::run_serialized_write;
use anyhow::Result;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::TransactionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

pub struct HookRunStore<'a> {
    connection: &'a SqliteDatabase,
}

impl<'a> HookRunStore<'a> {
    pub fn new(connection: &'a SqliteDatabase) -> Self {
        Self { connection }
    }

    pub async fn create_hook_run(
        &self,
        run: NewHookRunRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<HookRunRecord> {
        let change = self.connection.work_signal("hooks").changing();
        let result = async {
            run_serialized_write(|| {
                let run = run.clone();
                let now = now.clone();
                async move { hook_run::create_hook_run(self.connection, run, now).await }
            })
            .await
        }
        .await;
        if result.is_ok() {
            change.completed(true);
        }
        result
    }

    pub async fn find_hook_run(
        &self,
        run_id: &pioneer_hooks::HookRunId,
    ) -> Result<Option<HookRunRecord>> {
        hook_run::find_hook_run_by_id(self.connection, run_id).await
    }

    pub async fn find_hook_run_by_idempotency_key(
        &self,
        idempotency_key: &pioneer_hooks::HookRunIdempotencyKey,
    ) -> Result<Option<HookRunRecord>> {
        hook_run::find_hook_run_by_idempotency_key(self.connection, idempotency_key).await
    }

    pub async fn list_hook_runs_for_turn(
        &self,
        turn_id: &str,
        phase: Option<pioneer_hooks::HookPhase>,
        limit: u64,
    ) -> Result<Vec<HookRunRecord>> {
        hook_run::list_hook_runs_for_turn(self.connection, turn_id, phase, limit).await
    }

    pub async fn mark_hook_run_running(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        let change = self.connection.work_signal("hooks").changing();
        let result = async {
            run_serialized_write(|| {
                let now = now.clone();
                async move { hook_run::mark_hook_run_running(self.connection, run_id, now).await }
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(changed.is_some());
        }
        result
    }

    pub async fn complete_hook_run(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        completion: HookRunCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        let change = self.connection.work_signal("hooks").changing();
        let result = async {
            run_serialized_write(|| {
                let completion = completion.clone();
                let now = now.clone();
                async move {
                    hook_run::complete_hook_run(self.connection, run_id, completion, now).await
                }
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(changed.is_some());
        }
        result
    }

    pub async fn append_hook_run_attempt(
        &self,
        attempt: NewHookRunAttemptRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<HookRunAttemptRecord> {
        let change = self.connection.work_signal("hooks").changing();
        let result = async {
            run_serialized_write(|| {
                let attempt = attempt.clone();
                let now = now.clone();
                async move {
                    let transaction = self.connection.begin().await?;
                    let recorded =
                        hook_run::append_hook_run_attempt(&transaction, attempt, now).await?;
                    transaction.commit().await?;
                    Ok(recorded)
                }
            })
            .await
        }
        .await;
        if result.is_ok() {
            change.completed(true);
        }
        result
    }

    pub async fn complete_hook_run_attempt(
        &self,
        attempt_id: &pioneer_hooks::HookRunAttemptId,
        completion: HookRunAttemptCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunAttemptRecord>> {
        let change = self.connection.work_signal("hooks").changing();
        let result = async {
            run_serialized_write(|| {
                let completion = completion.clone();
                let now = now.clone();
                async move {
                    hook_run::complete_hook_run_attempt(
                        self.connection,
                        attempt_id,
                        completion,
                        now,
                    )
                    .await
                }
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(changed.is_some());
        }
        result
    }

    pub async fn list_hook_run_attempts(
        &self,
        run_id: &pioneer_hooks::HookRunId,
    ) -> Result<Vec<HookRunAttemptRecord>> {
        hook_run::list_hook_run_attempts(self.connection, run_id).await
    }

    pub async fn next_hook_recovery_deadline(
        &self,
        stale_running_after_ms: u64,
    ) -> Result<Option<i64>> {
        hook_run::next_recovery_deadline(self.connection, stale_running_after_ms).await
    }

    pub async fn list_recoverable_hook_runs(
        &self,
        scan: pioneer_hooks::HookRecoveryScan,
    ) -> Result<Vec<RecoverableHookRunRecord>> {
        hook_run::list_recoverable_hook_runs(self.connection, scan).await
    }

    pub async fn schedule_hook_run_retry(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        schedule: pioneer_hooks::HookRetrySchedule,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        let change = self.connection.work_signal("hooks").changing();
        let result = async {
            run_serialized_write(|| {
                let schedule = schedule.clone();
                let now = now.clone();
                async move {
                    hook_run::schedule_hook_run_retry(self.connection, run_id, schedule, now).await
                }
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(changed.is_some());
        }
        result
    }

    pub async fn mark_stale_hook_run_timed_out(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        completion: HookRunCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        let change = self.connection.work_signal("hooks").changing();
        let result = async {
            run_serialized_write(|| {
                let completion = completion.clone();
                let now = now.clone();
                async move {
                    let transaction = self.connection.begin().await?;
                    let completed = hook_run::mark_stale_hook_run_timed_out(
                        &transaction,
                        run_id,
                        completion,
                        now,
                    )
                    .await?;
                    transaction.commit().await?;
                    Ok(completed)
                }
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(changed.is_some());
        }
        result
    }

    pub async fn mark_hook_run_unrecoverable(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        completion: HookRunCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        let change = self.connection.work_signal("hooks").changing();
        let result = async {
            run_serialized_write(|| {
                let completion = completion.clone();
                let now = now.clone();
                async move {
                    hook_run::mark_hook_run_unrecoverable(self.connection, run_id, completion, now)
                        .await
                }
            })
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(changed.is_some());
        }
        result
    }

    pub async fn append_hook_audit_events(
        &self,
        records: Vec<NewHookAuditEventRecord>,
        now: DateTimeWithTimeZone,
    ) -> Result<Vec<HookAuditEventRecord>> {
        run_serialized_write(|| {
            let records = records.clone();
            let now = now.clone();
            async move { hook_run::append_hook_audit_events(self.connection, records, now).await }
        })
        .await
    }

    pub async fn list_hook_audit_events_for_run(
        &self,
        run_id: &pioneer_hooks::HookRunId,
    ) -> Result<Vec<HookAuditEventRecord>> {
        hook_run::list_hook_audit_events_for_run(self.connection, run_id).await
    }
}
