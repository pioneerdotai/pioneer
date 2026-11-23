use crate::{
    CrudStore, HookAuditEventRecord, HookRunAttemptCompletionRecord, HookRunAttemptRecord,
    HookRunCompletionRecord, HookRunRecord, NewHookAuditEventRecord, NewHookRunAttemptRecord,
    NewHookRunRecord, RecoverableHookRunRecord,
};
use anyhow::Result;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

impl CrudStore {
    pub async fn create_hook_run(
        &self,
        run: NewHookRunRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<HookRunRecord> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .create_hook_run(run, now)
            .await
    }

    pub async fn find_hook_run(
        &self,
        run_id: &pioneer_hooks::HookRunId,
    ) -> Result<Option<HookRunRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .find_hook_run(run_id)
            .await
    }

    pub async fn find_hook_run_by_idempotency_key(
        &self,
        idempotency_key: &pioneer_hooks::HookRunIdempotencyKey,
    ) -> Result<Option<HookRunRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .find_hook_run_by_idempotency_key(idempotency_key)
            .await
    }

    pub async fn list_hook_runs_for_turn(
        &self,
        turn_id: &str,
        phase: Option<pioneer_hooks::HookPhase>,
        limit: u64,
    ) -> Result<Vec<HookRunRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .list_hook_runs_for_turn(turn_id, phase, limit)
            .await
    }

    pub async fn mark_hook_run_running(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .mark_hook_run_running(run_id, now)
            .await
    }

    pub async fn complete_hook_run(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        completion: HookRunCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .complete_hook_run(run_id, completion, now)
            .await
    }

    pub async fn append_hook_run_attempt(
        &self,
        attempt: NewHookRunAttemptRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<HookRunAttemptRecord> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .append_hook_run_attempt(attempt, now)
            .await
    }

    pub async fn complete_hook_run_attempt(
        &self,
        attempt_id: &pioneer_hooks::HookRunAttemptId,
        completion: HookRunAttemptCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunAttemptRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .complete_hook_run_attempt(attempt_id, completion, now)
            .await
    }

    pub async fn list_hook_run_attempts(
        &self,
        run_id: &pioneer_hooks::HookRunId,
    ) -> Result<Vec<HookRunAttemptRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .list_hook_run_attempts(run_id)
            .await
    }

    pub async fn next_hook_recovery_deadline(
        &self,
        stale_running_after_ms: u64,
    ) -> Result<Option<i64>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .next_hook_recovery_deadline(stale_running_after_ms)
            .await
    }

    pub async fn list_recoverable_hook_runs(
        &self,
        scan: pioneer_hooks::HookRecoveryScan,
    ) -> Result<Vec<RecoverableHookRunRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .list_recoverable_hook_runs(scan)
            .await
    }

    pub async fn schedule_hook_run_retry(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        schedule: pioneer_hooks::HookRetrySchedule,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .schedule_hook_run_retry(run_id, schedule, now)
            .await
    }

    pub async fn mark_stale_hook_run_timed_out(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        completion: HookRunCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .mark_stale_hook_run_timed_out(run_id, completion, now)
            .await
    }

    pub async fn mark_hook_run_unrecoverable(
        &self,
        run_id: &pioneer_hooks::HookRunId,
        completion: HookRunCompletionRecord,
        now: DateTimeWithTimeZone,
    ) -> Result<Option<HookRunRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .mark_hook_run_unrecoverable(run_id, completion, now)
            .await
    }

    pub async fn append_hook_audit_events(
        &self,
        records: Vec<NewHookAuditEventRecord>,
        now: DateTimeWithTimeZone,
    ) -> Result<Vec<HookAuditEventRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .append_hook_audit_events(records, now)
            .await
    }

    pub async fn list_hook_audit_events_for_run(
        &self,
        run_id: &pioneer_hooks::HookRunId,
    ) -> Result<Vec<HookAuditEventRecord>> {
        pioneer_crud_execution::hook_run_store::HookRunStore::new(&self.connection)
            .list_hook_audit_events_for_run(run_id)
            .await
    }
}
