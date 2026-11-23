//! Execution state, run bindings, and dispatch admission.

use crate::{
    AppendedTaskEvent, CrudStore, TaskRunChildAnchor, TaskRuntimeInvariantSnapshot,
    TerminalTaskOccurrenceMismatch, TerminalTaskOccurrenceMismatchPage,
};
use anyhow::Result;
use pioneer_protocol::{
    TaskExecutorKind, TaskRun, TaskRunExecution, TaskRunStatus, TaskRunThreadBinding, TaskRunTurn,
    TaskRunTurnStatus, TaskWriteLock,
};
use std::collections::HashMap;

impl CrudStore {
    pub async fn get_task_runs(&self, task_id: &str) -> Result<Vec<TaskRun>> {
        self.task_view_store().get_task_runs(task_id).await
    }

    pub async fn get_task_run(&self, run_id: &str) -> Result<Option<TaskRun>> {
        pioneer_crud_tasks::task_event_context::TaskEventContextRepository::new(&self.connection)
            .get_task_run(run_id)
            .await
    }

    pub async fn claim_task_run_execution_for_dispatch(
        &self,
        run_id: &str,
        executor_kind: TaskExecutorKind,
        worker_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        self.task_execution_store()
            .claim_task_run_execution_for_dispatch(
                run_id,
                executor_kind,
                worker_id,
                now,
                lease_until,
            )
            .await
    }

    pub async fn claim_task_run_execution_for_recovery(
        &self,
        run_id: &str,
        executor_kind: TaskExecutorKind,
        worker_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        self.task_execution_store()
            .claim_task_run_execution_for_recovery(
                run_id,
                executor_kind,
                worker_id,
                now,
                lease_until,
            )
            .await
    }

    /// Revalidates a queued or waiting-review run and synchronizes its
    /// non-terminal occurrence in the same short transaction. A stale active
    /// snapshot can never reopen a terminal occurrence.
    pub async fn revalidate_and_sync_active_task_occurrence(
        &self,
        run_id: &str,
        expected_run_status: TaskRunStatus,
        now: i64,
    ) -> Result<bool> {
        self.task_execution_store()
            .revalidate_and_sync_active_task_occurrence(run_id, expected_run_status, now)
            .await
    }

    pub async fn list_terminal_task_occurrence_mismatches(
        &self,
        limit: u64,
    ) -> Result<Vec<TerminalTaskOccurrenceMismatch>> {
        self.task_occurrence_repository()
            .list_terminal_task_occurrence_mismatches(limit)
            .await
    }

    pub async fn scan_terminal_task_occurrence_mismatches(
        &self,
        after_occurrence_id: Option<&str>,
        limit: u64,
    ) -> Result<TerminalTaskOccurrenceMismatchPage> {
        self.task_occurrence_repository()
            .scan_terminal_task_occurrence_mismatches(after_occurrence_id, limit)
            .await
    }

    pub async fn reserve_execution_for_run(
        &self,
        run_id: &str,
        executor_kind: TaskExecutorKind,
        now: i64,
    ) -> Result<TaskRunExecution> {
        self.task_execution_store()
            .reserve_execution_for_run(run_id, executor_kind, now)
            .await
    }

    pub async fn heartbeat_execution(
        &self,
        execution_id: &str,
        heartbeat_at: i64,
        lease_until: Option<i64>,
    ) -> Result<Option<TaskRunExecution>> {
        self.task_execution_store()
            .heartbeat_execution(execution_id, heartbeat_at, lease_until)
            .await
    }

    pub async fn heartbeat_execution_for_agent_attempt(
        &self,
        execution_id: &str,
        expected_attempt_generation: i64,
        heartbeat_at: i64,
        lease_until: Option<i64>,
    ) -> Result<Option<TaskRunExecution>> {
        self.task_execution_store()
            .heartbeat_execution_for_agent_attempt(
                execution_id,
                expected_attempt_generation,
                heartbeat_at,
                lease_until,
            )
            .await
    }

    pub async fn record_agent_execution_progress(
        &self,
        execution_id: &str,
        expected_attempt_generation: i64,
        progress_frontier_json: &str,
        progress_at: i64,
        idle_deadline_at: Option<i64>,
    ) -> Result<bool> {
        self.turn_execution_store()
            .record_agent_execution_progress(
                execution_id,
                expected_attempt_generation,
                progress_frontier_json,
                progress_at,
                idle_deadline_at,
            )
            .await
    }

    pub async fn upsert_task_run_thread_binding(
        &self,
        binding: TaskRunThreadBinding,
    ) -> Result<TaskRunThreadBinding> {
        self.task_view_store()
            .upsert_task_run_thread_binding(binding)
            .await
    }

    pub async fn get_task_run_thread_binding(
        &self,
        id: &str,
    ) -> Result<Option<TaskRunThreadBinding>> {
        self.task_view_store().get_task_run_thread_binding(id).await
    }

    pub async fn get_task_run_primary_thread_binding(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskRunThreadBinding>> {
        self.task_view_store()
            .get_task_run_primary_thread_binding(run_id)
            .await
    }

    pub async fn get_task_run_thread_binding_by_thread(
        &self,
        thread_id: &str,
    ) -> Result<Option<TaskRunThreadBinding>> {
        self.task_view_store()
            .get_task_run_thread_binding_by_thread(thread_id)
            .await
    }

    pub async fn list_task_run_thread_bindings(
        &self,
        run_id: &str,
    ) -> Result<Vec<TaskRunThreadBinding>> {
        self.task_view_store()
            .list_task_run_thread_bindings(run_id)
            .await
    }

    pub async fn upsert_task_run_turn(&self, turn: TaskRunTurn) -> Result<TaskRunTurn> {
        pioneer_crud_tasks::child_repository::TaskChildWorkRepository::new(&self.connection)
            .upsert_task_run_turn(turn)
            .await
    }

    pub async fn get_task_run_turn(&self, id: &str) -> Result<Option<TaskRunTurn>> {
        pioneer_crud_tasks::child_repository::TaskChildWorkRepository::new(&self.connection)
            .get_task_run_turn(id)
            .await
    }

    pub async fn get_task_run_turn_by_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TaskRunTurn>> {
        self.task_view_store()
            .get_task_run_turn_by_turn(thread_id, turn_id)
            .await
    }

    pub async fn list_task_run_turns(&self, run_id: &str) -> Result<Vec<TaskRunTurn>> {
        self.task_view_store().list_task_run_turns(run_id).await
    }

    pub async fn get_latest_task_run_turn(&self, run_id: &str) -> Result<Option<TaskRunTurn>> {
        self.task_view_store()
            .get_latest_task_run_turn(run_id)
            .await
    }

    pub async fn update_task_run_turn_status(
        &self,
        id: &str,
        status: TaskRunTurnStatus,
        completed_at: Option<i64>,
    ) -> Result<Option<TaskRunTurn>> {
        self.task_view_store()
            .update_task_run_turn_status(id, status, completed_at)
            .await
    }

    pub async fn get_task_run_child_anchor(&self, run_id: &str) -> Result<TaskRunChildAnchor> {
        self.task_view_store()
            .get_task_run_child_anchor(run_id)
            .await
    }

    pub async fn count_task_run_executions_for_run(&self, run_id: &str) -> Result<u64> {
        self.task_view_store()
            .count_task_run_executions_for_run(run_id)
            .await
    }

    pub async fn load_task_runtime_invariant_snapshot(
        &self,
    ) -> Result<TaskRuntimeInvariantSnapshot> {
        pioneer_crud_tasks::runtime_invariants::load_task_runtime_invariant_snapshot(
            &self.connection,
        )
        .await
    }

    pub async fn claim_task_run_for_dispatch(
        &self,
        run_id: &str,
        claimed_at: i64,
    ) -> Result<Option<TaskRun>> {
        Ok(self
            .claim_task_run_for_dispatch_with_event(run_id, claimed_at)
            .await?
            .map(|(run, _)| run))
    }

    /// Claim and durable activity notification commit together. The caller may
    /// wake the existing event fanout only after this operation has committed.
    pub async fn claim_task_run_for_dispatch_with_event(
        &self,
        run_id: &str,
        claimed_at: i64,
    ) -> Result<Option<(TaskRun, AppendedTaskEvent)>> {
        self.task_event_store()
            .claim_task_run_for_dispatch_with_event(run_id, claimed_at)
            .await
    }

    /// Retry occurrence rebinding and admission share one writer boundary.
    /// A finish can observe either the failed source or an admitted successor,
    /// never a queued retry that has already stolen the source's occurrence.
    pub async fn claim_task_retry_for_dispatch_with_event(
        &self,
        run_id: &str,
        claimed_at: i64,
        occurrence: pioneer_protocol::TaskOccurrenceContract,
    ) -> Result<Option<(TaskRun, AppendedTaskEvent)>> {
        self.task_event_store()
            .claim_task_retry_for_dispatch_with_event(run_id, claimed_at, occurrence)
            .await
    }

    pub async fn append_task_run_started_once(
        &self,
        task_id: String,
        run_id: String,
        started_at: i64,
    ) -> Result<Option<AppendedTaskEvent>> {
        self.task_event_store()
            .append_task_run_started_once(task_id, run_id, started_at)
            .await
    }

    pub async fn list_task_runs_by_status(
        &self,
        status: TaskRunStatus,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        self.task_view_store()
            .list_task_runs_by_status(status, limit)
            .await
    }

    pub async fn list_due_retry_task_runs(&self, now: i64, limit: u64) -> Result<Vec<TaskRun>> {
        self.task_view_store()
            .list_due_retry_task_runs(now, limit)
            .await
    }

    pub async fn list_task_write_locks_by_run(&self, run_id: &str) -> Result<Vec<TaskWriteLock>> {
        self.task_view_store()
            .list_task_write_locks_by_run(run_id)
            .await
    }

    pub async fn list_active_task_write_locks(
        &self,
        workspace_id: &str,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskWriteLock>> {
        self.task_view_store()
            .list_active_task_write_locks(workspace_id, now, limit)
            .await
    }

    pub async fn list_stale_task_write_locks(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskWriteLock>> {
        self.task_view_store()
            .list_stale_task_write_locks(now, limit)
            .await
    }

    pub async fn get_task_run_turns_by_sources(
        &self,
        workspace: &str,
        sources: &[(String, String)],
    ) -> Result<HashMap<(String, String), TaskRunTurn>> {
        self.task_view_store()
            .get_task_run_turns_by_sources(workspace, sources)
            .await
    }
}
