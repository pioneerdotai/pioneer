use crate::TaskRuntimeResult;
use crate::event_bus::TaskEventBus;
use crate::projector::TaskProjector;
use crate::scheduler::TASK_EXECUTION_LEASE_SECONDS;
use anyhow::{Context, bail};
use async_trait::async_trait;
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    TaskError, TaskErrorClass, TaskEventPayload, TaskExecutorKind, TaskProgressDetails, TaskResult,
    TaskResultCandidate, TaskResultReviewEvent, TaskRun, TaskRunExecution, TaskRunThreadBinding,
    TaskRunTurn, TaskThreadLineage, TaskWriteLockStatus,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct TaskExecutionContext {
    pub workspace_id: String,
    pub task_id: String,
    pub execution_id: Option<String>,
    pub worker_id: String,
}

#[derive(Clone)]
pub struct TaskExecutionHandle {
    store: Arc<CrudStore>,
    projector: TaskProjector,
    event_bus: Arc<TaskEventBus>,
    task_id: String,
    run_id: String,
    agent_attempt_generation: Arc<OnceCell<Option<i64>>>,
    #[cfg(test)]
    pub(crate) terminal_preparation_barrier: Option<Arc<tokio::sync::Barrier>>,
}

impl TaskExecutionHandle {
    pub fn new(
        store: Arc<CrudStore>,
        event_bus: Arc<TaskEventBus>,
        task_id: String,
        run_id: String,
    ) -> Self {
        let projector = TaskProjector::new(store.clone());
        Self {
            store,
            projector,
            event_bus,
            task_id,
            run_id,
            agent_attempt_generation: Arc::new(OnceCell::new()),
            #[cfg(test)]
            terminal_preparation_barrier: None,
        }
    }

    pub fn task_id(&self) -> &str {
        self.task_id.as_str()
    }

    pub fn run_id(&self) -> &str {
        self.run_id.as_str()
    }

    /// Reclassifies durable executor-lifecycle writes independently from the
    /// scheduler or reconciler which created this handle. Those producers do
    /// maintenance discovery, but once a run is handed to its executor its
    /// lease, progress, and terminal transitions are control-plane state.
    pub fn with_critical_writes(mut self) -> Self {
        self.store = Arc::new(self.store.with_critical_writes());
        self.projector = TaskProjector::new(self.store.clone());
        self
    }

    pub async fn link_child_thread_with_runtime(
        &self,
        lineage: TaskThreadLineage,
        binding: TaskRunThreadBinding,
        task_run_turn: TaskRunTurn,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        self.ensure_execution_exists_for_child_runtime().await?;
        self.append_and_publish(
            vec![
                TaskEventPayload::TaskThreadLineageCreated {
                    task_id: binding.task_id.clone(),
                    run_id: binding.run_id.clone(),
                    lineage,
                },
                TaskEventPayload::TaskRunThreadBindingCreated { binding },
                TaskEventPayload::TaskRunTurnStarted { task_run_turn },
            ],
            event_timestamp_secs,
        )
        .await
    }

    pub async fn record_task_run_turn_failed(
        &self,
        task_run_turn: TaskRunTurn,
        error: Option<TaskError>,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        self.append_and_publish(
            vec![TaskEventPayload::TaskRunTurnFailed {
                task_run_turn,
                error,
            }],
            event_timestamp_secs,
        )
        .await
    }

    pub async fn record_task_run_turn_blocked(
        &self,
        task_run_turn: TaskRunTurn,
        error: Option<TaskError>,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        self.append_and_publish(
            vec![TaskEventPayload::TaskRunTurnBlocked {
                task_run_turn,
                error,
            }],
            event_timestamp_secs,
        )
        .await
    }

    pub async fn record_task_run_turn_completed(
        &self,
        task_run_turn: TaskRunTurn,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        self.append_and_publish(
            vec![TaskEventPayload::TaskRunTurnCompleted { task_run_turn }],
            event_timestamp_secs,
        )
        .await
    }

    pub async fn record_auto_accepted_result_candidate(
        &self,
        task_run_turn: TaskRunTurn,
        candidate: TaskResultCandidate,
        review_event: TaskResultReviewEvent,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        let review_event_id = review_event.id.clone();
        self.append_and_publish(
            vec![
                TaskEventPayload::TaskRunTurnCompleted { task_run_turn },
                TaskEventPayload::TaskResultCandidateCreated {
                    candidate: candidate.clone(),
                },
                TaskEventPayload::TaskResultReviewEventRecorded { review_event },
                TaskEventPayload::TaskResultCandidateAccepted {
                    candidate,
                    review_event_id,
                },
            ],
            event_timestamp_secs,
        )
        .await
    }

    pub async fn record_pending_review_result_candidate(
        &self,
        task_run_turn: TaskRunTurn,
        candidate: TaskResultCandidate,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        let task_id = candidate.task_id.clone();
        let run_id = candidate.run_id.clone();
        let candidate_id = candidate.id.clone();
        let mut events = vec![
            TaskEventPayload::TaskRunTurnCompleted { task_run_turn },
            TaskEventPayload::TaskResultCandidateCreated { candidate },
            TaskEventPayload::TaskRunEnteredReview {
                task_id,
                run_id,
                candidate_id,
                entered_at: event_timestamp_secs,
            },
        ];
        self.push_waiting_review_write_lock_extensions(&mut events, event_timestamp_secs)
            .await?;
        self.append_and_publish(events, event_timestamp_secs)
            .await?;
        self.update_occurrence_status(
            pioneer_protocol::TaskOccurrenceStatus::WaitingReview,
            None,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn mark_started(&self, started_at: i64) -> TaskRuntimeResult<()> {
        if let Some(appended) = self
            .store
            .append_task_run_started_once(self.task_id.clone(), self.run_id.clone(), started_at)
            .await?
        {
            self.event_bus.publish(appended).await;
        }
        self.update_occurrence_status(
            pioneer_protocol::TaskOccurrenceStatus::Running,
            None,
            started_at,
        )
        .await?;
        Ok(())
    }

    pub async fn progress(
        &self,
        message: impl Into<String>,
        details: Option<TaskProgressDetails>,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        let message = message.into();
        let frontier_message = message.chars().take(2_048).collect::<String>();
        let frontier = serde_json::json!({
            "message": frontier_message,
            "has_details": details.is_some(),
        })
        .to_string();
        self.append_and_publish(
            vec![TaskEventPayload::Progress {
                task_id: self.task_id.clone(),
                run_id: Some(self.run_id.clone()),
                message,
                details,
            }],
            event_timestamp_secs,
        )
        .await?;
        // TaskExecutionHandle is keyed by the stable TaskRun id for the task
        // event stream, while agent domain liveness is keyed by the separately
        // generated TaskRunExecution id. Resolve that exact row instead of
        // accidentally writing progress under the run id (which silently
        // becomes a no-op for durable agent executions).
        if let Some(execution) = self
            .store
            .load_execution_for_run(self.run_id.as_str())
            .await?
        {
            if let Some(attempt_generation) = self
                .pinned_agent_attempt_generation(execution.id.as_str())
                .await?
            {
                self.store
                    .record_agent_execution_progress(
                        execution.id.as_str(),
                        attempt_generation,
                        frontier.as_str(),
                        event_timestamp_secs,
                        Some(event_timestamp_secs.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn complete_run(
        &self,
        result: Option<TaskResult>,
        completed_at: i64,
    ) -> TaskRuntimeResult<()> {
        self.commit_terminal_run(TaskEventPayload::RunCompleted {
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            result,
            completed_at,
        })
        .await
    }

    pub async fn fail_run(
        &self,
        error: Option<TaskError>,
        completed_at: i64,
    ) -> TaskRuntimeResult<()> {
        if task_error_is_cancellation(error.as_ref()) {
            return self
                .cancel_run(error.as_ref().map(|e| e.message.clone()), completed_at)
                .await;
        }
        self.commit_terminal_run(TaskEventPayload::RunFailed {
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            error,
            completed_at,
        })
        .await
    }

    pub async fn block_run(
        &self,
        error: Option<TaskError>,
        blocked_at: i64,
    ) -> TaskRuntimeResult<()> {
        self.commit_terminal_run(TaskEventPayload::RunBlocked {
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            error,
            blocked_at,
        })
        .await
    }

    pub async fn cancel_run(
        &self,
        reason: Option<String>,
        cancelled_at: i64,
    ) -> TaskRuntimeResult<()> {
        self.commit_terminal_run(TaskEventPayload::RunCancelled {
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            reason,
            cancelled_at,
        })
        .await
    }

    async fn commit_terminal_run(&self, terminal: TaskEventPayload) -> TaskRuntimeResult<()> {
        let prepared = self
            .store
            .prepare_task_terminal_transition(terminal)
            .await?
            .with_pinned_agent_attempt(self.agent_attempt_generation.get().copied().flatten())?;
        #[cfg(test)]
        if let Some(barrier) = self.terminal_preparation_barrier.as_ref() {
            barrier.wait().await;
        }
        let outcome = self.store.commit_task_terminal_transition(prepared).await?;
        self.event_bus.publish_many(outcome.events).await;
        Ok(())
    }

    async fn update_occurrence_status(
        &self,
        status: pioneer_protocol::TaskOccurrenceStatus,
        terminal_reason: Option<String>,
        now: i64,
    ) -> TaskRuntimeResult<()> {
        let mut occurrence = self
            .store
            .get_task_occurrence_contract_by_run(self.run_id.as_str())
            .await?
            .with_context(|| {
                format!(
                    "Task run `{}` has no durable occurrence contract",
                    self.run_id
                )
            })?;
        occurrence.status = status;
        occurrence.terminal_reason = terminal_reason;
        self.store
            .upsert_task_occurrence_contract(&occurrence, now)
            .await?;
        Ok(())
    }

    pub async fn heartbeat_execution(
        &self,
        heartbeat_at: i64,
        lease_until: Option<i64>,
    ) -> TaskRuntimeResult<()> {
        if let Some(execution) = self
            .store
            .load_execution_for_run(self.run_id.as_str())
            .await?
            && !execution.status.is_terminal()
        {
            if let Some(attempt_generation) = self
                .pinned_agent_attempt_generation(execution.id.as_str())
                .await?
            {
                let _ = self
                    .store
                    .heartbeat_execution_for_agent_attempt(
                        execution.id.as_str(),
                        attempt_generation,
                        heartbeat_at,
                        lease_until,
                    )
                    .await?;
            } else {
                let _ = self
                    .store
                    .heartbeat_execution(execution.id.as_str(), heartbeat_at, lease_until)
                    .await?;
            }
        }
        Ok(())
    }

    async fn pinned_agent_attempt_generation(
        &self,
        execution_id: &str,
    ) -> TaskRuntimeResult<Option<i64>> {
        let attempt = self
            .agent_attempt_generation
            .get_or_try_init(|| async {
                Ok::<_, anyhow::Error>(
                    pioneer_crud::load_agent_execution_resource_state(
                        &self.store.database_connection(),
                        execution_id,
                    )
                    .await?
                    .map(|state| state.attempt_generation),
                )
            })
            .await?;
        Ok(*attempt)
    }

    pub async fn load_execution(&self) -> TaskRuntimeResult<Option<TaskRunExecution>> {
        self.store
            .load_execution_for_run(self.run_id.as_str())
            .await
    }

    async fn ensure_execution_exists_for_child_runtime(&self) -> TaskRuntimeResult<()> {
        if self
            .store
            .load_execution_for_run(self.run_id.as_str())
            .await?
            .is_none()
        {
            bail!(
                "cannot record child runtime for task run `{}` without task run execution",
                self.run_id
            );
        }
        Ok(())
    }

    async fn append_and_publish(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        let appended = self
            .projector
            .append_events(events, event_timestamp_secs)
            .await?;
        self.event_bus.publish_many(appended).await;
        Ok(())
    }

    async fn push_waiting_review_write_lock_extensions(
        &self,
        events: &mut Vec<TaskEventPayload>,
        extended_at: i64,
    ) -> TaskRuntimeResult<()> {
        for mut lock in self
            .store
            .list_task_write_locks_by_run(self.run_id.as_str())
            .await?
            .into_iter()
            .filter(|lock| lock.status == TaskWriteLockStatus::Acquired)
        {
            if lock.expires_at.is_none() {
                continue;
            }
            lock.expires_at = None;
            lock.reason = Some("write lock held while task waits for review".to_owned());
            lock.updated_at = extended_at;
            events.push(TaskEventPayload::WriteLockExtended { lock, extended_at });
        }
        Ok(())
    }
}

fn task_error_is_cancellation(error: Option<&TaskError>) -> bool {
    let Some(error) = error else {
        return false;
    };
    if error.class == TaskErrorClass::Cancelled {
        return true;
    }
    let code = error.code.to_ascii_lowercase();
    matches!(
        code.as_str(),
        "task_cancelled" | "task_run_cancelled" | "child_turn_cancelled" | "cancelled"
    ) || code.contains("cancel")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExecutorStartOutcome {
    Started,
    Queued,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExecutorRecoveryOutcome {
    Recovered,
    AlreadyRunning,
    LeftUnchanged,
}

#[async_trait]
pub trait TaskExecutor: Send + Sync {
    fn kind(&self) -> TaskExecutorKind;

    async fn start_run(
        &self,
        context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome>;

    async fn cancel_run(
        &self,
        context: TaskExecutionContext,
        run_id: &str,
        reason: &str,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()>;

    async fn recover_run(
        &self,
        _context: TaskExecutionContext,
        _run: TaskRun,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
        Ok(TaskExecutorRecoveryOutcome::LeftUnchanged)
    }
}

#[derive(Default)]
pub struct TaskExecutorRegistry {
    executors: RwLock<HashMap<&'static str, Arc<dyn TaskExecutor>>>,
}

impl TaskExecutorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, executor: Arc<dyn TaskExecutor>) {
        self.executors
            .write()
            .await
            .insert(executor_key(executor.kind()), executor);
    }

    pub async fn get(&self, kind: TaskExecutorKind) -> Option<Arc<dyn TaskExecutor>> {
        self.executors.read().await.get(executor_key(kind)).cloned()
    }
}

fn executor_key(kind: TaskExecutorKind) -> &'static str {
    match kind {
        TaskExecutorKind::Agent => "agent",
        TaskExecutorKind::Tool => "tool",
        TaskExecutorKind::Workflow => "workflow",
        TaskExecutorKind::Webhook => "webhook",
        TaskExecutorKind::System => "system",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{TaskArtifact, TaskDeliveryFormat, TaskValue};
    use std::collections::BTreeMap;

    #[test]
    fn summary_delivery_snapshot_drops_full_data_and_artifacts() {
        let result = TaskResult {
            summary: Some("safe summary".to_owned()),
            data: Some(TaskValue::Object(BTreeMap::from([(
                "secret".to_owned(),
                TaskValue::String("must-not-cross".to_owned()),
            )]))),
            artifacts: vec![TaskArtifact {
                artifact_id: Some("artifact-1".to_owned()),
                version_id: Some("version-1".to_owned()),
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some("run-1".to_owned()),
        };

        let projected = task_delivery_result_snapshot(result, TaskDeliveryFormat::Summary);
        assert_eq!(projected.summary.as_deref(), Some("safe summary"));
        assert!(projected.data.is_none());
        assert!(projected.artifacts.is_empty());
        assert_eq!(projected.completed_by_run_id.as_deref(), Some("run-1"));
    }
}
