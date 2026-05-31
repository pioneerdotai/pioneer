use crate::TaskRuntimeResult;
use crate::event_bus::TaskEventBus;
use crate::projector::TaskProjector;
use anyhow::bail;
use async_trait::async_trait;
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    TaskCompletionBehavior, TaskDeliveriesParams, TaskDeliveryMode, TaskDeliveryStatus, TaskError,
    TaskErrorClass, TaskEventPayload, TaskExecutorKind, TaskGetResponse, TaskProgressDetails,
    TaskResult, TaskResultCandidate, TaskResultReviewEvent, TaskRetryBackoffKind, TaskRun,
    TaskRunExecution, TaskRunExecutionStatus, TaskRunStatus, TaskRunThreadBinding, TaskRunTurn,
    TaskThreadLineage, TaskWriteLockStatus, generate_id,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const ID_LEN: usize = 21;

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
        }
    }

    pub fn task_id(&self) -> &str {
        self.task_id.as_str()
    }

    pub fn run_id(&self) -> &str {
        self.run_id.as_str()
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
        self.append_and_publish(events, event_timestamp_secs).await
    }

    pub async fn mark_started(&self, started_at: i64) -> TaskRuntimeResult<()> {
        if let Some(appended) = self
            .store
            .append_task_run_started_once(self.task_id.clone(), self.run_id.clone(), started_at)
            .await?
        {
            self.event_bus.publish(appended).await;
        }
        Ok(())
    }

    pub async fn progress(
        &self,
        message: impl Into<String>,
        details: Option<TaskProgressDetails>,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        self.append_and_publish(
            vec![TaskEventPayload::Progress {
                task_id: self.task_id.clone(),
                run_id: Some(self.run_id.clone()),
                message: message.into(),
                details,
            }],
            event_timestamp_secs,
        )
        .await
    }

    pub async fn complete_run(
        &self,
        result: Option<TaskResult>,
        completed_at: i64,
    ) -> TaskRuntimeResult<()> {
        if self.run_is_terminal().await? {
            return Ok(());
        }
        if self.task_is_terminal().await? {
            return self
                .release_locks_only(
                    TaskWriteLockStatus::Released,
                    Some("run completed after task terminal".to_owned()),
                    completed_at,
                )
                .await;
        }

        let mut events = vec![TaskEventPayload::RunCompleted {
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            result: result.clone(),
            completed_at,
        }];
        self.push_write_lock_released(
            &mut events,
            TaskWriteLockStatus::Released,
            Some("run completed".to_owned()),
            completed_at,
        )
        .await?;
        if self.should_emit_terminal_task_event().await? {
            events.push(TaskEventPayload::TaskCompleted {
                task_id: self.task_id.clone(),
                result: result.clone(),
                completed_at,
            });
        } else {
            self.push_active_schedule_after_terminal(&mut events, completed_at)
                .await?;
        }
        self.push_delivery_queued(&mut events, completed_at, result.clone(), None)
            .await?;
        self.append_and_publish(events, completed_at).await?;
        self.mark_execution_terminal(
            TaskRunExecutionStatus::Succeeded,
            completed_at,
            result.as_ref(),
            None,
        )
        .await
    }

    pub async fn fail_run(
        &self,
        error: Option<TaskError>,
        completed_at: i64,
    ) -> TaskRuntimeResult<()> {
        if task_error_is_cancellation(error.as_ref()) {
            return self
                .cancel_run(
                    error.as_ref().map(|error| error.message.clone()),
                    completed_at,
                )
                .await;
        }
        if self.run_is_terminal().await? {
            return Ok(());
        }
        if self.task_is_terminal().await? {
            return self
                .release_locks_only(
                    TaskWriteLockStatus::Released,
                    Some("run failed after task terminal".to_owned()),
                    completed_at,
                )
                .await;
        }

        let mut events = vec![TaskEventPayload::RunFailed {
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            error: error.clone(),
            completed_at,
        }];
        self.push_write_lock_released(
            &mut events,
            TaskWriteLockStatus::Released,
            Some("run failed".to_owned()),
            completed_at,
        )
        .await?;
        if self
            .push_retry_after_failure(&mut events, error.clone(), completed_at)
            .await?
        {
            return self.append_and_publish(events, completed_at).await;
        }
        if self.should_emit_terminal_task_event().await? {
            events.push(TaskEventPayload::TaskFailed {
                task_id: self.task_id.clone(),
                error: error.clone(),
                completed_at,
            });
        } else {
            self.push_active_schedule_after_terminal(&mut events, completed_at)
                .await?;
        }
        self.push_delivery_queued(&mut events, completed_at, None, error.clone())
            .await?;
        self.append_and_publish(events, completed_at).await?;
        self.mark_execution_terminal(
            failure_execution_status(error.as_ref()),
            completed_at,
            None,
            error.as_ref(),
        )
        .await
    }

    pub async fn cancel_run(
        &self,
        reason: Option<String>,
        cancelled_at: i64,
    ) -> TaskRuntimeResult<()> {
        if self.run_is_terminal().await? {
            return Ok(());
        }
        if self.task_is_terminal().await? {
            return self
                .release_locks_only(TaskWriteLockStatus::Cancelled, reason.clone(), cancelled_at)
                .await;
        }

        let mut events = vec![TaskEventPayload::RunCancelled {
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            reason: reason.clone(),
            cancelled_at,
        }];
        self.push_write_lock_released(
            &mut events,
            TaskWriteLockStatus::Cancelled,
            reason.clone(),
            cancelled_at,
        )
        .await?;
        if self.should_emit_terminal_task_event().await? {
            events.push(TaskEventPayload::TaskCancelled {
                task_id: self.task_id.clone(),
                reason: reason.clone(),
                completed_at: cancelled_at,
            });
        } else {
            self.push_active_schedule_after_terminal(&mut events, cancelled_at)
                .await?;
        }
        let error = reason.as_ref().map(|message| TaskError {
            code: "task_run_cancelled".to_owned(),
            message: message.clone(),
            class: TaskErrorClass::Cancelled,
            details: None,
            failed_run_id: Some(self.run_id.clone()),
        });
        self.push_delivery_queued(&mut events, cancelled_at, None, error)
            .await?;
        self.append_and_publish(events, cancelled_at).await?;
        let terminal_error = reason.as_ref().map(|message| TaskError {
            code: "task_run_cancelled".to_owned(),
            message: message.clone(),
            class: TaskErrorClass::Cancelled,
            details: None,
            failed_run_id: Some(self.run_id.clone()),
        });
        self.mark_execution_terminal(
            TaskRunExecutionStatus::Cancelled,
            cancelled_at,
            None,
            terminal_error.as_ref(),
        )
        .await
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
            let _ = self
                .store
                .heartbeat_execution(execution.id.as_str(), heartbeat_at, lease_until)
                .await?;
        }
        Ok(())
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

    async fn release_locks_only(
        &self,
        status: TaskWriteLockStatus,
        reason: Option<String>,
        released_at: i64,
    ) -> TaskRuntimeResult<()> {
        let mut events = Vec::new();
        self.push_write_lock_released(&mut events, status, reason, released_at)
            .await?;
        if events.is_empty() {
            return Ok(());
        }
        self.append_and_publish(events, released_at).await
    }

    async fn task_is_terminal(&self) -> TaskRuntimeResult<bool> {
        let Some(task_response) = self.store.get_task(self.task_id.as_str()).await? else {
            return Ok(true);
        };
        Ok(task_response.task.status.is_terminal())
    }

    async fn run_is_terminal(&self) -> TaskRuntimeResult<bool> {
        let Some(run) = self.store.get_task_run(self.run_id.as_str()).await? else {
            return Ok(true);
        };
        Ok(run.status.is_terminal())
    }

    async fn should_emit_terminal_task_event(&self) -> TaskRuntimeResult<bool> {
        let Some(task_response) = self.store.get_task(self.task_id.as_str()).await? else {
            return Ok(false);
        };
        Ok(task_response
            .task
            .lifecycle_policy
            .as_ref()
            .map(|policy| {
                matches!(
                    policy.completion,
                    TaskCompletionBehavior::CompleteOnTerminalRun
                )
            })
            .unwrap_or(true))
    }

    async fn push_active_schedule_after_terminal(
        &self,
        events: &mut Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<()> {
        let Some(task_response) = self.store.get_task(self.task_id.as_str()).await? else {
            return Ok(());
        };
        let Some(run) = task_response
            .runs
            .iter()
            .rev()
            .find(|run| run.id == self.run_id)
        else {
            return Ok(());
        };
        let Some(trigger_id) = run.trigger_id.as_deref() else {
            return Ok(());
        };
        if let Some(mut trigger) = task_response
            .triggers
            .into_iter()
            .find(|trigger| trigger.id == trigger_id)
        {
            trigger.updated_at = event_timestamp_secs;
            events.push(TaskEventPayload::TaskRescheduled {
                task_id: self.task_id.clone(),
                trigger,
                rescheduled_at: event_timestamp_secs,
                reason: pioneer_protocol::TaskRescheduleReason::RunTerminalStatusRefresh,
            });
        }
        Ok(())
    }

    async fn push_delivery_queued(
        &self,
        events: &mut Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        result_snapshot: Option<TaskResult>,
        error_snapshot: Option<TaskError>,
    ) -> TaskRuntimeResult<()> {
        let Some(task_response) = self.store.get_task(self.task_id.as_str()).await? else {
            return Ok(());
        };
        let Some(delivery) = delivery_for_terminal_run(
            &task_response,
            self.run_id.as_str(),
            event_timestamp_secs,
            result_snapshot,
            error_snapshot,
        ) else {
            return Ok(());
        };
        let existing = self
            .store
            .list_task_deliveries(TaskDeliveriesParams {
                workspace_id: task_response.task.workspace_id.clone(),
                task_id: Some(task_response.task.id.clone()),
                run_id: Some(self.run_id.clone()),
                statuses: Vec::new(),
                limit: Some(100),
            })
            .await?;
        if existing
            .deliveries
            .iter()
            .any(|existing| existing.delivery_key == delivery.delivery_key)
        {
            return Ok(());
        }
        events.push(TaskEventPayload::DeliveryQueued { delivery });
        Ok(())
    }

    async fn push_retry_after_failure(
        &self,
        events: &mut Vec<TaskEventPayload>,
        error: Option<TaskError>,
        completed_at: i64,
    ) -> TaskRuntimeResult<bool> {
        let Some(task_response) = self.store.get_task(self.task_id.as_str()).await? else {
            return Ok(false);
        };
        let Some(failed_run) = task_response
            .runs
            .iter()
            .rev()
            .find(|run| run.id == self.run_id)
            .cloned()
        else {
            return Ok(false);
        };
        let Some(policy) = task_response.task.retry_policy.as_ref() else {
            return Ok(false);
        };
        let error_class = error
            .as_ref()
            .map(|error| error.class)
            .unwrap_or(pioneer_protocol::TaskErrorClass::Unknown);
        if !policy.retry_on.iter().any(|class| *class == error_class) {
            return Ok(false);
        }
        if policy.max_attempts <= failed_run.attempt_number {
            events.push(TaskEventPayload::RunRetryExhausted {
                task_id: self.task_id.clone(),
                run_group_id: failed_run.run_group_id.clone(),
                final_run_id: failed_run.id.clone(),
                error,
                exhausted_at: completed_at,
            });
            return Ok(false);
        }

        let next_attempt = failed_run.attempt_number.saturating_add(1);
        let delay_seconds = retry_delay_seconds(policy, next_attempt)?;
        let ready_at = completed_at.saturating_add(delay_seconds);
        let retry_run = TaskRun {
            id: generate_id(ID_LEN),
            task_id: self.task_id.clone(),
            trigger_id: failed_run.trigger_id.clone(),
            parent_run_id: Some(failed_run.id.clone()),
            run_group_id: failed_run.run_group_id.clone(),
            attempt_number: next_attempt,
            retry_of_run_id: Some(failed_run.id.clone()),
            ready_at: Some(ready_at),
            run_number: task_response
                .runs
                .iter()
                .map(|run| run.run_number)
                .max()
                .unwrap_or(0)
                .saturating_add(1),
            status: TaskRunStatus::Queued,
            executor_kind: failed_run.executor_kind,
            started_at: None,
            completed_at: None,
            heartbeat_at: None,
            locked_by: None,
            lock_expires_at: None,
            result: None,
            error: None,
            created_at: completed_at,
            updated_at: completed_at,
        };
        let agent_spec = task_response
            .agent_specs
            .iter()
            .rev()
            .find(|spec| spec.run_id.as_deref() == Some(failed_run.id.as_str()))
            .or_else(|| {
                task_response
                    .agent_specs
                    .iter()
                    .rev()
                    .find(|spec| spec.run_id.is_none())
            })
            .cloned()
            .map(|mut spec| {
                spec.run_id = Some(retry_run.id.clone());
                spec.updated_at = completed_at;
                spec
            });
        events.push(TaskEventPayload::TaskQueued {
            task_id: self.task_id.clone(),
            run_id: Some(retry_run.id.clone()),
        });
        events.push(TaskEventPayload::RunRetryScheduled {
            task_id: self.task_id.clone(),
            failed_run_id: failed_run.id,
            retry_run: retry_run.clone(),
            next_attempt_at: ready_at,
            reason: error,
        });
        events.push(TaskEventPayload::RunCreated {
            run: retry_run,
            agent_spec,
        });
        Ok(true)
    }

    async fn push_write_lock_released(
        &self,
        events: &mut Vec<TaskEventPayload>,
        status: TaskWriteLockStatus,
        reason: Option<String>,
        released_at: i64,
    ) -> TaskRuntimeResult<()> {
        for mut lock in self
            .store
            .list_task_write_locks_by_run(self.run_id.as_str())
            .await?
            .into_iter()
            .filter(|lock| {
                matches!(
                    lock.status,
                    TaskWriteLockStatus::Pending
                        | TaskWriteLockStatus::Acquired
                        | TaskWriteLockStatus::Blocked
                )
            })
        {
            lock.status = status;
            lock.released_at = Some(released_at);
            lock.reason = reason.clone();
            lock.updated_at = released_at;
            events.push(TaskEventPayload::WriteLockReleased { lock, released_at });
        }
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

    async fn mark_execution_terminal(
        &self,
        status: TaskRunExecutionStatus,
        completed_at: i64,
        result: Option<&TaskResult>,
        error: Option<&TaskError>,
    ) -> TaskRuntimeResult<()> {
        if let Some(execution) = self
            .store
            .load_execution_for_run(self.run_id.as_str())
            .await?
            && !execution.status.is_terminal()
        {
            let _ = self
                .store
                .mark_execution_terminal(execution.id.as_str(), status, completed_at, result, error)
                .await?;
        }
        Ok(())
    }
}

fn retry_delay_seconds(
    policy: &pioneer_protocol::TaskRetryPolicy,
    next_attempt_number: u32,
) -> TaskRuntimeResult<i64> {
    let delay = match policy.backoff {
        TaskRetryBackoffKind::None => 0,
        TaskRetryBackoffKind::Fixed => policy.initial_delay_seconds.unwrap_or(0),
        TaskRetryBackoffKind::Exponential => {
            let initial = policy.initial_delay_seconds.unwrap_or(1).max(1);
            let exponent = next_attempt_number.saturating_sub(2).min(30);
            let multiplier = 1_i64.checked_shl(exponent).unwrap_or(i64::MAX);
            initial.saturating_mul(multiplier)
        }
    };
    let capped = policy
        .max_delay_seconds
        .map(|max_delay| delay.min(max_delay.max(0)))
        .unwrap_or(delay);
    Ok(capped.max(0))
}

fn delivery_for_terminal_run(
    task_response: &TaskGetResponse,
    run_id: &str,
    event_timestamp_secs: i64,
    result_snapshot: Option<TaskResult>,
    error_snapshot: Option<TaskError>,
) -> Option<pioneer_protocol::TaskDelivery> {
    let policy = task_response.task.delivery_policy.as_ref()?;
    if policy.mode == TaskDeliveryMode::None {
        return None;
    }
    let target_thread_id = match policy.mode {
        TaskDeliveryMode::OwnerThread => task_response
            .task
            .owner_id
            .clone()
            .filter(|_| task_response.task.owner_kind == pioneer_protocol::TaskOwnerKind::Thread)
            .or_else(|| task_response.task.created_by_thread_id.clone()),
        TaskDeliveryMode::Thread => policy.thread_id.clone(),
        _ => None,
    };
    let target_user_id = (policy.mode == TaskDeliveryMode::UserNotification)
        .then(|| task_response.task.owner_id.clone())
        .flatten();
    let webhook_url = (policy.mode == TaskDeliveryMode::Webhook)
        .then(|| policy.webhook_url.clone())
        .flatten();
    if matches!(
        policy.mode,
        TaskDeliveryMode::OwnerThread | TaskDeliveryMode::Thread
    ) && target_thread_id.is_none()
    {
        return None;
    }
    if policy.mode == TaskDeliveryMode::Webhook && webhook_url.is_none() {
        return None;
    }
    let run = task_response.runs.iter().rev().find(|run| run.id == run_id);
    let result_snapshot = if policy.include_result {
        result_snapshot
            .or_else(|| run.and_then(|run| run.result.clone()))
            .or_else(|| task_response.task.result.clone())
    } else {
        None
    };
    let error_snapshot = error_snapshot.or_else(|| {
        run.and_then(|run| run.error.clone())
            .or_else(|| task_response.task.error.clone())
    });
    let target = target_thread_id
        .clone()
        .or_else(|| target_user_id.clone())
        .or_else(|| webhook_url.clone())
        .unwrap_or_else(|| "none".to_owned());
    let delivery_key = format!(
        "{}:{}:{}:{}",
        task_response.task.id,
        run_id,
        delivery_mode_key(policy.mode),
        target
    );
    Some(pioneer_protocol::TaskDelivery {
        id: generate_id(ID_LEN),
        workspace_id: task_response.task.workspace_id.clone(),
        task_id: task_response.task.id.clone(),
        run_id: run_id.to_owned(),
        delivery_key,
        mode: policy.mode,
        target_thread_id,
        target_user_id,
        webhook_url: webhook_url.clone(),
        webhook_url_fingerprint: webhook_url.map(|url| sha256_hex(url.as_bytes())),
        status: TaskDeliveryStatus::Pending,
        next_attempt_at: Some(event_timestamp_secs),
        attempt_count: 0,
        max_attempts: if policy.mode == TaskDeliveryMode::Webhook {
            3
        } else {
            1
        },
        result_snapshot,
        error_snapshot,
        delivered_turn_id: None,
        delivered_notification_id: None,
        delivered_at: None,
        last_error: None,
        created_at: event_timestamp_secs,
        updated_at: event_timestamp_secs,
    })
}

fn delivery_mode_key(mode: TaskDeliveryMode) -> &'static str {
    match mode {
        TaskDeliveryMode::None => "none",
        TaskDeliveryMode::OwnerThread => "owner_thread",
        TaskDeliveryMode::Thread => "thread",
        TaskDeliveryMode::UserNotification => "user_notification",
        TaskDeliveryMode::Webhook => "webhook",
    }
}

fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hex::encode(hasher.finalize())
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

fn failure_execution_status(error: Option<&TaskError>) -> TaskRunExecutionStatus {
    match error.map(|error| error.class) {
        Some(TaskErrorClass::Timeout) => TaskRunExecutionStatus::TimedOut,
        _ => TaskRunExecutionStatus::Failed,
    }
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
