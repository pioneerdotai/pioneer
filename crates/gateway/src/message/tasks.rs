use super::*;
use anyhow::{Result, bail};
use pioneer_crud::TaskRunOccurrenceTerminalizationOutcome;
use pioneer_protocol::{
    ItemUpdatedNotification, TaskAttachmentMode, TaskDeliveryStatus, TaskEventPayload,
    TaskGetResponse, TaskRescheduleReason, TaskTriggerKind,
};
use serde_json::json;

struct TaskTimelineChangedTarget {
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    run_id: Option<String>,
}

impl MessageProcessor {
    pub(super) async fn reconcile_terminal_task_child_turns(&self, limit: u64) -> Result<usize> {
        let turns = self
            .crud_store
            .list_unreconciled_terminal_task_child_turns(limit)
            .await?;
        let mut reconciled = 0usize;
        let mut errors = Vec::new();
        for turn in turns {
            let result = match turn.status {
                TurnStatus::Completed => {
                    self.task_agent_executor
                        .reconcile_child_turn_completed(
                            turn.thread_id.as_str(),
                            turn.turn_id.as_str(),
                        )
                        .await
                }
                TurnStatus::Failed => {
                    let error = turn.error.as_deref().unwrap_or("child turn failed");
                    self.task_agent_executor
                        .reconcile_child_turn_failed(
                            turn.thread_id.as_str(),
                            turn.turn_id.as_str(),
                            error,
                        )
                        .await
                }
                TurnStatus::Interrupted => {
                    let reason = turn
                        .error
                        .as_deref()
                        .unwrap_or("child turn was interrupted");
                    self.task_agent_executor
                        .reconcile_child_turn_cancelled(
                            turn.thread_id.as_str(),
                            turn.turn_id.as_str(),
                            reason,
                        )
                        .await
                }
                TurnStatus::Blocked => {
                    let reason = turn.error.as_deref().unwrap_or("child turn was blocked");
                    self.task_agent_executor
                        .reconcile_child_turn_blocked(
                            turn.thread_id.as_str(),
                            turn.turn_id.as_str(),
                            reason,
                        )
                        .await
                }
                TurnStatus::InProgress => continue,
            };
            match result {
                Ok(true) => {
                    if turn.status != TurnStatus::Blocked {
                        if let Err(error) = self
                            .crud_store
                            .delete_turn_llm_context_for_turn(turn.turn_id.as_str())
                            .await
                        {
                            warn!(
                                thread_id = turn.thread_id.as_str(),
                                turn_id = turn.turn_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to delete child Turn LLM context after durable task reconciliation"
                            );
                        }
                        self.delete_turn_runtime_snapshot_for_closed_turn(
                            turn.thread_id.as_str(),
                            turn.turn_id.as_str(),
                            "durably reconciled",
                        )
                        .await;
                    }
                    reconciled = reconciled.saturating_add(1);
                }
                Ok(false) => {}
                Err(error) => {
                    errors.push(format!("{}/{}: {error:#}", turn.thread_id, turn.turn_id))
                }
            }
        }
        if !errors.is_empty() {
            bail!(
                "terminal task child reconciliation failed: {}",
                errors.join("; ")
            );
        }
        Ok(reconciled)
    }

    pub(super) async fn reconcile_terminal_task_run_occurrence_turns(
        &self,
        limit: u64,
    ) -> Result<usize> {
        let run_ids = self
            .crud_store
            .list_mismatched_terminal_task_run_occurrence_ids(limit)
            .await?;
        let mut reconciled = 0usize;
        for run_id in run_ids {
            let Some(run) = self.crud_store.get_task_run(run_id.as_str()).await? else {
                continue;
            };
            match self
                .mark_task_run_occurrence_turn_terminal(run.id.as_str())
                .await?
            {
                TaskRunOccurrenceTerminalizationOutcome::Changed => {
                    reconciled = reconciled.saturating_add(1);
                }
                TaskRunOccurrenceTerminalizationOutcome::AlreadyConsistent => {}
                TaskRunOccurrenceTerminalizationOutcome::NotFound => warn!(
                    run_id = run.id.as_str(),
                    "TaskRun occurrence reconciliation could not find the canonical occurrence Turn"
                ),
                TaskRunOccurrenceTerminalizationOutcome::InvalidBinding => warn!(
                    run_id = run.id.as_str(),
                    "TaskRun occurrence reconciliation rejected a non-task_run canonical Turn binding"
                ),
            }
        }
        Ok(reconciled)
    }

    pub(super) async fn emit_task_event(
        &self,
        event: pioneer_crud::AppendedTaskEvent,
    ) -> Result<()> {
        let task_id = event.payload.task_id().to_owned();
        let Some(task_response) = self.crud_store.get_task(task_id.as_str()).await? else {
            return Ok(());
        };
        let context = pioneer_protocol::TaskNotificationContext {
            workspace_id: task_response.task.workspace_id.clone(),
            task_id: task_response.task.id.clone(),
            run_id: event.payload.run_id().map(str::to_owned),
            parent_task_id: task_response.task.parent_task_id.clone(),
            root_task_id: task_response.task.root_task_id.clone(),
            thread_id: event.payload.thread_id().map(str::to_owned),
            turn_id: event.payload.turn_id().map(str::to_owned),
            event_id: event.id,
            sequence: event.sequence,
        };
        let workspace_id = context.workspace_id.clone();
        let notification_task_id = context.task_id.clone();
        let is_progress_event = matches!(event.payload, TaskEventPayload::Progress { .. });
        let is_terminal_event = event.payload.is_terminal();
        let awaits_origin_thread_delivery = match event.payload.run_id() {
            Some(run_id) => {
                self.task_run_awaits_origin_thread_delivery(&task_response, run_id)
                    .await?
            }
            None => false,
        };
        if is_progress_event {
            self.publish_parent_task_progress_snapshot(&task_response, &event.payload)
                .await?;
        } else if is_terminal_event {
            self.flush_parent_task_progress_snapshot(&task_response, &event.payload)
                .await;
        }
        let timeline_changed = if is_progress_event {
            None
        } else {
            self.task_timeline_changed_target(&task_response, &event.payload)
                .await
        };
        let refresh_parent_anchor = !is_progress_event
            && !awaits_origin_thread_delivery
            && should_refresh_parent_task_anchor(&task_response, &event.payload);
        if refresh_parent_anchor {
            self.refresh_parent_task_anchor(&task_response).await?;
        }
        if !awaits_origin_thread_delivery
            && let Some(notification) = timeline_changed.as_ref()
            && task_response.task.created_by_turn_id.as_deref()
                != Some(notification.turn_id.as_str())
        {
            self.refresh_task_anchor_in_turn(
                &task_response,
                notification.thread_id.as_str(),
                notification.turn_id.as_str(),
                notification.run_id.as_deref(),
            )
            .await?;
        }

        match event.payload {
            TaskEventPayload::TaskCreated { task } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_CREATED,
                    &json!({
                        "context": context,
                        "task": crate::task_projection::project_task(&task),
                    }),
                )
                .await;
            }
            TaskEventPayload::TriggerCreated { trigger } => {
                if trigger.kind() != TaskTriggerKind::Immediate {
                    self.send_notification_to_task_workspace_connections(
                        notification_task_id.as_str(),
                        workspace_id.as_str(),
                        events::TASK_SCHEDULED,
                        &json!({
                            "context": context,
                            "trigger": crate::task_projection::project_trigger(&trigger),
                        }),
                    )
                    .await;
                }
            }
            TaskEventPayload::TaskScheduled { trigger_id, .. } => {
                if let Some(trigger) = task_response
                    .triggers
                    .into_iter()
                    .find(|trigger| trigger.id == trigger_id)
                {
                    self.send_notification_to_task_workspace_connections(
                        notification_task_id.as_str(),
                        workspace_id.as_str(),
                        events::TASK_SCHEDULED,
                        &json!({
                            "context": context,
                            "trigger": crate::task_projection::project_trigger(&trigger),
                        }),
                    )
                    .await;
                }
            }
            TaskEventPayload::TaskQueued { .. } => {
                let run = context.run_id.as_ref().and_then(|run_id| {
                    task_response
                        .runs
                        .iter()
                        .find(|run| run.id == *run_id)
                        .cloned()
                });
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_QUEUED,
                    &json!({
                        "context": context,
                        "run": run.as_ref().map(crate::task_projection::project_run),
                    }),
                )
                .await;
            }
            TaskEventPayload::RunCreated { run, .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_RUN_CREATED,
                    &json!({
                        "context": context,
                        "run": crate::task_projection::project_run(&run),
                    }),
                )
                .await;
            }
            TaskEventPayload::RunStarted { run_id, .. } => {
                if let Some(run) = task_response.runs.into_iter().find(|run| run.id == run_id) {
                    self.send_notification_to_task_workspace_connections(
                        notification_task_id.as_str(),
                        workspace_id.as_str(),
                        events::TASK_RUN_STARTED,
                        &json!({
                            "context": context,
                            "run": crate::task_projection::project_run(&run),
                        }),
                    )
                    .await;
                }
            }
            TaskEventPayload::Progress {
                message: _,
                details,
                ..
            } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_PROGRESS,
                    &json!({
                        "context": context,
                        "message": "Task progress updated.",
                        "details": details.map(|details| json!({
                            "stage": details.stage,
                            "percent": details.percent,
                        })),
                    }),
                )
                .await;
            }
            TaskEventPayload::RunCompleted { run_id, .. } => {
                self.mark_task_run_occurrence_turn_terminal(run_id.as_str())
                    .await?;
                if let Some(run) = task_response.runs.into_iter().find(|run| run.id == run_id) {
                    self.send_notification_to_task_workspace_connections(
                        notification_task_id.as_str(),
                        workspace_id.as_str(),
                        events::TASK_RUN_COMPLETED,
                        &json!({
                            "context": context,
                            "run": crate::task_projection::project_run(&run),
                        }),
                    )
                    .await;
                }
            }
            TaskEventPayload::RunFailed { run_id, .. } => {
                self.mark_task_run_occurrence_turn_terminal(run_id.as_str())
                    .await?;
                if let Some(run) = task_response.runs.into_iter().find(|run| run.id == run_id) {
                    self.send_notification_to_task_workspace_connections(
                        notification_task_id.as_str(),
                        workspace_id.as_str(),
                        events::TASK_RUN_FAILED,
                        &json!({
                            "context": context,
                            "run": crate::task_projection::project_run(&run),
                        }),
                    )
                    .await;
                }
            }
            TaskEventPayload::RunBlocked { run_id, .. } => {
                self.mark_task_run_occurrence_turn_terminal(run_id.as_str())
                    .await?;
                if let Some(run) = task_response.runs.into_iter().find(|run| run.id == run_id) {
                    self.send_notification_to_task_workspace_connections(
                        notification_task_id.as_str(),
                        workspace_id.as_str(),
                        events::TASK_RUN_BLOCKED,
                        &json!({
                            "context": context,
                            "run": crate::task_projection::project_run(&run),
                        }),
                    )
                    .await;
                }
            }
            TaskEventPayload::RunCancelled { run_id, .. } => {
                self.mark_task_run_occurrence_turn_terminal(run_id.as_str())
                    .await?;
                if let Some(run) = task_response.runs.into_iter().find(|run| run.id == run_id) {
                    self.send_notification_to_task_workspace_connections(
                        notification_task_id.as_str(),
                        workspace_id.as_str(),
                        events::TASK_RUN_CANCELLED,
                        &json!({
                            "context": context,
                            "run": crate::task_projection::project_run(&run),
                        }),
                    )
                    .await;
                }
            }
            TaskEventPayload::TaskCompleted { .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_COMPLETED,
                    &json!({
                        "context": context,
                        "task": crate::task_projection::project_task(&task_response.task),
                    }),
                )
                .await;
            }
            TaskEventPayload::TaskFailed { .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_FAILED,
                    &json!({
                        "context": context,
                        "task": crate::task_projection::project_task(&task_response.task),
                    }),
                )
                .await;
            }
            TaskEventPayload::TaskBlocked { .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_BLOCKED,
                    &json!({
                        "context": context,
                        "task": crate::task_projection::project_task(&task_response.task),
                    }),
                )
                .await;
            }
            TaskEventPayload::TaskCancelled { .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_CANCELLED,
                    &json!({
                        "context": context,
                        "task": crate::task_projection::project_task(&task_response.task),
                    }),
                )
                .await;
            }
            TaskEventPayload::TaskDetached { .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_DETACHED,
                    &json!({
                        "context": context,
                        "task": crate::task_projection::project_task(&task_response.task),
                    }),
                )
                .await;
            }
            TaskEventPayload::TaskUpdated {
                task,
                trigger,
                agent_spec: _,
                changed_fields,
                ..
            } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_UPDATED,
                    &json!({
                        "context": context.clone(),
                        "task": crate::task_projection::project_task(&task),
                        "trigger": trigger.as_ref().map(crate::task_projection::project_trigger),
                        "changedFields": changed_fields,
                    }),
                )
                .await;
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::TaskRescheduled {
                trigger, reason, ..
            } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_RESCHEDULED,
                    &json!({
                        "context": context,
                        "trigger": crate::task_projection::project_trigger(&trigger),
                        "reason": reason,
                    }),
                )
                .await;
            }
            TaskEventPayload::TaskPaused {
                task,
                triggers,
                reason,
                ..
            } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_PAUSED,
                    &json!({
                        "context": context.clone(),
                        "task": crate::task_projection::project_task(&task),
                        "triggers": triggers.iter().map(crate::task_projection::project_trigger).collect::<Vec<_>>(),
                        "reason": reason,
                    }),
                )
                .await;
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::TaskResumed {
                task,
                triggers,
                reason,
                ..
            } => {
                let scheduled_trigger = triggers
                    .iter()
                    .rev()
                    .find(|trigger| trigger.next_fire_at.is_some())
                    .cloned();
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_RESUMED,
                    &json!({
                        "context": context.clone(),
                        "task": crate::task_projection::project_task(&task),
                        "triggers": triggers.iter().map(crate::task_projection::project_trigger).collect::<Vec<_>>(),
                        "reason": reason,
                    }),
                )
                .await;
                if let Some(trigger) = scheduled_trigger {
                    self.send_notification_to_task_workspace_connections(
                        notification_task_id.as_str(),
                        workspace_id.as_str(),
                        events::TASK_SCHEDULED,
                        &json!({
                            "context": context.clone(),
                            "trigger": crate::task_projection::project_trigger(&trigger),
                        }),
                    )
                    .await;
                }
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::TaskRecovered { recovered_at, .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_RECOVERED,
                    &pioneer_protocol::TaskRecoveredNotification {
                        context,
                        recovered_at,
                    },
                )
                .await;
            }
            TaskEventPayload::RunRetryScheduled { .. }
            | TaskEventPayload::RunRetryExhausted { .. }
            | TaskEventPayload::WriteLockAcquired { .. }
            | TaskEventPayload::WriteLockReleased { .. }
            | TaskEventPayload::WriteLockBlocked { .. }
            | TaskEventPayload::WriteLockExpired { .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::ChildThreadLinked { .. }
            | TaskEventPayload::TaskThreadLineageCreated { .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::DeliveryQueued { delivery } => {
                let (child_thread_id, child_turn_id) =
                    task_delivery_child_lineage(&self.crud_store, delivery.run_id.as_str()).await;
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_QUEUED,
                    &json!({
                        "context": context,
                        "summary": delivery.result_snapshot.as_ref().and_then(|result| result.summary.clone()),
                        "childThreadId": child_thread_id,
                        "childTurnId": child_turn_id,
                        "delivery": crate::task_projection::project_delivery(&delivery),
                    }),
                )
                .await;
            }
            TaskEventPayload::DeliveryStarted { delivery, attempt } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_STARTED,
                    &json!({
                        "context": context,
                        "delivery": crate::task_projection::project_delivery(&delivery),
                        "attempt": crate::task_projection::project_delivery_attempt(&attempt),
                    }),
                )
                .await;
            }
            TaskEventPayload::DeliveryDelivered { delivery, attempt } => {
                self.mark_task_run_occurrence_turn_terminal(delivery.run_id.as_str())
                    .await?;
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_DELIVERED,
                    &json!({
                        "context": context,
                        "delivery": crate::task_projection::project_delivery(&delivery),
                        "attempt": crate::task_projection::project_delivery_attempt(&attempt),
                    }),
                )
                .await;
            }
            TaskEventPayload::DeliveryFailed { delivery, attempt } => {
                if delivery.status == TaskDeliveryStatus::Failed {
                    self.mark_task_run_occurrence_turn_terminal(delivery.run_id.as_str())
                        .await?;
                }
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_FAILED,
                    &json!({
                        "context": context,
                        "delivery": crate::task_projection::project_delivery(&delivery),
                        "attempt": crate::task_projection::project_delivery_attempt(&attempt),
                    }),
                )
                .await;
            }
            TaskEventPayload::DeliveryCancelled {
                delivery, reason, ..
            } => {
                self.mark_task_run_occurrence_turn_terminal(delivery.run_id.as_str())
                    .await?;
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_CANCELLED,
                    &json!({
                        "context": context,
                        "delivery": crate::task_projection::project_delivery(&delivery),
                        "reason": reason,
                    }),
                )
                .await;
            }
            TaskEventPayload::DependencyCreated { .. }
            | TaskEventPayload::AgentSpecCreated { .. }
            | TaskEventPayload::TaskRunThreadBindingCreated { .. }
            | TaskEventPayload::TaskRunTurnStarted { .. }
            | TaskEventPayload::TaskRunTurnCompleted { .. }
            | TaskEventPayload::TaskRunTurnFailed { .. }
            | TaskEventPayload::TaskRunTurnBlocked { .. }
            | TaskEventPayload::TaskResultCandidateCreated { .. }
            | TaskEventPayload::TaskResultReviewEventRecorded { .. }
            | TaskEventPayload::TaskRevisionRequested { .. }
            | TaskEventPayload::TaskRunEnteredReview { .. }
            | TaskEventPayload::DepthLimitExceeded { .. }
            | TaskEventPayload::WriteLockExtended { .. } => {
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::TaskResultCandidateAccepted { .. }
            | TaskEventPayload::TaskResultCandidateRejected { .. }
            | TaskEventPayload::TaskResultCandidateCancelled { .. } => {
                // Candidate projection resolves the acceptance gate in the
                // durable terminal-effect outbox. Wake execution after the
                // committed task event is visible; otherwise an effect whose
                // Turn committed first can remain asleep until the periodic
                // resilience sweep.
                self.kick_native_terminal_effects();
                self.send_notification_to_task_workspace_connections(
                    notification_task_id.as_str(),
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
        }
        if let Some(notification) = timeline_changed {
            self.notify_semantic_timeline_turn_state_changed(
                notification.workspace_id.as_str(),
                notification.thread_id.as_str(),
                notification.turn_id.as_str(),
            )
            .await;
        }
        Ok(())
    }

    async fn refresh_parent_task_anchor(&self, response: &TaskGetResponse) -> Result<bool> {
        let Some(parent_thread_id) = response.task.created_by_thread_id.as_deref() else {
            return Ok(false);
        };
        let Some(parent_turn_id) = response.task.created_by_turn_id.as_deref() else {
            return Ok(false);
        };
        self.refresh_task_anchor_in_turn(response, parent_thread_id, parent_turn_id, None)
            .await
    }

    pub(super) async fn refresh_task_anchor_in_turn(
        &self,
        response: &TaskGetResponse,
        thread_id: &str,
        turn_id: &str,
        run_id: Option<&str>,
    ) -> Result<bool> {
        self.refresh_task_anchor_in_turn_with_progress(response, thread_id, turn_id, run_id, None)
            .await
    }

    async fn refresh_task_anchor_in_turn_with_progress(
        &self,
        response: &TaskGetResponse,
        thread_id: &str,
        turn_id: &str,
        run_id: Option<&str>,
        progress_preview: Option<String>,
    ) -> Result<bool> {
        let item = match run_id {
            Some(run_id) if task_run_uses_creation_anchor(response, run_id) => {
                crate::task_tools::task_turn_item_from_response_for_run_with_progress(
                    self,
                    response,
                    run_id,
                    crate::task_tools::task_anchor_id(response.task.id.as_str()),
                    progress_preview,
                )
                .await?
            }
            Some(run_id) => {
                crate::task_tools::task_turn_item_from_response_for_run_with_progress(
                    self,
                    response,
                    run_id,
                    crate::task_tools::task_run_anchor_id(run_id),
                    progress_preview,
                )
                .await?
            }
            None => {
                crate::task_tools::task_turn_item_from_response_with_progress(
                    self,
                    response,
                    progress_preview,
                )
                .await?
            }
        };
        let Some(existing) = self
            .crud_store
            .get_turn_item(turn_id, item.id.as_str())
            .await?
        else {
            return Ok(false);
        };
        if matches!(existing, TurnItem::Task { item: existing_item } if existing_item == item) {
            return Ok(false);
        }
        let notification = ItemUpdatedNotification {
            workspace_id: response.task.workspace_id.clone(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item: TurnItem::Task { item },
        };
        let event_timestamp_secs = now_timestamp_secs();
        self.crud_store
            .materialize_item_updated(notification.clone(), event_timestamp_secs)
            .await?;
        self.send_notification_to_thread_subscribers(
            thread_id,
            events::ITEM_UPDATED,
            &notification,
        )
        .await;
        self.notify_semantic_timeline_item_changed(
            notification.workspace_id.as_str(),
            notification.thread_id.as_str(),
            notification.turn_id.as_str(),
            &notification.item,
            None,
        )
        .await;
        Ok(true)
    }

    async fn publish_parent_task_progress_snapshot(
        &self,
        response: &TaskGetResponse,
        payload: &TaskEventPayload,
    ) -> Result<()> {
        let TaskEventPayload::Progress {
            task_id,
            run_id,
            message,
            ..
        } = payload
        else {
            return Ok(());
        };
        let Some((parent_thread_id, parent_turn_id)) =
            self.task_progress_parent_target(response, payload).await
        else {
            return Ok(());
        };
        let item_id = parent_task_anchor_item_id(response, run_id.as_deref());
        if !self
            .task_progress_target_has_anchor(parent_turn_id.as_str(), item_id.as_str())
            .await
        {
            debug!(
                task_id,
                parent_thread_id,
                parent_turn_id,
                "dropped task progress snapshot because target turn has no durable task anchor"
            );
            return Ok(());
        }
        let progress_preview = crate::task_tools::task_progress_preview(message);
        if progress_preview.is_some() {
            self.refresh_task_anchor_in_turn_with_progress(
                response,
                parent_thread_id.as_str(),
                parent_turn_id.as_str(),
                run_id.as_deref(),
                progress_preview,
            )
            .await?;
        }
        let published = self
            .agent_manager
            .publish_progress(
                parent_thread_id.as_str(),
                AgentProgressEvent::TaskProgress {
                    workspace_id: response.task.workspace_id.clone(),
                    thread_id: parent_thread_id.clone(),
                    turn_id: parent_turn_id.clone(),
                    item_id,
                    task_id: task_id.clone(),
                    run_id: run_id.clone(),
                    summary: message.clone(),
                },
            )
            .await;
        if !published {
            debug!(
                task_id,
                parent_thread_id,
                parent_turn_id,
                "dropped task progress snapshot because parent agent thread is not active"
            );
        }
        Ok(())
    }

    async fn flush_parent_task_progress_snapshot(
        &self,
        response: &TaskGetResponse,
        payload: &TaskEventPayload,
    ) {
        let item_id = parent_task_anchor_item_id(response, payload.run_id());
        for (parent_thread_id, parent_turn_id) in
            self.task_progress_flush_targets(response, payload).await
        {
            if !self
                .task_progress_target_has_anchor(parent_turn_id.as_str(), item_id.as_str())
                .await
            {
                continue;
            }
            let _ = self
                .agent_manager
                .flush_progress_for_item(
                    parent_thread_id.as_str(),
                    response.task.workspace_id.as_str(),
                    parent_turn_id.as_str(),
                    item_id.as_str(),
                )
                .await;
        }
    }

    async fn task_timeline_changed_target(
        &self,
        response: &TaskGetResponse,
        payload: &TaskEventPayload,
    ) -> Option<TaskTimelineChangedTarget> {
        let task = &response.task;
        let (parent_thread_id, parent_turn_id) = self
            .task_timeline_parent_target(response, payload)
            .await
            .or_else(|| {
                Some((
                    task.created_by_thread_id.clone()?,
                    task.created_by_turn_id.clone()?,
                ))
            })?;
        Some(TaskTimelineChangedTarget {
            workspace_id: task.workspace_id.clone(),
            thread_id: parent_thread_id,
            turn_id: parent_turn_id,
            run_id: payload.run_id().map(str::to_owned),
        })
    }

    async fn task_timeline_parent_target(
        &self,
        response: &TaskGetResponse,
        payload: &TaskEventPayload,
    ) -> Option<(String, String)> {
        if let TaskEventPayload::ChildThreadLinked { lineage } = payload {
            return lineage
                .parent_turn_id
                .as_ref()
                .map(|turn_id| (lineage.parent_thread_id.clone(), turn_id.clone()));
        }
        if let TaskEventPayload::TaskThreadLineageCreated { lineage, .. } = payload {
            return lineage.created_by_turn_id.as_ref().map(|turn_id| {
                (
                    lineage
                        .created_by_thread_id
                        .clone()
                        .unwrap_or_else(|| lineage.parent_thread_id.clone()),
                    turn_id.clone(),
                )
            });
        }

        self.task_run_parent_target(response, payload.run_id()?)
            .await
    }

    async fn task_progress_parent_target(
        &self,
        response: &TaskGetResponse,
        payload: &TaskEventPayload,
    ) -> Option<(String, String)> {
        match payload.run_id() {
            Some(run_id) => self.task_run_parent_target(response, run_id).await,
            None => Some((
                response.task.created_by_thread_id.clone()?,
                response.task.created_by_turn_id.clone()?,
            )),
        }
    }

    async fn task_progress_flush_targets(
        &self,
        response: &TaskGetResponse,
        payload: &TaskEventPayload,
    ) -> Vec<(String, String)> {
        let mut targets = Vec::new();
        if let Some(run_id) = payload.run_id() {
            if let Some(target) = self.task_run_parent_target(response, run_id).await {
                targets.push(target);
            }
            return targets;
        }

        if let (Some(thread_id), Some(turn_id)) = (
            response.task.created_by_thread_id.clone(),
            response.task.created_by_turn_id.clone(),
        ) {
            targets.push((thread_id, turn_id));
        }

        for run in &response.runs {
            if let Some(target) = self.task_run_parent_target(response, run.id.as_str()).await
                && !targets.iter().any(|existing| existing == &target)
            {
                targets.push(target);
            }
        }

        targets
    }

    async fn task_run_parent_target(
        &self,
        response: &TaskGetResponse,
        run_id: &str,
    ) -> Option<(String, String)> {
        if let Ok(Some(binding)) = self
            .crud_store
            .get_task_run_primary_thread_binding(run_id)
            .await
            && let Ok(Some(lineage)) = self
                .crud_store
                .get_task_thread_lineage(binding.thread_id.as_str())
                .await
            && let Some(parent_turn_id) = lineage.created_by_turn_id
        {
            let parent_thread_id = lineage
                .created_by_thread_id
                .unwrap_or(lineage.parent_thread_id);
            return Some((parent_thread_id, parent_turn_id));
        }

        let parent_thread_id = response.task.created_by_thread_id.clone()?;
        if task_run_uses_creation_anchor(response, run_id) {
            return Some((parent_thread_id, response.task.created_by_turn_id.clone()?));
        }
        Some((parent_thread_id, run_id.to_owned()))
    }

    async fn task_progress_target_has_anchor(&self, turn_id: &str, item_id: &str) -> bool {
        matches!(
            self.crud_store.get_turn_item(turn_id, item_id).await,
            Ok(Some(TurnItem::Task { .. }))
        )
    }

    #[cfg(test)]
    pub(super) async fn task_progress_parent_target_for_test(
        &self,
        response: &TaskGetResponse,
        payload: &TaskEventPayload,
    ) -> Option<(String, String)> {
        self.task_progress_parent_target(response, payload).await
    }
}

impl MessageProcessor {
    pub(super) async fn mark_task_run_occurrence_turn_terminal(
        &self,
        run_id: &str,
    ) -> Result<TaskRunOccurrenceTerminalizationOutcome> {
        let outcome = self
            .crud_store
            .compare_and_materialize_task_run_occurrence_terminal(run_id, now_timestamp_secs())
            .await?;
        if outcome != TaskRunOccurrenceTerminalizationOutcome::Changed {
            return Ok(outcome);
        }

        // Durable CAS is authoritative. Subscriber fan-out is deliberately
        // best-effort and cannot turn an already committed reconciliation into
        // an apparent failure that would be retried and counted again.
        let Some((parent_thread_id, _workspace_id)) =
            (match self.crud_store.get_turn_location(run_id).await {
                Ok(location) => location,
                Err(error) => {
                    warn!(
                        run_id,
                        error = %format!("{error:#}"),
                        "failed to locate reconciled TaskRun occurrence for subscriber fan-out"
                    );
                    None
                }
            })
        else {
            return Ok(outcome);
        };
        let Some((workspace_id, turn)) = (match self
            .crud_store
            .get_turn(parent_thread_id.as_str(), run_id)
            .await
        {
            Ok(turn) => turn,
            Err(error) => {
                warn!(
                    run_id,
                    thread_id = parent_thread_id.as_str(),
                    error = %format!("{error:#}"),
                    "failed to load reconciled TaskRun occurrence for subscriber fan-out"
                );
                None
            }
        }) else {
            return Ok(outcome);
        };

        match turn.status {
            TurnStatus::Completed => {
                let notification = TurnCompletedNotification {
                    workspace_id,
                    thread_id: parent_thread_id.clone(),
                    turn,
                };
                self.send_notification_to_thread_subscribers(
                    parent_thread_id.as_str(),
                    events::TURN_COMPLETED,
                    &notification,
                )
                .await;
                self.notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
            }
            TurnStatus::Failed | TurnStatus::Interrupted => {
                let notification = TurnFailedNotification {
                    workspace_id,
                    thread_id: parent_thread_id.clone(),
                    turn,
                };
                self.send_notification_to_thread_subscribers(
                    parent_thread_id.as_str(),
                    events::TURN_FAILED,
                    &notification,
                )
                .await;
                self.notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
            }
            TurnStatus::Blocked => {
                let notification = TurnBlockedNotification {
                    workspace_id,
                    thread_id: parent_thread_id.clone(),
                    turn,
                    resume: None,
                };
                self.send_notification_to_thread_subscribers(
                    parent_thread_id.as_str(),
                    events::TURN_BLOCKED,
                    &notification,
                )
                .await;
                self.notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
            }
            TurnStatus::InProgress => warn!(
                run_id,
                "TaskRun occurrence remained in_progress after terminal CAS"
            ),
        }
        Ok(outcome)
    }
}

fn should_refresh_parent_task_anchor(
    response: &TaskGetResponse,
    payload: &TaskEventPayload,
) -> bool {
    if let Some(run_id) = payload.run_id() {
        return task_run_uses_creation_anchor(response, run_id);
    }

    match payload {
        TaskEventPayload::TaskScheduled { .. }
        | TaskEventPayload::TaskUpdated { .. }
        | TaskEventPayload::TaskPaused { .. }
        | TaskEventPayload::TaskResumed { .. }
        | TaskEventPayload::TaskDetached { .. }
        | TaskEventPayload::TaskCancelled { .. } => true,
        TaskEventPayload::TaskRescheduled { reason, .. } => matches!(
            reason,
            TaskRescheduleReason::UserRequested | TaskRescheduleReason::MissedFireSkipped
        ),
        TaskEventPayload::TaskCompleted { .. }
        | TaskEventPayload::TaskFailed { .. }
        | TaskEventPayload::TaskBlocked { .. } => response
            .runs
            .last()
            .map(|run| task_run_uses_creation_anchor(response, run.id.as_str()))
            .unwrap_or(false),
        _ => false,
    }
}

fn task_run_uses_creation_anchor(response: &TaskGetResponse, run_id: &str) -> bool {
    if response.task.created_by_turn_id.is_none() {
        return false;
    }
    if !response
        .task
        .lifecycle_policy
        .as_ref()
        .map(|policy| policy.attachment == TaskAttachmentMode::Attached)
        .unwrap_or(false)
    {
        return false;
    }
    response
        .runs
        .iter()
        .find(|run| run.id == run_id)
        .and_then(|run| run.trigger_id.as_deref())
        .and_then(|trigger_id| {
            response
                .triggers
                .iter()
                .find(|trigger| trigger.id == trigger_id)
        })
        .map(|trigger| trigger.kind() == TaskTriggerKind::Immediate)
        .unwrap_or(false)
}

fn parent_task_anchor_item_id(response: &TaskGetResponse, run_id: Option<&str>) -> String {
    match run_id {
        Some(run_id) if !task_run_uses_creation_anchor(response, run_id) => {
            crate::task_tools::task_run_anchor_id(run_id)
        }
        _ => crate::task_tools::task_anchor_id(response.task.id.as_str()),
    }
}

async fn task_delivery_child_lineage(
    store: &std::sync::Arc<pioneer_crud::CrudStore>,
    run_id: &str,
) -> (Option<String>, Option<String>) {
    match store.get_task_run_child_anchor(run_id).await {
        Ok(anchor) => (anchor.child_thread_id, anchor.child_turn_id),
        Err(error) => {
            warn!(
                run_id,
                error = %format!("{error:#}"),
                "failed to load child lineage for task delivery notification"
            );
            (None, None)
        }
    }
}
