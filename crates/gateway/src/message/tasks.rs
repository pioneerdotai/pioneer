use super::*;
use anyhow::Result;
use pioneer_protocol::{
    ItemUpdatedNotification, TaskAttachmentMode, TaskEventPayload, TaskGetResponse,
    TaskRescheduleReason, TaskTriggerKind,
};

impl MessageProcessor {
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
        let is_progress_event = matches!(event.payload, TaskEventPayload::Progress { .. });
        let is_terminal_event = event.payload.is_terminal();
        if is_progress_event {
            self.publish_parent_task_progress_snapshot(&task_response, &event.payload)
                .await;
        } else if is_terminal_event {
            self.flush_parent_task_progress_snapshot(&task_response, &event.payload)
                .await;
        }
        let timeline_changed = if is_progress_event {
            None
        } else {
            self.task_timeline_changed_notification(&task_response, &event.payload)
                .await
        };
        let refresh_parent_anchor =
            !is_progress_event && should_refresh_parent_task_anchor(&task_response, &event.payload);
        if refresh_parent_anchor {
            self.refresh_parent_task_anchor(&task_response).await?;
        }
        if let Some(notification) = timeline_changed.as_ref()
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
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_CREATED,
                    &pioneer_protocol::TaskCreatedNotification { context, task },
                )
                .await;
            }
            TaskEventPayload::TriggerCreated { trigger } => {
                if trigger.kind() != TaskTriggerKind::Immediate {
                    self.send_notification_to_workspace_connections(
                        workspace_id.as_str(),
                        events::TASK_SCHEDULED,
                        &pioneer_protocol::TaskScheduledNotification { context, trigger },
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
                    self.send_notification_to_workspace_connections(
                        workspace_id.as_str(),
                        events::TASK_SCHEDULED,
                        &pioneer_protocol::TaskScheduledNotification { context, trigger },
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
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_QUEUED,
                    &pioneer_protocol::TaskQueuedNotification { context, run },
                )
                .await;
            }
            TaskEventPayload::RunCreated { run, .. } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_RUN_CREATED,
                    &pioneer_protocol::TaskRunCreatedNotification { context, run },
                )
                .await;
            }
            TaskEventPayload::RunStarted { run_id, .. } => {
                if let Some(run) = task_response.runs.into_iter().find(|run| run.id == run_id) {
                    self.send_notification_to_workspace_connections(
                        workspace_id.as_str(),
                        events::TASK_RUN_STARTED,
                        &pioneer_protocol::TaskRunStartedNotification { context, run },
                    )
                    .await;
                }
            }
            TaskEventPayload::Progress {
                message, details, ..
            } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_PROGRESS,
                    &pioneer_protocol::TaskProgressNotification {
                        context,
                        message,
                        details,
                    },
                )
                .await;
            }
            TaskEventPayload::RunCompleted { run_id, .. } => {
                if let Some(run) = task_response.runs.into_iter().find(|run| run.id == run_id) {
                    self.send_notification_to_workspace_connections(
                        workspace_id.as_str(),
                        events::TASK_RUN_COMPLETED,
                        &pioneer_protocol::TaskRunCompletedNotification { context, run },
                    )
                    .await;
                }
            }
            TaskEventPayload::RunFailed { run_id, .. } => {
                if let Some(run) = task_response.runs.into_iter().find(|run| run.id == run_id) {
                    self.send_notification_to_workspace_connections(
                        workspace_id.as_str(),
                        events::TASK_RUN_FAILED,
                        &pioneer_protocol::TaskRunFailedNotification { context, run },
                    )
                    .await;
                }
            }
            TaskEventPayload::RunCancelled { run_id, .. } => {
                if let Some(run) = task_response.runs.into_iter().find(|run| run.id == run_id) {
                    self.send_notification_to_workspace_connections(
                        workspace_id.as_str(),
                        events::TASK_RUN_CANCELLED,
                        &pioneer_protocol::TaskRunFailedNotification { context, run },
                    )
                    .await;
                }
            }
            TaskEventPayload::TaskCompleted { .. } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_COMPLETED,
                    &pioneer_protocol::TaskCompletedNotification {
                        context,
                        task: task_response.task,
                    },
                )
                .await;
            }
            TaskEventPayload::TaskFailed { .. } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_FAILED,
                    &pioneer_protocol::TaskFailedNotification {
                        context,
                        task: task_response.task,
                    },
                )
                .await;
            }
            TaskEventPayload::TaskCancelled { .. } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_CANCELLED,
                    &pioneer_protocol::TaskCancelledNotification {
                        context,
                        task: task_response.task,
                    },
                )
                .await;
            }
            TaskEventPayload::TaskDetached { .. } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_DETACHED,
                    &pioneer_protocol::TaskDetachedNotification {
                        context,
                        task: task_response.task,
                    },
                )
                .await;
            }
            TaskEventPayload::TaskUpdated {
                task,
                trigger,
                agent_spec,
                changed_fields,
                ..
            } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_UPDATED,
                    &pioneer_protocol::TaskUpdatedNotification {
                        context: context.clone(),
                        task,
                        trigger,
                        agent_spec,
                        changed_fields,
                    },
                )
                .await;
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::TaskRescheduled {
                trigger, reason, ..
            } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_RESCHEDULED,
                    &pioneer_protocol::TaskRescheduledNotification {
                        context,
                        trigger,
                        reason,
                    },
                )
                .await;
            }
            TaskEventPayload::TaskPaused {
                task,
                triggers,
                reason,
                ..
            } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_PAUSED,
                    &pioneer_protocol::TaskPausedNotification {
                        context: context.clone(),
                        task,
                        triggers,
                        reason,
                    },
                )
                .await;
                self.send_notification_to_workspace_connections(
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
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_RESUMED,
                    &pioneer_protocol::TaskResumedNotification {
                        context: context.clone(),
                        task,
                        triggers: triggers.clone(),
                        reason,
                    },
                )
                .await;
                if let Some(trigger) = scheduled_trigger {
                    self.send_notification_to_workspace_connections(
                        workspace_id.as_str(),
                        events::TASK_SCHEDULED,
                        &pioneer_protocol::TaskScheduledNotification {
                            context: context.clone(),
                            trigger,
                        },
                    )
                    .await;
                }
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::TaskRecovered { recovered_at, .. } => {
                self.send_notification_to_workspace_connections(
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
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::ChildThreadLinked { .. }
            | TaskEventPayload::TaskThreadLineageCreated { .. } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
            TaskEventPayload::DeliveryQueued { delivery } => {
                let (child_thread_id, child_turn_id) =
                    task_delivery_child_lineage(&self.crud_store, delivery.run_id.as_str()).await;
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_QUEUED,
                    &pioneer_protocol::TaskDeliveryQueuedNotification {
                        context,
                        summary: delivery
                            .result_snapshot
                            .as_ref()
                            .and_then(|result| result.summary.clone()),
                        error_preview: delivery
                            .error_snapshot
                            .as_ref()
                            .map(|error| error.message.clone()),
                        child_thread_id,
                        child_turn_id,
                        delivery,
                    },
                )
                .await;
            }
            TaskEventPayload::DeliveryStarted { delivery, attempt } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_STARTED,
                    &pioneer_protocol::TaskDeliveryStartedNotification {
                        context,
                        delivery,
                        attempt,
                    },
                )
                .await;
            }
            TaskEventPayload::DeliveryDelivered { delivery, attempt } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_DELIVERED,
                    &pioneer_protocol::TaskDeliveryDeliveredNotification {
                        context,
                        delivery,
                        attempt,
                    },
                )
                .await;
            }
            TaskEventPayload::DeliveryFailed { delivery, attempt } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_FAILED,
                    &pioneer_protocol::TaskDeliveryFailedNotification {
                        context,
                        delivery,
                        attempt,
                    },
                )
                .await;
            }
            TaskEventPayload::DeliveryCancelled { delivery, reason } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_DELIVERY_CANCELLED,
                    &pioneer_protocol::TaskDeliveryCancelledNotification {
                        context,
                        delivery,
                        reason,
                    },
                )
                .await;
            }
            TaskEventPayload::DependencyCreated { .. }
            | TaskEventPayload::AgentSpecCreated { .. }
            | TaskEventPayload::TaskRunThreadBindingCreated { .. }
            | TaskEventPayload::TaskRunTurnStarted { .. }
            | TaskEventPayload::TaskRunTurnCompleted { .. }
            | TaskEventPayload::TaskRunTurnFailed { .. }
            | TaskEventPayload::TaskResultCandidateCreated { .. }
            | TaskEventPayload::TaskResultReviewEventRecorded { .. }
            | TaskEventPayload::TaskResultCandidateAccepted { .. }
            | TaskEventPayload::TaskResultCandidateRejected { .. }
            | TaskEventPayload::TaskResultCandidateCancelled { .. }
            | TaskEventPayload::TaskRevisionRequested { .. }
            | TaskEventPayload::TaskRunEnteredReview { .. }
            | TaskEventPayload::DepthLimitExceeded { .. }
            | TaskEventPayload::WriteLockExtended { .. } => {
                self.send_notification_to_workspace_connections(
                    workspace_id.as_str(),
                    events::TASK_TREE_CHANGED,
                    &pioneer_protocol::TaskTreeChangedNotification { context },
                )
                .await;
            }
        }
        if let Some(notification) = timeline_changed {
            self.send_notification_to_thread_subscribers(
                notification.thread_id.as_str(),
                events::TURN_TIMELINE_CHANGED,
                &notification,
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
        let item = match run_id {
            Some(run_id) if task_run_uses_creation_anchor(response, run_id) => {
                crate::task_tools::task_turn_item_from_response_for_run(
                    self,
                    response,
                    run_id,
                    crate::task_tools::task_anchor_id(response.task.id.as_str()),
                )
                .await?
            }
            Some(run_id) => {
                crate::task_tools::task_turn_item_from_response_for_run(
                    self,
                    response,
                    run_id,
                    crate::task_tools::task_run_anchor_id(run_id),
                )
                .await?
            }
            None => crate::task_tools::task_turn_item_from_response(self, response).await?,
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
        Ok(true)
    }

    async fn publish_parent_task_progress_snapshot(
        &self,
        response: &TaskGetResponse,
        payload: &TaskEventPayload,
    ) {
        let TaskEventPayload::Progress {
            task_id,
            run_id,
            message,
            ..
        } = payload
        else {
            return;
        };
        let Some((parent_thread_id, parent_turn_id)) =
            self.task_progress_parent_target(response, payload).await
        else {
            return;
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
            return;
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

    async fn task_timeline_changed_notification(
        &self,
        response: &TaskGetResponse,
        payload: &TaskEventPayload,
    ) -> Option<TurnTimelineChangedNotification> {
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
        let (child_thread_id, child_turn_id) =
            task_event_child_lineage(&self.crud_store, payload).await;
        Some(TurnTimelineChangedNotification {
            workspace_id: task.workspace_id.clone(),
            thread_id: parent_thread_id,
            turn_id: parent_turn_id,
            task_id: Some(task.id.clone()),
            run_id: payload.run_id().map(str::to_owned),
            child_thread_id,
            child_turn_id,
            reason: TurnTimelineChangedReason::TaskEventChanged,
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
        TaskEventPayload::TaskCompleted { .. } | TaskEventPayload::TaskFailed { .. } => response
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

async fn task_event_child_lineage(
    store: &std::sync::Arc<pioneer_crud::CrudStore>,
    payload: &TaskEventPayload,
) -> (Option<String>, Option<String>) {
    if let TaskEventPayload::ChildThreadLinked { lineage } = payload {
        return (
            Some(lineage.child_thread_id.clone()),
            Some(lineage.child_turn_id.clone()),
        );
    }
    if let TaskEventPayload::TaskThreadLineageCreated { lineage, .. } = payload {
        return (Some(lineage.child_thread_id.clone()), None);
    }

    if payload.thread_id().is_some() || payload.turn_id().is_some() {
        return (
            payload.thread_id().map(str::to_owned),
            payload.turn_id().map(str::to_owned),
        );
    }

    match payload.run_id() {
        Some(run_id) => task_delivery_child_lineage(store, run_id).await,
        None => (None, None),
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
