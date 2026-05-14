use super::*;
use crate::app::conversation::ConversationEvent;

impl PioneerDesktop {
    pub(in crate::app::flow) fn apply_gateway_notification(
        &mut self,
        notification: GatewayNotification,
        cx: &mut Context<Self>,
    ) {
        match notification {
            GatewayNotification::ThreadStarted(notification) => {
                self.apply_thread_started_notification(notification);
            }
            GatewayNotification::TurnStarted(notification) => {
                self.apply_turn_started_notification(notification);
            }
            GatewayNotification::TurnCompleted(notification) => {
                self.apply_turn_completed_notification(notification, cx);
            }
            GatewayNotification::TurnFailed(notification) => {
                self.apply_turn_failed_notification(notification);
            }
            GatewayNotification::ItemStarted(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemStarted {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item: notification.item,
                });
            }
            GatewayNotification::ItemDelta(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemDelta {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    delta: notification.delta,
                    stream: notification.stream,
                    payload: notification.payload,
                    markdown: notification.markdown,
                    markdown_version: notification.markdown_version,
                });
            }
            GatewayNotification::ItemCompleted(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemCompleted {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item: notification.item,
                });
            }
            GatewayNotification::ItemUpdated(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemUpdated {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item: notification.item,
                });
            }
            GatewayNotification::ContextCompressing(_notification) => {
                // TODO: show "Compressing conversation..." indicator in UI
            }
            GatewayNotification::ContextCompressed(_notification) => {
                // TODO: dismiss compression indicator in UI
            }
            GatewayNotification::ItemTimeoutDetected(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemTimeoutDetected {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    attempt_number: notification.attempt_number,
                    reason: notification.reason,
                    recovery_job_id: notification.recovery_job_id,
                });
            }
            GatewayNotification::ItemRecoveryOpened(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemRecoveryOpened {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                });
            }
            GatewayNotification::ItemRecoveryAttached(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemRecoveryAttached {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    recovery_item_id: notification.recovery_item_id,
                    recovery_item_type: notification.recovery_item_type,
                    existing_status: notification.existing_status,
                    next_attempt_number: notification.next_attempt_number,
                });
            }
            GatewayNotification::ItemRetryScheduled(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemRetryScheduled {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                    next_run_at_unix: notification.next_run_at_unix,
                    reason: notification.reason,
                });
            }
            GatewayNotification::ItemRetryAttemptStarted(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemRetryAttemptStarted {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                });
            }
            GatewayNotification::ItemRecoverySucceeded(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemRecoverySucceeded {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                });
            }
            GatewayNotification::ItemRecoveryExhausted(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemRecoveryExhausted {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    recovery_job_id: notification.recovery_job_id,
                    attempt_number: notification.attempt_number,
                    status: notification.status,
                    error_message: notification.error_message,
                });
            }
            GatewayNotification::ItemToolRetryScheduled(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemToolRetryScheduled {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    tool_retry_episode_id: notification.tool_retry_episode_id,
                    tool_name: notification.tool_name,
                    attempt_number: notification.attempt_number,
                    error_class: notification.error_class,
                    retry_hint: notification.retry_hint,
                    budgets: notification.budgets,
                    failure_signature_fingerprint: notification.failure_signature_fingerprint,
                    reason: notification.reason,
                });
            }
            GatewayNotification::ItemToolRetryResolved(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemToolRetryResolved {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    tool_retry_episode_id: notification.tool_retry_episode_id,
                    tool_name: notification.tool_name,
                    attempt_number: notification.attempt_number,
                    resolution: notification.resolution,
                    budgets: notification.budgets,
                    reason: notification.reason,
                });
            }
            GatewayNotification::ItemToolRetryExhausted(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::ItemToolRetryExhausted {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    item_id: notification.item_id,
                    item_type: notification.item_type,
                    tool_retry_episode_id: notification.tool_retry_episode_id,
                    tool_name: notification.tool_name,
                    attempt_number: notification.attempt_number,
                    error_class: notification.error_class,
                    exhaustion_kind: notification.exhaustion_kind,
                    budgets: notification.budgets,
                    failure_signature_fingerprint: notification.failure_signature_fingerprint,
                    reason: notification.reason,
                });
            }
            GatewayNotification::TurnToolLoopBudgetExceeded(notification) => {
                let thread_id = notification.thread_id.clone();
                self.upsert_thread_conversation_mut(
                    thread_id.as_str(),
                    notification.workspace_id.as_str(),
                )
                .apply(ConversationEvent::TurnToolLoopBudgetExceeded {
                    thread_id: notification.thread_id,
                    turn_id: notification.turn_id,
                    limit_kind: notification.limit_kind,
                    limit: notification.limit,
                    observed: notification.observed,
                    action: notification.action,
                    reason: notification.reason,
                });
            }
            GatewayNotification::Unknown(_notification) => {}
            GatewayNotification::ThreadClosed(notification) => {
                let matches_thread_workspace = self.thread_workspace_matches(
                    notification.thread_id.as_str(),
                    notification.workspace_id.as_str(),
                );
                if matches_thread_workspace {
                    self.remove_thread_conversation(notification.thread_id.as_str());
                    let _ = self.clear_active_thread_if_matches(notification.thread_id.as_str());
                    self.queue_thread_list_refresh();
                }
            }
            GatewayNotification::ThreadTreeChanged(notification) => {
                self.apply_thread_tree_changed_notification(notification);
            }
            GatewayNotification::ThreadAgentsDocChanged(notification) => {
                self.apply_thread_agents_doc_changed_notification(notification);
            }
            GatewayNotification::ThreadUpdated(notification) => {
                self.apply_thread_updated_notification(notification);
            }
            GatewayNotification::SkillsChanged(notification) => {
                self.apply_skills_changed_notification(notification);
            }
            GatewayNotification::SkillsUploadChunkAck(_notification) => {}
            GatewayNotification::McpChanged(notification) => {
                self.apply_mcp_changed_notification(notification);
            }
            GatewayNotification::McpServerStatusChanged(notification) => {
                self.apply_mcp_server_status_changed_notification(notification);
            }
            GatewayNotification::McpServerCatalogChanged(notification) => {
                self.apply_mcp_server_catalog_changed_notification(notification);
            }
            GatewayNotification::ThreadArtifactsChanged(notification) => {
                self.apply_thread_artifacts_changed_notification(notification, cx);
            }
            GatewayNotification::ArtifactCreated(notification) => {
                if let Some(thread_id) = notification.artifact.primary_thread_id {
                    self.refresh_thread_artifacts(thread_id, true, cx);
                }
            }
            GatewayNotification::ArtifactUpdated(notification) => {
                if let Some(thread_id) = notification.artifact.primary_thread_id {
                    self.refresh_thread_artifacts(thread_id, true, cx);
                }
            }
            GatewayNotification::ArtifactDeleted(notification) => {
                self.refresh_current_thread_artifacts_if_contains(
                    notification.artifact_id.as_str(),
                    cx,
                );
            }
            GatewayNotification::ArtifactProjectionUpdated(_)
            | GatewayNotification::ArtifactUploadProgress(_)
            | GatewayNotification::ArtifactDownloadProgress(_) => {}
            GatewayNotification::TurnTimelineChanged(notification) => {
                self.refresh_turn_timeline(notification.thread_id, notification.turn_id, cx);
            }
            GatewayNotification::TaskCreated(_)
            | GatewayNotification::TaskScheduled(_)
            | GatewayNotification::TaskQueued(_)
            | GatewayNotification::TaskRunCreated(_)
            | GatewayNotification::TaskRunStarted(_)
            | GatewayNotification::TaskProgress(_)
            | GatewayNotification::TaskRunCompleted(_)
            | GatewayNotification::TaskRunFailed(_)
            | GatewayNotification::TaskRunCancelled(_)
            | GatewayNotification::TaskCompleted(_)
            | GatewayNotification::TaskFailed(_)
            | GatewayNotification::TaskCancelled(_)
            | GatewayNotification::TaskDetached(_)
            | GatewayNotification::TaskUpdated(_)
            | GatewayNotification::TaskRescheduled(_)
            | GatewayNotification::TaskPaused(_)
            | GatewayNotification::TaskResumed(_)
            | GatewayNotification::TaskDeliveryQueued(_)
            | GatewayNotification::TaskDeliveryStarted(_)
            | GatewayNotification::TaskDeliveryDelivered(_)
            | GatewayNotification::TaskDeliveryFailed(_)
            | GatewayNotification::TaskDeliveryCancelled(_)
            | GatewayNotification::TaskTreeChanged(_)
            | GatewayNotification::TaskRecovered(_)
            | GatewayNotification::MemoryChanged(_)
            | GatewayNotification::MemoryCandidateCreated(_)
            | GatewayNotification::MemoryForgotten(_) => {}
        }
    }

    fn apply_thread_started_notification(
        &mut self,
        notification: pioneer_protocol::ThreadStartedNotification,
    ) {
        let thread = notification.thread;
        let thread_id = thread.id.clone();
        let workspace_id = thread.workspace_id.clone();
        self.upsert_thread_snapshot(thread);
        self.upsert_thread_for_workspace(thread_id.as_str(), workspace_id.as_str());

        let pending_thread_id = self.thread_start_coordinator().pending_thread_id.clone();
        let started_local_pending = pending_thread_id.as_deref() == Some(thread_id.as_str());
        if started_local_pending {
            self.set_draft_thread_id(Some(thread_id.clone()));
            if self.current_active_thread_id().is_none() {
                self.set_active_thread_id(Some(thread_id.clone()));
            }
            self.reset_thread_start_state();
            self.clear_thread_start_queue();
        }

        self.set_preferred_workspace_id(Some(workspace_id.clone()));
        self.persist_active_gateway_workspace_id(workspace_id);
        self.queue_thread_list_refresh();
        self.sync_composer_model_selection_for_active_thread();
    }

    fn apply_turn_started_notification(
        &mut self,
        notification: pioneer_protocol::TurnStartedNotification,
    ) {
        let thread_id = notification.thread_id.clone();
        self.promote_thread_from_draft(thread_id.as_str());
        self.queue_thread_list_refresh();
        if let Some(coordinator) = self.thread_coordinator_mut(thread_id.as_str()) {
            if let Some(thread) = coordinator.thread_mut() {
                thread.status = pioneer_protocol::ThreadStatus::Active;
            }
        }
        self.upsert_thread_conversation_mut(thread_id.as_str(), notification.workspace_id.as_str())
            .apply(ConversationEvent::TurnStarted {
                thread_id: notification.thread_id,
                turn: notification.turn,
            });
        self.reset_thread_resume_state(thread_id.as_str());
        self.sync_composer_model_selection_for_active_thread();
    }

    fn apply_thread_updated_notification(
        &mut self,
        notification: pioneer_protocol::ThreadUpdatedNotification,
    ) {
        let thread = notification.thread;
        let thread_id = thread.id.clone();
        let workspace_id = thread.workspace_id.clone();
        self.upsert_thread_snapshot(thread);
        self.upsert_thread_for_workspace(thread_id.as_str(), workspace_id.as_str());
        self.sync_composer_model_selection_for_active_thread();
    }

    fn apply_turn_completed_notification(
        &mut self,
        notification: pioneer_protocol::TurnCompletedNotification,
        cx: &mut Context<Self>,
    ) {
        let thread_id = notification.thread_id.clone();
        if let Some(coordinator) = self.thread_coordinator_mut(thread_id.as_str()) {
            if let Some(thread) = coordinator.thread_mut() {
                thread.status = pioneer_protocol::ThreadStatus::Idle;
            }
        }
        self.upsert_thread_conversation_mut(thread_id.as_str(), notification.workspace_id.as_str())
            .apply(ConversationEvent::TurnCompleted {
                thread_id: notification.thread_id,
                turn: notification.turn,
            });
        if let Some(conversation) = self.thread_conversation_mut(thread_id.as_str()) {
            let _ = conversation.tick();
        }
        self.reset_thread_resume_state(thread_id.as_str());
        self.refresh_thread_artifacts(thread_id, true, cx);
    }

    fn apply_turn_failed_notification(
        &mut self,
        notification: pioneer_protocol::TurnFailedNotification,
    ) {
        let thread_id = notification.thread_id.clone();
        if let Some(coordinator) = self.thread_coordinator_mut(thread_id.as_str()) {
            if let Some(thread) = coordinator.thread_mut() {
                thread.status = pioneer_protocol::ThreadStatus::Idle;
            }
        }
        self.upsert_thread_conversation_mut(thread_id.as_str(), notification.workspace_id.as_str())
            .apply(ConversationEvent::TurnFailed {
                thread_id: notification.thread_id,
                turn: notification.turn,
            });
        self.reset_thread_resume_state(thread_id.as_str());
    }

    fn apply_thread_tree_changed_notification(
        &mut self,
        notification: pioneer_protocol::ThreadTreeChangedNotification,
    ) {
        let active_workspace = self.active_workspace_scope_for_notifications();
        if should_refresh_workspace_bound_data(
            active_workspace.as_deref(),
            notification.workspace_id.as_str(),
        ) {
            self.queue_thread_list_refresh();
        }
    }

    fn apply_thread_agents_doc_changed_notification(
        &mut self,
        notification: pioneer_protocol::ThreadAgentsDocChangedNotification,
    ) {
        let active_workspace = self.active_workspace_scope_for_notifications();
        if should_refresh_workspace_bound_data(
            active_workspace.as_deref(),
            notification.workspace_id.as_str(),
        ) {
            self.queue_thread_list_refresh();
        }
    }

    fn apply_skills_changed_notification(
        &mut self,
        notification: pioneer_protocol::SkillsChangedNotification,
    ) {
        let active_workspace = self.active_workspace_scope_for_notifications();
        if should_refresh_workspace_bound_data(
            active_workspace.as_deref(),
            notification.workspace_id.as_str(),
        ) {
            self.queue_skills_refresh();
        }
    }

    fn active_workspace_scope_for_notifications(&self) -> Option<String> {
        self.preferred_workspace_id()
            .map(str::to_owned)
            .or_else(|| {
                self.gateway
                    .runtime
                    .as_ref()
                    .and_then(GatewayRuntime::active_workspace_id)
                    .map(str::to_owned)
            })
    }
}

pub(super) fn should_refresh_workspace_bound_data(
    active_workspace: Option<&str>,
    notification_workspace: &str,
) -> bool {
    active_workspace == Some(notification_workspace)
}
