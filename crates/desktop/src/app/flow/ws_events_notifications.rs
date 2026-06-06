use super::*;
use pioneer_client::notifications::router::{
    ArtifactDeletedRefreshReduction, ArtifactThreadRefreshReduction, ConversationEventReduction,
    SkillsRefreshReduction, ThreadArtifactsRefreshReduction, ThreadClosedReduction,
    ThreadStartedContext, ThreadStartedReduction, ThreadUpdatedReduction, TurnLifecycleReduction,
    TurnTimelineRefreshReduction, WorkspaceRefreshReduction, reduce_artifact_created_notification,
    reduce_artifact_deleted_notification, reduce_artifact_updated_notification,
    reduce_item_completed_notification, reduce_item_delta_notification,
    reduce_item_recovery_attached_notification, reduce_item_recovery_exhausted_notification,
    reduce_item_recovery_opened_notification, reduce_item_recovery_succeeded_notification,
    reduce_item_retry_attempt_started_notification, reduce_item_retry_scheduled_notification,
    reduce_item_started_notification, reduce_item_timeout_detected_notification,
    reduce_item_tool_retry_exhausted_notification, reduce_item_tool_retry_resolved_notification,
    reduce_item_tool_retry_scheduled_notification, reduce_item_updated_notification,
    reduce_skills_changed_notification, reduce_thread_agents_doc_changed_notification,
    reduce_thread_artifacts_changed_notification, reduce_thread_closed_notification,
    reduce_thread_started_notification, reduce_thread_tree_changed_notification,
    reduce_thread_updated_notification, reduce_turn_blocked_notification,
    reduce_turn_completed_notification, reduce_turn_execution_window_blocked_notification,
    reduce_turn_execution_window_checkpointed_notification,
    reduce_turn_execution_window_continued_notification,
    reduce_turn_execution_window_exhausted_notification,
    reduce_turn_execution_window_started_notification, reduce_turn_failed_notification,
    reduce_turn_started_notification, reduce_turn_timeline_changed_notification,
    reduce_turn_tool_loop_budget_exceeded_notification,
};
use pioneer_client::workspaces::actions::{
    WorkspacePreferenceReduction, reduce_workspace_preference_after_catalog_change,
};
use pioneer_client::workspaces::selectors as workspace_selectors;

use pioneer_client::workspaces::actions::apply_workspace_changed_to_catalog;

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
            GatewayNotification::TurnBlocked(notification) => {
                self.apply_turn_blocked_notification(notification);
            }
            GatewayNotification::ItemStarted(notification) => {
                let reduction = reduce_item_started_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemDelta(notification) => {
                let reduction = reduce_item_delta_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemCompleted(notification) => {
                let reduction = reduce_item_completed_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemUpdated(notification) => {
                let reduction = reduce_item_updated_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ContextCompressing(_notification) => {
                // TODO: show "Compressing conversation..." indicator in UI
            }
            GatewayNotification::ContextCompressed(_notification) => {
                // TODO: dismiss compression indicator in UI
            }
            GatewayNotification::ItemTimeoutDetected(notification) => {
                let reduction = reduce_item_timeout_detected_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemRecoveryOpened(notification) => {
                let reduction = reduce_item_recovery_opened_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemRecoveryAttached(notification) => {
                let reduction = reduce_item_recovery_attached_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemRetryScheduled(notification) => {
                let reduction = reduce_item_retry_scheduled_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemRetryAttemptStarted(notification) => {
                let reduction = reduce_item_retry_attempt_started_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemRecoverySucceeded(notification) => {
                let reduction = reduce_item_recovery_succeeded_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemRecoveryExhausted(notification) => {
                let reduction = reduce_item_recovery_exhausted_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemToolRetryScheduled(notification) => {
                let reduction = reduce_item_tool_retry_scheduled_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemToolRetryResolved(notification) => {
                let reduction = reduce_item_tool_retry_resolved_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::ItemToolRetryExhausted(notification) => {
                let reduction = reduce_item_tool_retry_exhausted_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::TurnToolLoopBudgetExceeded(notification) => {
                let reduction = reduce_turn_tool_loop_budget_exceeded_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::TurnExecutionWindowStarted(notification) => {
                let reduction = reduce_turn_execution_window_started_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::TurnExecutionWindowExhausted(notification) => {
                let reduction = reduce_turn_execution_window_exhausted_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::TurnExecutionWindowCheckpointed(notification) => {
                let reduction =
                    reduce_turn_execution_window_checkpointed_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::TurnExecutionWindowContinued(notification) => {
                let reduction = reduce_turn_execution_window_continued_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::TurnExecutionWindowBlocked(notification) => {
                let reduction = reduce_turn_execution_window_blocked_notification(notification);
                self.apply_conversation_event_reduction(reduction);
            }
            GatewayNotification::Unknown(_notification) => {}
            GatewayNotification::ThreadClosed(notification) => {
                let matches_thread_workspace = self.thread_workspace_matches(
                    notification.thread_id.as_str(),
                    notification.workspace_id.as_str(),
                );
                let reduction =
                    reduce_thread_closed_notification(notification, matches_thread_workspace);
                self.apply_thread_closed_reduction(reduction);
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
                let matches_thread_workspace = self.thread_workspace_matches(
                    notification.thread_id.as_str(),
                    notification.workspace_id.as_str(),
                );
                let reduction = reduce_thread_artifacts_changed_notification(
                    notification,
                    matches_thread_workspace,
                );
                self.apply_thread_artifacts_refresh_reduction(reduction, cx);
            }
            GatewayNotification::ArtifactCreated(notification) => {
                let reduction = reduce_artifact_created_notification(notification);
                self.apply_artifact_thread_refresh_reduction(reduction, cx);
            }
            GatewayNotification::ArtifactUpdated(notification) => {
                let reduction = reduce_artifact_updated_notification(notification);
                self.apply_artifact_thread_refresh_reduction(reduction, cx);
            }
            GatewayNotification::ArtifactDeleted(notification) => {
                let active_thread_id = self.current_active_thread_id().map(str::to_owned);
                let active_thread_artifacts = self.thread_artifacts.items_for_active_thread();
                let reduction = reduce_artifact_deleted_notification(
                    notification,
                    active_thread_id.as_deref(),
                    active_thread_artifacts,
                );
                self.apply_artifact_deleted_refresh_reduction(reduction, cx);
            }
            GatewayNotification::ArtifactProjectionUpdated(_)
            | GatewayNotification::ArtifactUploadProgress(_)
            | GatewayNotification::ArtifactDownloadProgress(_) => {}
            GatewayNotification::TurnTimelineChanged(notification) => {
                let reduction = reduce_turn_timeline_changed_notification(notification);
                self.apply_turn_timeline_refresh_reduction(reduction, cx);
            }
            GatewayNotification::WorkspaceChanged(notification) => {
                self.apply_workspace_changed_notification(notification);
            }
            GatewayNotification::TaskCreated(_)
            | GatewayNotification::TaskScheduled(_)
            | GatewayNotification::TaskQueued(_)
            | GatewayNotification::TaskRunCreated(_)
            | GatewayNotification::TaskRunStarted(_)
            | GatewayNotification::TaskProgress(_)
            | GatewayNotification::TaskRunCompleted(_)
            | GatewayNotification::TaskRunFailed(_)
            | GatewayNotification::TaskRunBlocked(_)
            | GatewayNotification::TaskRunCancelled(_)
            | GatewayNotification::TaskCompleted(_)
            | GatewayNotification::TaskFailed(_)
            | GatewayNotification::TaskBlocked(_)
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
        let pending_thread_id = self.thread_start_coordinator().pending_thread_id.clone();
        let active_workspace = self.active_workspace_scope_for_notifications();
        let reduction = reduce_thread_started_notification(
            notification,
            ThreadStartedContext {
                pending_thread_id: pending_thread_id.as_deref(),
                active_thread_id: self.current_active_thread_id(),
                active_workspace_id: active_workspace.as_deref(),
            },
        );
        self.apply_thread_started_reduction(reduction);
    }

    fn apply_thread_started_reduction(&mut self, reduction: ThreadStartedReduction) {
        self.upsert_thread_snapshot(reduction.thread);
        self.upsert_thread_for_workspace(
            reduction.thread_id.as_str(),
            reduction.workspace_id.as_str(),
        );

        if let Some(thread_id) = reduction.set_draft_thread_id {
            self.set_draft_thread_id(Some(thread_id));
        }
        if let Some(thread_id) = reduction.set_active_thread_id {
            self.set_active_thread_id(Some(thread_id));
        }
        if let Some(workspace_id) = reduction.set_preferred_workspace_id {
            self.set_preferred_workspace_id(Some(workspace_id));
        }
        if let Some(workspace_id) = reduction.persist_active_gateway_workspace_id {
            self.persist_active_gateway_workspace_id(workspace_id);
        }
        if reduction.reset_thread_start {
            self.reset_thread_start_state();
        }
        if reduction.clear_thread_start_queue {
            self.clear_thread_start_queue();
        }
        if reduction.queue_thread_list_refresh {
            self.queue_thread_list_refresh();
        }
        if reduction.sync_composer_model_selection {
            self.sync_composer_model_selection_for_active_thread();
        }
    }

    fn apply_workspace_changed_notification(&mut self, notification: WorkspaceChangedNotification) {
        apply_workspace_changed_to_catalog(&mut self.workspaces, &notification);
        let reduction = reduce_workspace_preference_after_catalog_change(
            self.preferred_workspace_id(),
            self.workspaces(),
        );
        self.apply_workspace_preference_reduction(reduction);
    }

    fn apply_turn_started_notification(
        &mut self,
        notification: pioneer_protocol::TurnStartedNotification,
    ) {
        let reduction = reduce_turn_started_notification(notification);
        self.apply_turn_lifecycle_reduction(reduction, None);
    }

    fn apply_thread_updated_notification(
        &mut self,
        notification: pioneer_protocol::ThreadUpdatedNotification,
    ) {
        let reduction = reduce_thread_updated_notification(notification);
        self.apply_thread_updated_reduction(reduction);
    }

    fn apply_turn_completed_notification(
        &mut self,
        notification: pioneer_protocol::TurnCompletedNotification,
        cx: &mut Context<Self>,
    ) {
        let reduction = reduce_turn_completed_notification(notification);
        self.apply_turn_lifecycle_reduction(reduction, Some(cx));
    }

    fn apply_turn_failed_notification(
        &mut self,
        notification: pioneer_protocol::TurnFailedNotification,
    ) {
        let reduction = reduce_turn_failed_notification(notification);
        self.apply_turn_lifecycle_reduction(reduction, None);
    }

    fn apply_turn_blocked_notification(
        &mut self,
        notification: pioneer_protocol::TurnBlockedNotification,
    ) {
        let reduction = reduce_turn_blocked_notification(notification);
        self.apply_turn_lifecycle_reduction(reduction, None);
    }

    fn apply_turn_lifecycle_reduction(
        &mut self,
        reduction: TurnLifecycleReduction,
        mut cx: Option<&mut Context<Self>>,
    ) {
        let thread_id = reduction.thread_id.clone();
        if reduction.promote_thread_from_draft {
            self.promote_thread_from_draft(thread_id.as_str());
        }
        if reduction.queue_thread_list_refresh {
            self.queue_thread_list_refresh();
        }
        if let Some(status) = reduction.thread_status
            && let Some(coordinator) = self.thread_coordinator_mut(thread_id.as_str())
            && let Some(thread) = coordinator.thread_mut()
        {
            thread.status = status;
        }
        self.upsert_thread_conversation_mut(thread_id.as_str(), reduction.workspace_id.as_str())
            .apply(reduction.conversation_event);
        if reduction.tick_conversation
            && let Some(conversation) = self.thread_conversation_mut(thread_id.as_str())
        {
            let _ = conversation.tick();
        }
        if reduction.reset_thread_resume {
            self.reset_thread_resume_state(thread_id.as_str());
        }
        if reduction.refresh_thread_artifacts
            && let Some(cx) = cx.as_deref_mut()
        {
            self.refresh_thread_artifacts(thread_id.clone(), true, cx);
        }
        if reduction.sync_composer_model_selection {
            self.sync_composer_model_selection_for_active_thread();
        }
    }

    fn apply_conversation_event_reduction(&mut self, reduction: ConversationEventReduction) {
        self.upsert_thread_conversation_mut(
            reduction.thread_id.as_str(),
            reduction.workspace_id.as_str(),
        )
        .apply(reduction.conversation_event);
    }

    fn apply_thread_closed_reduction(&mut self, reduction: ThreadClosedReduction) {
        if reduction.remove_thread_conversation {
            self.remove_thread_conversation(reduction.thread_id.as_str());
        }
        if reduction.clear_active_thread_if_matches {
            let _ = self.clear_active_thread_if_matches(reduction.thread_id.as_str());
        }
        if reduction.queue_thread_list_refresh {
            self.queue_thread_list_refresh();
        }
    }

    fn apply_thread_updated_reduction(&mut self, reduction: ThreadUpdatedReduction) {
        self.upsert_thread_snapshot(reduction.thread);
        self.upsert_thread_for_workspace(
            reduction.thread_id.as_str(),
            reduction.workspace_id.as_str(),
        );
        if reduction.sync_composer_model_selection {
            self.sync_composer_model_selection_for_active_thread();
        }
    }

    fn apply_workspace_refresh_reduction(&mut self, reduction: WorkspaceRefreshReduction) {
        if reduction.queue_thread_list_refresh {
            self.queue_thread_list_refresh();
        }
    }

    fn apply_workspace_preference_reduction(&mut self, reduction: WorkspacePreferenceReduction) {
        if let Some(workspace_id) = reduction.set_preferred_workspace_id {
            self.set_preferred_workspace_id(workspace_id);
        }
        if let Some(workspace_id) = reduction.persist_active_gateway_workspace_id {
            self.persist_active_gateway_workspace_id(workspace_id);
        }
        if reduction.queue_thread_list_refresh {
            self.queue_thread_list_refresh();
        }
    }

    fn apply_thread_tree_changed_notification(
        &mut self,
        notification: pioneer_protocol::ThreadTreeChangedNotification,
    ) {
        let active_workspace = self.active_workspace_scope_for_notifications();
        let reduction =
            reduce_thread_tree_changed_notification(notification, active_workspace.as_deref());
        self.apply_workspace_refresh_reduction(reduction);
    }

    fn apply_thread_agents_doc_changed_notification(
        &mut self,
        notification: pioneer_protocol::ThreadAgentsDocChangedNotification,
    ) {
        let active_workspace = self.active_workspace_scope_for_notifications();
        let reduction = reduce_thread_agents_doc_changed_notification(
            notification,
            active_workspace.as_deref(),
        );
        self.apply_workspace_refresh_reduction(reduction);
    }

    fn apply_skills_changed_notification(
        &mut self,
        notification: pioneer_protocol::SkillsChangedNotification,
    ) {
        let active_workspace = self.active_workspace_scope_for_notifications();
        let reduction =
            reduce_skills_changed_notification(notification, active_workspace.as_deref());
        self.apply_skills_refresh_reduction(reduction);
    }

    fn apply_skills_refresh_reduction(&mut self, reduction: SkillsRefreshReduction) {
        if reduction.queue_skills_refresh {
            self.queue_skills_refresh();
        }
    }

    fn apply_thread_artifacts_refresh_reduction(
        &mut self,
        reduction: ThreadArtifactsRefreshReduction,
        cx: &mut Context<Self>,
    ) {
        if reduction.refresh_thread_artifacts {
            self.refresh_thread_artifacts(reduction.thread_id, reduction.force_refresh, cx);
        }
    }

    fn apply_artifact_thread_refresh_reduction(
        &mut self,
        reduction: ArtifactThreadRefreshReduction,
        cx: &mut Context<Self>,
    ) {
        if reduction.refresh_thread_artifacts
            && let Some(thread_id) = reduction.thread_id
        {
            self.refresh_thread_artifacts(thread_id, reduction.force_refresh, cx);
        }
    }

    fn apply_artifact_deleted_refresh_reduction(
        &mut self,
        reduction: ArtifactDeletedRefreshReduction,
        cx: &mut Context<Self>,
    ) {
        if reduction.refresh_thread_artifacts
            && let Some(thread_id) = reduction.active_thread_id
        {
            self.refresh_thread_artifacts(thread_id, reduction.force_refresh, cx);
        }
    }

    fn apply_turn_timeline_refresh_reduction(
        &mut self,
        reduction: TurnTimelineRefreshReduction,
        cx: &mut Context<Self>,
    ) {
        if reduction.queue_turn_timeline_refresh {
            self.refresh_turn_timeline(reduction.thread_id, reduction.turn_id, cx);
        }
    }

    fn active_workspace_scope_for_notifications(&self) -> Option<String> {
        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id);
        workspace_selectors::resolve_workspace_scope(
            self.active_workspace_id(),
            self.preferred_workspace_id(),
            runtime_workspace_id,
        )
    }
}
