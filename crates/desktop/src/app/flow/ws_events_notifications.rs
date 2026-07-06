use super::*;
use crate::app::root::DesktopVoiceComposerState;
use crate::audio::capture::DesktopVoiceCaptureErrorKind;
use pioneer_client::notifications::router::{
    ArtifactDeletedRefreshReduction, ArtifactThreadRefreshReduction, CLIRuntimeRefreshReduction,
    ConversationEventReduction, SkillsRefreshReduction, ThreadArtifactsRefreshReduction,
    ThreadClosedReduction, ThreadStartedReduction, ThreadUpdatedReduction, TurnLifecycleReduction,
    WorkspacePreferenceReduction, WorkspaceRefreshReduction, apply_workspace_changed_to_catalog,
};
use pioneer_client::runtime::{ClientRuntimeNotification, ClientRuntimeNotificationContext};
use pioneer_client::voice::{VoiceFinalizeUiAction, VoiceSessionResultReduction};
use pioneer_client::workspaces::selectors as workspace_selectors;
use pioneer_protocol::{VoiceError, VoiceSessionOutcome};

impl PioneerDesktop {
    pub(in crate::app::flow) fn apply_gateway_notification(
        &mut self,
        notification: GatewayNotification,
        cx: &mut Context<Self>,
    ) {
        let active_workspace = self.active_workspace_scope_for_notifications();
        let mcp_workspace = self.mcp_workspace_scope();
        let notification_thread_workspace_matches =
            self.notification_thread_workspace_matches(&notification);
        let context = ClientRuntimeNotificationContext {
            pending_thread_id: self.thread_start_coordinator().pending_thread_id.as_deref(),
            active_thread_id: self.current_active_thread_id(),
            active_workspace_id: active_workspace.as_deref(),
            notification_thread_workspace_matches,
            active_thread_artifacts: self.thread_artifacts.items_for_active_thread(),
            preferred_workspace_id: self.preferred_workspace_id(),
            workspaces: self.workspaces(),
            mcp_workspace_id: mcp_workspace.as_deref(),
            mcp_selected_server_id: self.mcp_selected_server_id.as_deref(),
            mcp_details_loaded: self.mcp_server_details.is_some(),
        };
        let Some(reduction) = self
            .gateway
            .client_runtime
            .reduce_gateway_notification(notification, context)
        else {
            return;
        };
        self.apply_gateway_notification_reduction(reduction, cx);
    }

    fn apply_voice_session_result_reduction(
        &mut self,
        reduction: VoiceSessionResultReduction,
        cx: &mut Context<Self>,
    ) {
        match reduction.action {
            VoiceFinalizeUiAction::KeepFinalizing => {}
            VoiceFinalizeUiAction::ClearFinalizing => {
                if let DesktopVoiceComposerState::Finalizing { thread_id } =
                    &self.desktop_voice_composer
                {
                    let thread_id = thread_id.clone();
                    self.desktop_voice_composer = DesktopVoiceComposerState::Idle;
                    if matches!(reduction.outcome, VoiceSessionOutcome::TurnStarted) {
                        self.clear_composer_payload_for_thread(thread_id.as_str());
                    }
                    cx.notify();
                }
            }
            VoiceFinalizeUiAction::ShowNoSpeechError => {
                self.desktop_voice_composer = DesktopVoiceComposerState::Error {
                    kind: DesktopVoiceCaptureErrorKind::NoSpeech,
                    message: desktop_voice_no_speech_message(reduction.error.as_ref()),
                };
                cx.notify();
            }
            VoiceFinalizeUiAction::ShowFinalizeError => {
                self.desktop_voice_composer = DesktopVoiceComposerState::Error {
                    kind: DesktopVoiceCaptureErrorKind::GatewayFinalize,
                    message: desktop_voice_transcription_failed_message(reduction.error.as_ref()),
                };
                cx.notify();
            }
        }
    }

    fn notification_thread_workspace_matches(&self, notification: &GatewayNotification) -> bool {
        match notification {
            GatewayNotification::ThreadClosed(notification) => self.thread_workspace_matches(
                notification.thread_id.as_str(),
                notification.workspace_id.as_str(),
            ),
            GatewayNotification::ThreadArtifactsChanged(notification) => self
                .thread_workspace_matches(
                    notification.thread_id.as_str(),
                    notification.workspace_id.as_str(),
                ),
            _ => false,
        }
    }

    fn apply_gateway_notification_reduction(
        &mut self,
        reduction: ClientRuntimeNotification,
        cx: &mut Context<Self>,
    ) {
        match reduction {
            ClientRuntimeNotification::ThreadStarted(reduction) => {
                self.apply_thread_started_reduction(reduction);
            }
            ClientRuntimeNotification::TurnLifecycle(reduction) => {
                self.apply_turn_lifecycle_reduction(reduction, Some(cx));
            }
            ClientRuntimeNotification::ConversationEvent(reduction) => {
                self.apply_conversation_event_reduction(reduction);
            }
            ClientRuntimeNotification::ThreadClosed(reduction) => {
                self.apply_thread_closed_reduction(reduction);
            }
            ClientRuntimeNotification::WorkspaceRefresh(reduction) => {
                self.apply_workspace_refresh_reduction(reduction);
            }
            ClientRuntimeNotification::ThreadUpdated(reduction) => {
                self.apply_thread_updated_reduction(reduction);
            }
            ClientRuntimeNotification::SkillsRefresh(reduction) => {
                self.apply_skills_refresh_reduction(reduction);
            }
            ClientRuntimeNotification::McpRefresh(reduction) => {
                self.apply_mcp_refresh_reduction(reduction);
            }
            ClientRuntimeNotification::McpServerStatusChanged(reduction) => {
                self.apply_mcp_server_status_changed_reduction(reduction);
            }
            ClientRuntimeNotification::McpServerCatalogChanged(reduction) => {
                self.apply_mcp_server_catalog_changed_reduction(reduction);
            }
            ClientRuntimeNotification::ThreadArtifactsRefresh(reduction) => {
                self.apply_thread_artifacts_refresh_reduction(reduction, cx);
            }
            ClientRuntimeNotification::ArtifactThreadRefresh(reduction) => {
                self.apply_artifact_thread_refresh_reduction(reduction, cx);
            }
            ClientRuntimeNotification::ArtifactDeletedRefresh(reduction) => {
                self.apply_artifact_deleted_refresh_reduction(reduction, cx);
            }
            ClientRuntimeNotification::SemanticTimeline(update) => {
                self.apply_semantic_timeline_live_update(update, cx);
            }
            ClientRuntimeNotification::VoiceSessionResult(reduction) => {
                self.apply_voice_session_result_reduction(reduction, cx);
            }
            ClientRuntimeNotification::CLIRuntimeRefresh(reduction) => {
                self.apply_cli_runtime_refresh_reduction(reduction, cx);
            }
            ClientRuntimeNotification::CLIRuntimePendingRequests { refresh, reduction } => {
                self.apply_pending_requests_reduction(reduction, cx);
                self.apply_cli_runtime_refresh_reduction(refresh, cx);
            }
            ClientRuntimeNotification::PendingRequests { reduction } => {
                self.apply_pending_requests_reduction(reduction, cx);
            }
            ClientRuntimeNotification::GatewayRemoteAccessStatusChanged(notification) => {
                self.apply_remote_access_status_changed(notification.status, cx);
            }
            ClientRuntimeNotification::WorkspaceChanged {
                notification,
                preference,
            } => {
                apply_workspace_changed_to_catalog(&mut self.workspaces, &notification);
                self.apply_workspace_preference_reduction(preference);
            }
        }
    }

    fn apply_remote_access_status_changed(
        &mut self,
        status: pioneer_protocol::GatewayRemoteAccessStatusSnapshot,
        cx: &mut Context<Self>,
    ) {
        let Some(settings) = self.gateway.settings.as_mut() else {
            return;
        };
        settings.remote_access.status = status;
        cx.notify();
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
            .apply(reduction.conversation_event.clone());
        if pioneer_client::timeline::semantic::apply_conversation_event_to_semantic_timeline(
            &mut self.semantic_timelines,
            reduction.workspace_id.as_str(),
            &reduction.conversation_event,
            pioneer_client::timeline::labels::now_unix_ms(),
        ) {
            self.semantic_timeline_revision = self.semantic_timeline_revision.saturating_add(1);
            if let Some(cx) = cx.as_deref_mut() {
                cx.notify();
            }
        }
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
        if let Some(pending_reduction) = reduction.pending_requests
            && self.pending_requests.apply(pending_reduction)
            && let Some(cx) = cx.as_deref_mut()
        {
            cx.notify();
        }
    }

    fn apply_conversation_event_reduction(&mut self, reduction: ConversationEventReduction) {
        self.upsert_thread_conversation_mut(
            reduction.thread_id.as_str(),
            reduction.workspace_id.as_str(),
        )
        .apply(reduction.conversation_event.clone());
        if pioneer_client::timeline::semantic::apply_conversation_event_to_semantic_timeline(
            &mut self.semantic_timelines,
            reduction.workspace_id.as_str(),
            &reduction.conversation_event,
            pioneer_client::timeline::labels::now_unix_ms(),
        ) {
            self.semantic_timeline_revision = self.semantic_timeline_revision.saturating_add(1);
        }
    }

    fn apply_thread_closed_reduction(&mut self, reduction: ThreadClosedReduction) {
        if let Some(pending_reduction) = reduction.pending_requests {
            self.pending_requests.apply(pending_reduction);
        }
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

    fn apply_semantic_timeline_live_update(
        &mut self,
        update: pioneer_client::timeline::semantic::SemanticTimelineLiveUpdate,
        cx: &mut Context<Self>,
    ) {
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);
        let mut refetch_thread_id = None;
        let mut refetch_work_candidate = None;
        match &update {
            pioneer_client::timeline::semantic::SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(notification)
                if active_thread_id.as_deref() == Some(notification.thread_id.as_str()) =>
            {
                refetch_thread_id = Some(notification.thread_id.clone());
            }
            pioneer_client::timeline::semantic::SemanticTimelineLiveUpdate::TurnWorkItemsChanged(notification)
                if active_thread_id.as_deref() == Some(notification.thread_id.as_str()) =>
            {
                refetch_work_candidate =
                    Some((notification.thread_id.clone(), notification.turn_id.clone()));
            }
            pioneer_client::timeline::semantic::SemanticTimelineLiveUpdate::TurnWorkStateChanged(notification)
                if active_thread_id.as_deref() == Some(notification.thread_id.as_str()) =>
            {
                refetch_work_candidate =
                    Some((notification.thread_id.clone(), notification.turn_id.clone()));
            }
            _ => {}
        }

        if pioneer_client::timeline::semantic::apply_semantic_timeline_live_update(
            &mut self.semantic_timelines,
            update,
        ) {
            self.semantic_timeline_revision = self.semantic_timeline_revision.saturating_add(1);
            cx.notify();
        }

        if let Some(thread_id) = refetch_thread_id {
            self.request_semantic_thread_newest_page(thread_id, cx);
        }
        if let Some((thread_id, turn_id)) = refetch_work_candidate
            && self.should_refetch_semantic_turn_work(thread_id.as_str(), turn_id.as_str())
        {
            self.request_semantic_turn_work_newest_page(thread_id, turn_id, cx);
        }
    }

    fn should_refetch_semantic_turn_work(&self, thread_id: &str, turn_id: &str) -> bool {
        self.semantic_timelines
            .thread(thread_id)
            .and_then(|thread| {
                thread.cached_turn_work_block(turn_id).map(|work| {
                    pioneer_client::timeline::semantic::resolve_work_expanded(
                        work,
                        &thread.expansion,
                    )
                })
            })
            .unwrap_or(false)
    }

    fn apply_cli_runtime_refresh_reduction(
        &mut self,
        reduction: CLIRuntimeRefreshReduction,
        cx: &mut Context<Self>,
    ) {
        if reduction.queue_runtime_refresh {
            self.refresh_cli_providers_auto(cx);
        }
    }

    fn apply_pending_requests_reduction(
        &mut self,
        reduction: pioneer_client::cli_runtime::approvals::PendingRequestsReduction,
        cx: &mut Context<Self>,
    ) {
        if self.pending_requests.apply(reduction) {
            cx.notify();
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

fn desktop_voice_no_speech_message(error: Option<&VoiceError>) -> String {
    let Some(details) = error.and_then(|error| desktop_voice_error_details(error.message.as_str()))
    else {
        return t!("chat.composer.voice.no_speech").to_string();
    };

    t!(
        "chat.composer.voice.no_speech_with_details",
        details = details.as_str()
    )
    .to_string()
}

fn desktop_voice_transcription_failed_message(error: Option<&VoiceError>) -> String {
    let Some(error) = error else {
        return t!("chat.composer.voice.transcription_failed").to_string();
    };

    t!(
        "chat.composer.voice.transcription_failed_with_details",
        error = error.message.as_str()
    )
    .to_string()
}

fn desktop_voice_error_details(message: &str) -> Option<String> {
    let (_, details) = message.split_once("reason=")?;
    Some(format!("reason={details}"))
}
