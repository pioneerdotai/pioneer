use super::*;
use crate::app::root::{AdministrationContentView, DesktopVoiceComposerState, MainContentView};
use crate::audio::capture::DesktopVoiceCaptureErrorKind;
use pioneer_client::administration::{AdministrationEvent, AdministrationRefetch};
use pioneer_client::authorization::{
    AccessChangedPlan, ThreadAuthorizationScope, plan_access_changed,
};
use pioneer_client::notifications::router::{
    ArtifactDeletedRefreshReduction, ArtifactThreadRefreshReduction, CLIRuntimeSnapshotReduction,
    ConversationEventReduction, SkillsRefreshReduction, ThreadArtifactsRefreshReduction,
    ThreadClosedReduction, ThreadStartedReduction, ThreadUpdatedReduction, TurnLifecycleReduction,
    WorkspacePreferenceReduction, WorkspaceRefreshReduction, apply_workspace_changed_to_catalog,
};
use pioneer_client::providers::list::CliRuntimeSnapshotUpdate;
use pioneer_client::runtime::{ClientRuntimeNotification, ClientRuntimeNotificationContext};
use pioneer_client::voice::{VoiceFinalizeUiAction, VoiceSessionResultReduction};
use pioneer_client::workspaces::selectors as workspace_selectors;
use pioneer_protocol::{VoiceError, VoiceSessionOutcome};
use std::time::Instant;

impl PioneerDesktop {
    pub(in crate::app::flow) fn apply_gateway_notification(
        &mut self,
        notification: GatewayNotification,
        cx: &mut Context<Self>,
    ) {
        let timeline_input = match &notification {
            GatewayNotification::ItemDelta(notification) => Some((
                notification.delta.len(),
                notification
                    .markdown
                    .as_ref()
                    .map(|document| document.blocks.len()),
                if notification.markdown.is_some() {
                    pioneer_observability::DesktopTimelineContentKind::Markdown
                } else {
                    pioneer_observability::DesktopTimelineContentKind::PlainText
                },
            )),
            _ => None,
        };
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
        let reduce_started = timeline_input.map(|_| Instant::now());
        let reduction = self
            .gateway
            .client_runtime
            .reduce_gateway_notification(notification, context);
        if let (Some((input_bytes, block_count, content)), Some(started)) =
            (timeline_input, reduce_started.as_ref())
        {
            pioneer_observability::record_desktop_timeline_stage(
                pioneer_observability::DesktopTimelineStageMetric {
                    stage: pioneer_observability::DesktopTimelineStage::NotificationReduce,
                    cache: pioneer_observability::DesktopTimelineCacheStatus::NotApplicable,
                    content,
                    outcome: if reduction.is_some() {
                        pioneer_observability::DesktopTimelineOutcome::Ok
                    } else {
                        pioneer_observability::DesktopTimelineOutcome::Skipped
                    },
                    elapsed: started.elapsed(),
                    input_bytes: Some(input_bytes),
                    block_count,
                    row_count: None,
                },
            );
        }
        let Some(reduction) = reduction else {
            return;
        };
        let apply_started = timeline_input.map(|_| Instant::now());
        self.apply_gateway_notification_reduction(reduction, cx);
        if let (Some((input_bytes, block_count, content)), Some(started)) =
            (timeline_input, apply_started.as_ref())
        {
            pioneer_observability::record_desktop_timeline_stage(
                pioneer_observability::DesktopTimelineStageMetric {
                    stage: pioneer_observability::DesktopTimelineStage::NotificationApply,
                    cache: pioneer_observability::DesktopTimelineCacheStatus::NotApplicable,
                    content,
                    outcome: pioneer_observability::DesktopTimelineOutcome::Ok,
                    elapsed: started.elapsed(),
                    input_bytes: Some(input_bytes),
                    block_count,
                    row_count: None,
                },
            );
        }
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
            ClientRuntimeNotification::AccessChanged(notification) => {
                self.apply_access_changed_notification(notification, cx);
            }
            ClientRuntimeNotification::AuthorizationProjectionChanged(notification) => {
                self.apply_authorization_projection_changed_notification(notification, cx);
            }
            ClientRuntimeNotification::AdministrationChanged(event) => {
                self.apply_administration_event(event, cx);
            }
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
                self.apply_thread_updated_reduction(reduction, cx);
            }
            ClientRuntimeNotification::ThreadParticipantsChanged(notification) => {
                self.apply_thread_participants_changed_notification(notification, cx);
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
            ClientRuntimeNotification::CLIRuntimeSnapshot(reduction) => {
                self.apply_cli_runtime_snapshot_reduction(reduction, cx);
            }
            ClientRuntimeNotification::CLIRuntimePendingRequests(reduction) => {
                self.apply_pending_requests_reduction(reduction, cx);
            }
            ClientRuntimeNotification::PendingRequests { reduction } => {
                self.apply_pending_requests_reduction(reduction, cx);
            }
            ClientRuntimeNotification::TaskUserNotificationDelivered(notification) => {
                self.apply_task_user_notification_delivered(notification, cx);
            }
            ClientRuntimeNotification::GatewayRemoteAccessStatusChanged(notification) => {
                self.apply_remote_access_status_changed(notification.status, cx);
            }
            ClientRuntimeNotification::GatewayThreadEpisodicVectorRefillStatusChanged(
                notification,
            ) => {
                self.apply_thread_episodic_vector_refill_status_changed(notification, cx);
            }
            ClientRuntimeNotification::GatewayVoiceInputStatusChanged(notification) => {
                self.apply_voice_input_status_changed(notification.settings, cx);
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

    fn apply_task_user_notification_delivered(
        &mut self,
        _notification: pioneer_protocol::TaskUserNotificationDeliveredNotification,
        cx: &mut Context<Self>,
    ) {
        // The websocket event is only a live invalidation hint. Durable inbox
        // reconciliation is performed by the Task notification controller so
        // reconnect and foreground recovery use the same server-owned source.
        self.refresh_task_user_notifications(cx);
        cx.notify();
    }

    fn apply_administration_event(&mut self, event: AdministrationEvent, cx: &mut Context<Self>) {
        let current_profile_changed = matches!(
            &event,
            AdministrationEvent::MemberChanged(notification)
                if self.gateway.current_auth.as_ref().is_some_and(|auth| {
                    auth.principal.id == notification.principal_id
                })
        );
        let invalidation = self.administration.apply_event(&event);
        if invalidation.apply {
            self.apply_administration_refetches(invalidation.effects, cx);
            if current_profile_changed {
                self.refresh_current_principal(cx);
            }
        }
    }

    pub(in crate::app) fn apply_administration_refetches(
        &mut self,
        effects: Vec<AdministrationRefetch>,
        cx: &mut Context<Self>,
    ) {
        for effect in effects {
            match effect {
                AdministrationRefetch::InvitationList
                    if self.main_content_view == MainContentView::Administration
                        && self.administration_content_view
                            == AdministrationContentView::Invitations =>
                {
                    self.refresh_invitations(false, cx);
                }
                AdministrationRefetch::MemberDirectory => {
                    self.members_error = None;
                    if self.main_content_view == MainContentView::Administration
                        && self.administration_content_view == AdministrationContentView::Members
                    {
                        self.refresh_members(false, cx);
                    }
                }
                AdministrationRefetch::WorkspaceMembers { workspace_id }
                    if self.main_content_view == MainContentView::Administration
                        && self.administration_content_view
                            == AdministrationContentView::Members =>
                {
                    self.refresh_workspace_members(workspace_id, cx);
                }
                _ => {}
            }
        }
    }

    fn apply_access_changed_notification(
        &mut self,
        notification: pioneer_protocol::AccessChangedNotification,
        cx: &mut Context<Self>,
    ) {
        let active_workspace_id = self.active_workspace_id().map(str::to_owned);
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);
        let known_threads = desktop_thread_authorization_scopes(&self.thread_coordinators);
        let plan = plan_access_changed(
            &notification,
            self.gateway.authorization_revision,
            active_workspace_id.as_deref(),
            active_thread_id.as_deref(),
            known_threads.as_slice(),
        );
        if !plan.apply {
            return;
        }

        let administration_invalidation = self.administration.apply_access_changed(&notification);

        self.gateway.authorization_revision = Some(plan.authorization_revision);
        self.gateway
            .authorization_projections
            .invalidate_for_revision(plan.authorization_revision);
        // A newer revision is one atomic fence across global, workspace and
        // thread projections. No capability from the previous generation may
        // remain readable while its replacement is fetched.
        self.invalidate_active_thread_capability_projection();
        self.gateway.capability_snapshot = None;
        self.reconcile_composer_draft_with_capabilities();
        if plan.clear_active_thread || plan.clear_active_workspace {
            self.thread_scope_pending = Default::default();
            self.thread_scope_error = None;
            self.message_revision_dialog = None;
            self.message_revision_loading = false;
            self.message_mutation_pending = false;
        }
        if notification.outcome == pioneer_protocol::AccessChangeOutcome::Revoked {
            apply_desktop_workspace_catalog_invalidation(
                &mut self.workspaces,
                &mut self.preferred_workspace_id,
                &plan,
            );
        }
        self.workspaces_error = None;

        self.thread_artifacts
            .remove_threads(plan.invalidate_thread_ids.as_slice());
        for thread_id in &plan.invalidate_thread_ids {
            self.remove_thread_conversation(thread_id.as_str());
        }
        let invalidated_thread_ids = plan
            .invalidate_thread_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        self.thread_unread
            .retain(|thread_id, _| !invalidated_thread_ids.contains(thread_id.as_str()));
        let workspace_wide = plan.change == pioneer_protocol::AccessChangeKind::WorkspaceMembership;
        let workspace_access_lost = workspace_wide
            && notification.outcome == pioneer_protocol::AccessChangeOutcome::Revoked;
        if workspace_wide && active_workspace_id.as_deref() == Some(plan.workspace_id.as_str()) {
            // Membership/role changes fence every provider projection from
            // the previous authorization generation. The shared client
            // effect below reloads both catalogs through current-ACL APIs.
            self.providers.clear_for_workspace_switch();
            self.sync_open_model_selector_cli_runtime_snapshot();
        }
        self.task_thread_navigation_stack.retain(|entry| {
            !(workspace_access_lost && entry.workspace_id == plan.workspace_id)
                && !invalidated_thread_ids.contains(entry.parent_thread_id.as_str())
                && !invalidated_thread_ids.contains(entry.child_thread_id.as_str())
        });
        self.ready_turn_resume_threads
            .retain(|thread_id| !invalidated_thread_ids.contains(thread_id.as_str()));
        self.ready_turn_resume_thread_set
            .retain(|thread_id| !invalidated_thread_ids.contains(thread_id.as_str()));
        self.last_active_thread_by_workspace
            .retain(|workspace_id, thread_id| {
                !(workspace_access_lost && workspace_id == &plan.workspace_id)
                    && !invalidated_thread_ids.contains(thread_id.as_str())
            });
        self.draft_thread_by_workspace
            .retain(|workspace_id, thread_id| {
                !(workspace_access_lost && workspace_id == &plan.workspace_id)
                    && !invalidated_thread_ids.contains(thread_id.as_str())
            });

        if workspace_access_lost {
            let removed_folder_ids = self
                .thread_folders
                .iter()
                .filter(|(_, folder)| folder.workspace_id == plan.workspace_id)
                .map(|(folder_id, _)| folder_id.clone())
                .collect::<Vec<_>>();
            self.thread_folders
                .retain(|_, folder| folder.workspace_id != plan.workspace_id);
            self.thread_placements
                .retain(|_, placement| placement.workspace_id != plan.workspace_id);
            self.thread_agents_doc_summaries
                .retain(|_, summary| summary.workspace_id != plan.workspace_id);
            for folder_id in removed_folder_ids {
                self.thread_folder_expanded.remove(folder_id.as_str());
            }
        }

        let active_editor_lost = workspace_access_lost
            && self
                .active_agents_doc_editor_scope
                .as_ref()
                .is_some_and(|scope| scope.workspace_id() == plan.workspace_id);
        if active_editor_lost {
            self.active_agents_doc_editor_scope = None;
            self.agents_doc_editor = None;
            if self.main_content_view == MainContentView::AgentsDoc {
                self.main_content_view = MainContentView::Threads;
            }
        }

        if workspace_access_lost {
            self.pending_requests.apply(
                pioneer_client::cli_runtime::approvals::PendingRequestsReduction::ClearWorkspace {
                    workspace_id: plan.workspace_id.clone(),
                },
            );
        }

        if plan.clear_active_thread {
            self.set_active_thread_id(None);
            self.thread_artifacts.activate_thread(None);
            self.thread_tree_selected_node_id = None;
            *self.thread_timeline_view_state.borrow_mut() = Default::default();
            self.thread_timeline_item_expanded.borrow_mut().clear();
            self.thread_timeline_terminal_item.borrow_mut().clear();
            *self.code_highlight_cache.borrow_mut() = Default::default();
            self.show_thread_artifacts_sidebar = false;
            self.show_thread_members_sidebar = false;
            self.thread_members_thread_id = None;
            self.thread_members.clear();
            self.thread_members_loading = false;
            self.task_review_actions = Default::default();
        }

        if plan.clear_workspace_capability_projections {
            self.clear_workspace_capability_projections();
            self.clear_persisted_active_gateway_workspace_id();
        }

        let affected_workspace_is_active =
            active_workspace_id.as_deref() == Some(plan.workspace_id.as_str());
        if affected_workspace_is_active
            && (workspace_access_lost || !plan.invalidate_thread_ids.is_empty())
        {
            self.rebuild_sidebar_tree_state(cx);
        }
        execute_desktop_client_effects(self, plan.effects, cx);
        if administration_invalidation.apply {
            self.apply_administration_refetches(administration_invalidation.effects, cx);
        }
        self.refresh_current_principal(cx);
        cx.notify();
    }

    fn apply_authorization_projection_changed_notification(
        &mut self,
        notification: pioneer_protocol::AuthorizationProjectionChangedNotification,
        cx: &mut Context<Self>,
    ) {
        let (invalidate_workspace, invalidate_thread) = desktop_authorization_projection_effects(
            &notification.affected,
            self.active_workspace_id(),
            self.current_active_thread_id(),
        );
        if !invalidate_workspace && !invalidate_thread {
            return;
        }
        let generation = notification.policy_generation.get();
        if self
            .gateway
            .authorization_revision
            // `access/changed` and the typed projection event intentionally
            // share one durable generation for the same ACL commit. The
            // access event can arrive first, so equality must still apply the
            // typed, exact-scope cache invalidation.
            .is_some_and(|current| current > generation)
        {
            return;
        }
        self.gateway.authorization_revision = Some(generation);
        self.gateway
            .authorization_projections
            .invalidate_for_revision(generation);

        self.gateway.capability_snapshot = None;
        self.invalidate_active_thread_capability_projection();
        self.reconcile_composer_draft_with_capabilities();
        // The durable generation is a fail-closed fence, not merely a cache
        // hint.  Once old projections are removed, immediately rebuild both
        // active scopes from the Gateway so a connected client cannot remain
        // indefinitely disabled (or retain a stale draft) until an unrelated
        // lifecycle event happens to refresh it.
        self.refresh_current_principal(cx);
        self.ensure_active_thread_capabilities_loaded(true, cx);
        cx.notify();
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

    fn apply_thread_episodic_vector_refill_status_changed(
        &mut self,
        notification: pioneer_protocol::GatewayThreadEpisodicVectorRefillStatusChangedNotification,
        cx: &mut Context<Self>,
    ) {
        let Some(settings) = self.gateway.settings.as_mut() else {
            return;
        };
        let should_refresh = apply_vector_refill_notification(
            &mut settings.thread_episodic.vector_search,
            &notification,
        );
        cx.notify();

        if should_refresh {
            self.refresh_gateway_settings(cx);
        }
    }

    fn apply_voice_input_status_changed(
        &mut self,
        settings: pioneer_protocol::GatewayVoiceInputSettings,
        cx: &mut Context<Self>,
    ) {
        self.desktop_voice_status = settings.runtime.phase.coarse_voice_status();
        self.desktop_voice_status_error =
            settings.runtime.error.as_ref().map(|error| error.clone());
        self.desktop_voice_status_poll_generation =
            self.desktop_voice_status_poll_generation.saturating_add(1);
        let Some(current) = self.gateway.settings.as_mut() else {
            self.refresh_gateway_settings(cx);
            cx.notify();
            return;
        };
        current.voice_input = settings;
        self.gateway.settings_error = None;
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
        let terminal_work_reconciliation =
            pioneer_client::timeline::semantic::terminal_turn_work_reconciliation(
                &self.semantic_timelines,
                &reduction.conversation_event,
            );
        if let (Some(reconciliation), Some(cx)) = (terminal_work_reconciliation, cx.as_deref_mut())
        {
            self.request_semantic_turn_work_items(
                reconciliation.thread_id,
                reconciliation.turn_id,
                reconciliation.running_work_item_ids,
                cx,
            );
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

    fn apply_thread_updated_reduction(
        &mut self,
        reduction: ThreadUpdatedReduction,
        cx: &mut Context<Self>,
    ) {
        let affected_workspace_is_active =
            self.active_workspace_id() == Some(reduction.workspace_id.as_str());
        if let Some(placement) = reduction.placement {
            self.thread_placements
                .insert(reduction.thread_id.clone(), placement);
        }
        self.upsert_thread_snapshot(reduction.thread);
        self.upsert_thread_for_workspace(
            reduction.thread_id.as_str(),
            reduction.workspace_id.as_str(),
        );
        if reduction.sync_composer_model_selection {
            self.sync_composer_model_selection_for_active_thread();
        }
        if affected_workspace_is_active {
            self.rebuild_sidebar_tree_state(cx);
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
        let mut reconcile_work_items = None;
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
                reconcile_work_items = Some((
                    notification.thread_id.clone(),
                    notification.turn_id.clone(),
                    notification.changed_work_item_ids.clone(),
                ));
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
        if let Some((thread_id, turn_id, work_item_ids)) = reconcile_work_items
            && self.should_refetch_semantic_turn_work(thread_id.as_str(), turn_id.as_str())
        {
            self.request_semantic_turn_work_items(thread_id, turn_id, work_item_ids, cx);
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

    fn apply_cli_runtime_snapshot_reduction(
        &mut self,
        reduction: CLIRuntimeSnapshotReduction,
        cx: &mut Context<Self>,
    ) {
        match reduction {
            CLIRuntimeSnapshotReduction::Upsert {
                revision,
                runtime,
                removed,
                workspace_matches: true,
                ..
            } => {
                match self
                    .providers
                    .apply_cli_runtime_snapshot_update(revision, *runtime, removed)
                {
                    CliRuntimeSnapshotUpdate::Applied => {
                        self.refresh_composer_capability_target_for_selected_provider();
                        self.sync_open_model_selector_cli_runtime_snapshot();
                        cx.notify();
                    }
                    CliRuntimeSnapshotUpdate::ReloadRequired => {
                        self.load_cli_provider_snapshot(cx);
                    }
                    CliRuntimeSnapshotUpdate::Stale => {}
                }
            }
            CLIRuntimeSnapshotReduction::Reload {
                workspace_matches: true,
                ..
            } => self.load_cli_provider_snapshot(cx),
            CLIRuntimeSnapshotReduction::Upsert { .. }
            | CLIRuntimeSnapshotReduction::Reload { .. } => {}
        }
    }

    pub(in crate::app) fn apply_pending_requests_reduction(
        &mut self,
        reduction: pioneer_client::cli_runtime::approvals::PendingRequestsReduction,
        cx: &mut Context<Self>,
    ) {
        let resolved_request_id = match &reduction {
            pioneer_client::cli_runtime::approvals::PendingRequestsReduction::Resolved {
                request_id,
            } => Some(request_id.clone()),
            _ => None,
        };
        let pending_changed = self.pending_requests.apply(reduction);
        let semantic_changed = resolved_request_id.is_some_and(|request_id| {
            pioneer_client::timeline::semantic::remove_pending_request_blocks(
                &mut self.semantic_timelines,
                request_id.as_str(),
            )
        });
        if semantic_changed {
            self.semantic_timeline_revision = self.semantic_timeline_revision.saturating_add(1);
        }
        if pending_changed || semantic_changed {
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

fn desktop_authorization_projection_effects(
    affected: &pioneer_protocol::AuthorizationChangeScope,
    active_workspace_id: Option<&str>,
    active_thread_id: Option<&str>,
) -> (bool, bool) {
    match affected {
        pioneer_protocol::AuthorizationChangeScope::Global
        | pioneer_protocol::AuthorizationChangeScope::Role { .. }
        | pioneer_protocol::AuthorizationChangeScope::Principal { .. } => (true, true),
        pioneer_protocol::AuthorizationChangeScope::PrincipalWorkspace { workspace_id, .. } => {
            let active = active_workspace_id == Some(workspace_id.as_str());
            (active, active)
        }
        pioneer_protocol::AuthorizationChangeScope::PrincipalThread {
            workspace_id,
            thread_id,
            ..
        } => (
            false,
            active_workspace_id == Some(workspace_id.as_str())
                && active_thread_id == Some(thread_id.as_str()),
        ),
        pioneer_protocol::AuthorizationChangeScope::Invitation { .. } => (false, false),
        pioneer_protocol::AuthorizationChangeScope::Workspace { workspace_id }
        | pioneer_protocol::AuthorizationChangeScope::ResourceSelector { workspace_id, .. } => {
            let active = active_workspace_id == Some(workspace_id.as_str());
            (active, active)
        }
        pioneer_protocol::AuthorizationChangeScope::Thread {
            workspace_id,
            thread_id,
        } => (
            false,
            active_workspace_id == Some(workspace_id.as_str())
                && active_thread_id == Some(thread_id.as_str()),
        ),
    }
}

fn desktop_thread_authorization_scopes(
    coordinators: &std::collections::HashMap<String, crate::app::thread::ThreadCoordinator>,
) -> Vec<ThreadAuthorizationScope> {
    coordinators
        .iter()
        .map(|(thread_id, coordinator)| ThreadAuthorizationScope {
            thread_id: thread_id.clone(),
            workspace_id: coordinator.workspace_id.clone(),
        })
        .collect()
}

#[cfg(test)]
fn desktop_access_change_invalidates_workspace_capability_snapshot(
    notification: &pioneer_protocol::AccessChangedNotification,
) -> bool {
    notification.change == pioneer_protocol::AccessChangeKind::WorkspaceMembership
}

fn apply_desktop_workspace_catalog_invalidation(
    workspaces: &mut Vec<pioneer_protocol::Workspace>,
    preferred_workspace_id: &mut Option<String>,
    plan: &AccessChangedPlan,
) {
    if plan.change != pioneer_protocol::AccessChangeKind::WorkspaceMembership {
        return;
    }
    workspaces.retain(|workspace| workspace.id != plan.workspace_id);
    if preferred_workspace_id.as_deref() == Some(plan.workspace_id.as_str()) {
        *preferred_workspace_id = None;
    }
}

#[cfg(test)]
mod access_change_tests {
    use super::*;
    use crate::app::thread::ThreadCoordinator;
    use pioneer_protocol::{AccessChangeKind, AccessChangedNotification};
    use std::collections::HashMap;

    #[::core::prelude::v1::test]
    fn policy_generation_change_invalidates_only_the_exact_desktop_scope() {
        let principal_id = pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap();
        assert_eq!(
            desktop_authorization_projection_effects(
                &pioneer_protocol::AuthorizationChangeScope::PrincipalThread {
                    principal_id: principal_id.clone(),
                    workspace_id: "workspace-active".to_owned(),
                    thread_id: "thread-active".to_owned(),
                },
                Some("workspace-active"),
                Some("thread-active"),
            ),
            (false, true)
        );
        assert_eq!(
            desktop_authorization_projection_effects(
                &pioneer_protocol::AuthorizationChangeScope::PrincipalThread {
                    principal_id,
                    workspace_id: "workspace-active".to_owned(),
                    thread_id: "thread-other".to_owned(),
                },
                Some("workspace-active"),
                Some("thread-active"),
            ),
            (false, false)
        );
        assert_eq!(
            desktop_authorization_projection_effects(
                &pioneer_protocol::AuthorizationChangeScope::Workspace {
                    workspace_id: "workspace-active".to_owned(),
                },
                Some("workspace-active"),
                Some("thread-active"),
            ),
            (true, true)
        );
        assert_eq!(
            desktop_authorization_projection_effects(
                &pioneer_protocol::AuthorizationChangeScope::Invitation {
                    invitation_id: pioneer_protocol::InvitationId::new("I00000000000000000001",)
                        .unwrap(),
                },
                Some("workspace-active"),
                Some("thread-active"),
            ),
            (false, false)
        );
    }

    #[::core::prelude::v1::test]
    fn thread_scoped_access_changes_retain_verified_workspace_capabilities() {
        for change in [
            AccessChangeKind::ThreadCreated,
            AccessChangeKind::ThreadVisibility,
            AccessChangeKind::ThreadParticipantAdded,
            AccessChangeKind::ThreadParticipantRemoved,
        ] {
            assert!(
                !desktop_access_change_invalidates_workspace_capability_snapshot(
                    &AccessChangedNotification {
                        authorization_revision: 2,
                        workspace_id: "workspace-member".to_owned(),
                        thread_id: Some("thread-member".to_owned()),
                        outcome: pioneer_protocol::AccessChangeOutcome::Retained,
                        change,
                    },
                ),
                "{change:?} must refresh without collapsing workspace agent capabilities"
            );
        }

        assert!(
            desktop_access_change_invalidates_workspace_capability_snapshot(
                &AccessChangedNotification {
                    authorization_revision: 3,
                    workspace_id: "workspace-member".to_owned(),
                    thread_id: None,
                    outcome: pioneer_protocol::AccessChangeOutcome::Retained,
                    change: AccessChangeKind::WorkspaceMembership,
                },
            )
        );
    }

    fn workspace(id: &str) -> pioneer_protocol::Workspace {
        pioneer_protocol::Workspace {
            id: id.to_owned(),
            name: format!("{id} workspace"),
            is_active: true,
            is_current: false,
            created_at: 1,
            updated_at: 2,
        }
    }

    #[::core::prelude::v1::test]
    fn desktop_adapter_uses_shared_plan_and_keeps_unrelated_workspace_threads() {
        let coordinators = HashMap::from([
            (
                "thread-protected".to_owned(),
                ThreadCoordinator::pending("thread-protected", "workspace-protected"),
            ),
            (
                "thread-kept".to_owned(),
                ThreadCoordinator::pending("thread-kept", "workspace-kept"),
            ),
        ]);
        let scopes = desktop_thread_authorization_scopes(&coordinators);

        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 9,
                workspace_id: "workspace-protected".to_owned(),
                thread_id: None,
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::WorkspaceMembership,
            },
            Some(8),
            Some("workspace-protected"),
            Some("thread-protected"),
            scopes.as_slice(),
        );

        assert!(plan.apply);
        assert!(plan.clear_active_workspace);
        assert!(plan.clear_active_thread);
        assert_eq!(plan.invalidate_thread_ids, vec!["thread-protected"]);
        assert!(scopes.iter().any(|scope| scope.thread_id == "thread-kept"));
    }

    #[::core::prelude::v1::test]
    fn desktop_adapter_ignores_stale_access_change_without_superuser_state_effects() {
        let coordinators = HashMap::from([(
            "thread-superuser".to_owned(),
            ThreadCoordinator::pending("thread-superuser", "workspace-superuser"),
        )]);
        let scopes = desktop_thread_authorization_scopes(&coordinators);

        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 4,
                workspace_id: "workspace-superuser".to_owned(),
                thread_id: Some("thread-superuser".to_owned()),
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::ThreadVisibility,
            },
            Some(4),
            Some("workspace-superuser"),
            Some("thread-superuser"),
            scopes.as_slice(),
        );

        assert!(!plan.apply);
        assert!(plan.invalidate_thread_ids.is_empty());
        assert!(plan.effects.is_empty());
    }

    #[::core::prelude::v1::test]
    fn desktop_thread_access_loss_preserves_workspace_catalog_and_preference() {
        let mut workspaces = vec![workspace("workspace-kept"), workspace("workspace-affected")];
        let mut preferred_workspace_id = Some("workspace-affected".to_owned());
        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 5,
                workspace_id: "workspace-affected".to_owned(),
                thread_id: Some("thread-affected".to_owned()),
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::ThreadParticipantRemoved,
            },
            Some(4),
            Some("workspace-affected"),
            Some("thread-affected"),
            &[
                ThreadAuthorizationScope {
                    thread_id: "thread-affected".to_owned(),
                    workspace_id: "workspace-affected".to_owned(),
                },
                ThreadAuthorizationScope {
                    thread_id: "thread-kept".to_owned(),
                    workspace_id: "workspace-affected".to_owned(),
                },
            ],
        );

        assert_eq!(plan.invalidate_thread_ids, vec!["thread-affected"]);
        apply_desktop_workspace_catalog_invalidation(
            &mut workspaces,
            &mut preferred_workspace_id,
            &plan,
        );

        assert_eq!(
            workspaces,
            vec![workspace("workspace-kept"), workspace("workspace-affected")]
        );
        assert_eq!(
            preferred_workspace_id.as_deref(),
            Some("workspace-affected")
        );
    }

    #[::core::prelude::v1::test]
    fn desktop_access_loss_wiring_evicts_every_protected_projection() {
        let source = include_str!("ws_events_notifications.rs");
        let production_source = source
            .split_once("#[cfg(test)]\nmod access_change_tests")
            .map(|(production_source, _)| production_source)
            .expect("access-change tests must remain separated from production wiring");
        for required in [
            "remove_threads(plan.invalidate_thread_ids.as_slice())",
            "self.remove_thread_conversation(thread_id.as_str())",
            "self.thread_folders",
            "self.thread_placements",
            "self.thread_agents_doc_summaries",
            "self.active_agents_doc_editor_scope = None",
            "PendingRequestsReduction::ClearWorkspace",
            "*self.thread_timeline_view_state.borrow_mut() = Default::default()",
            "self.thread_timeline_item_expanded.borrow_mut().clear()",
            "self.thread_timeline_terminal_item.borrow_mut().clear()",
            "*self.code_highlight_cache.borrow_mut() = Default::default()",
            "self.clear_workspace_capability_projections()",
            "self.clear_persisted_active_gateway_workspace_id()",
            "execute_desktop_client_effects(self, plan.effects, cx)",
        ] {
            assert!(
                production_source.contains(required),
                "Desktop access-loss path is missing `{required}`"
            );
        }
        for forbidden in [
            "remove_gateway(",
            "delete_gateway(",
            "clear_refresh_credential(",
            "revoke_auth_session(",
        ] {
            assert!(
                !production_source.contains(forbidden),
                "Desktop access-loss path must preserve endpoint/session state: `{forbidden}`"
            );
        }

        let mutations_source = include_str!("../root/mutations.rs");
        for required in [
            "self.providers.clear_for_workspace_switch()",
            "self.mcp_servers.clear()",
            "self.installed_skills.clear()",
            "self.composer_capabilities.clear()",
            "self.composer_skill_selections.clear()",
        ] {
            assert!(
                mutations_source.contains(required),
                "Desktop capability cleanup is missing `{required}`"
            );
        }
    }

    #[::core::prelude::v1::test]
    fn desktop_routes_administration_events_through_shared_revisioned_invalidation() {
        let source = include_str!("ws_events_notifications.rs");
        let production_source = source
            .split_once("#[cfg(test)]\nmod access_change_tests")
            .map(|(production_source, _)| production_source)
            .expect("Desktop notification tests must remain outside production wiring");

        assert!(production_source.contains("self.administration.apply_event(&event)"));
        assert!(
            production_source.contains("self.administration.apply_access_changed(&notification)")
        );
        assert!(
            production_source.contains(
                "ClientRuntimeNotification::AuthorizationProjectionChanged(notification)"
            )
        );
        assert!(production_source.contains("self.refresh_current_principal(cx)"));
        assert!(
            production_source.contains("self.ensure_active_thread_capabilities_loaded(true, cx)")
        );
        assert!(
            !production_source
                .contains("ClientRuntimeNotification::AdministrationChanged(_) => {}")
        );
    }
}

fn apply_vector_refill_notification(
    vector_search: &mut pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings,
    notification: &pioneer_protocol::GatewayThreadEpisodicVectorRefillStatusChangedNotification,
) -> bool {
    vector_search.refill_status = notification.status;
    if let Some(local_model_status) = notification.local_model_status {
        vector_search.local_model_status = local_model_status;
        if local_model_status
            == pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Downloading
        {
            vector_search.downloaded_bytes = notification.downloaded_bytes;
            vector_search.total_bytes = notification.total_bytes;
        } else {
            vector_search.downloaded_bytes = None;
            vector_search.total_bytes = None;
        }
    } else if notification.status
        == pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Running
        && vector_search.provider
            == Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local)
        && vector_search.local_model_status
            != pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Installed
    {
        vector_search.local_model_status =
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Downloading;
    }

    let terminal = matches!(
        notification.status,
        pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Complete
            | pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Failed
    );
    if terminal {
        vector_search.downloaded_bytes = None;
        vector_search.total_bytes = None;
    }
    terminal
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

#[cfg(test)]
mod vector_refill_tests {
    use super::*;
    use pioneer_protocol::{
        GatewayThreadEpisodicVectorLocalModelStatus, GatewayThreadEpisodicVectorProvider,
        GatewayThreadEpisodicVectorRefillStatus,
        GatewayThreadEpisodicVectorRefillStatusChangedNotification,
        GatewayThreadEpisodicVectorSearchSettings,
    };

    fn notification(
        status: GatewayThreadEpisodicVectorRefillStatus,
        local_model_status: Option<GatewayThreadEpisodicVectorLocalModelStatus>,
        downloaded_bytes: Option<u64>,
        total_bytes: Option<u64>,
    ) -> GatewayThreadEpisodicVectorRefillStatusChangedNotification {
        GatewayThreadEpisodicVectorRefillStatusChangedNotification {
            workspace_id: "workspace-a".to_owned(),
            status,
            local_model_status,
            downloaded_bytes,
            total_bytes,
        }
    }

    #[::core::prelude::v1::test]
    fn vector_refill_progress_reducer_covers_download_and_terminal_states() {
        let mut settings = GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProvider::Local),
            model: Some("bge-small-en-v1.5".to_owned()),
            local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::Missing,
            ..GatewayThreadEpisodicVectorSearchSettings::default()
        };

        let terminal = apply_vector_refill_notification(
            &mut settings,
            &notification(
                GatewayThreadEpisodicVectorRefillStatus::Running,
                Some(GatewayThreadEpisodicVectorLocalModelStatus::Downloading),
                Some(16 * 1024 * 1024),
                Some(64 * 1024 * 1024),
            ),
        );
        assert!(!terminal);
        assert_eq!(
            settings.local_model_status,
            GatewayThreadEpisodicVectorLocalModelStatus::Downloading
        );
        assert_eq!(settings.downloaded_bytes, Some(16 * 1024 * 1024));
        assert_eq!(settings.total_bytes, Some(64 * 1024 * 1024));

        let terminal = apply_vector_refill_notification(
            &mut settings,
            &notification(
                GatewayThreadEpisodicVectorRefillStatus::Running,
                Some(GatewayThreadEpisodicVectorLocalModelStatus::Installed),
                None,
                None,
            ),
        );
        assert!(!terminal);
        assert_eq!(
            settings.local_model_status,
            GatewayThreadEpisodicVectorLocalModelStatus::Installed
        );
        assert_eq!(settings.downloaded_bytes, None);
        assert_eq!(settings.total_bytes, None);

        settings.downloaded_bytes = Some(64 * 1024 * 1024);
        settings.total_bytes = Some(64 * 1024 * 1024);
        let terminal = apply_vector_refill_notification(
            &mut settings,
            &notification(
                GatewayThreadEpisodicVectorRefillStatus::Failed,
                Some(GatewayThreadEpisodicVectorLocalModelStatus::Failed),
                None,
                None,
            ),
        );
        assert!(terminal);
        assert_eq!(
            settings.local_model_status,
            GatewayThreadEpisodicVectorLocalModelStatus::Failed
        );
        assert_eq!(settings.downloaded_bytes, None);
        assert_eq!(settings.total_bytes, None);
    }

    #[::core::prelude::v1::test]
    fn vector_refill_progress_reducer_accepts_legacy_running_notification() {
        let mut settings = GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProvider::Local),
            local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::Missing,
            ..GatewayThreadEpisodicVectorSearchSettings::default()
        };

        let terminal = apply_vector_refill_notification(
            &mut settings,
            &notification(
                GatewayThreadEpisodicVectorRefillStatus::Running,
                None,
                None,
                None,
            ),
        );

        assert!(!terminal);
        assert_eq!(
            settings.local_model_status,
            GatewayThreadEpisodicVectorLocalModelStatus::Downloading
        );
    }
}
