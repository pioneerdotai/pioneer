use super::*;
use crate::state;
use pioneer_client::composer::draft::{
    ComposerDomainDraft, ComposerDraftLifecycleAction, composer_thread_switch_fallback,
    normalize_composer_draft_text, reduce_composer_draft_lifecycle,
};
use pioneer_client::composer::state_machine::ComposerDomainAction;
use pioneer_client::state::reducers as client_state_reducers;
use pioneer_client::threads::{
    resume as thread_resume, session as thread_session, start as thread_start, tree as thread_tree,
};
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn reconcile_composer_permission_mode_with_capabilities(&mut self) {
        let allowed = self.allowed_composer_permission_modes();
        if allowed.is_empty() || allowed.contains(&self.composer_permission_mode) {
            return;
        }
        let Some(mode) = allowed.last().copied() else {
            return;
        };
        self.reduce_composer_domain(ComposerDomainAction::SetPermissionMode { mode });
    }

    pub(in crate::app) fn invalidate_active_thread_capability_projection(&mut self) {
        self.thread_scope_capabilities_refresh_generation = self
            .thread_scope_capabilities_refresh_generation
            .wrapping_add(1);
        self.thread_scope_capabilities_thread_id = None;
        self.thread_scope_capabilities_loading_thread_id = None;
        self.thread_scope_capabilities = ThreadPresentationCapabilities::default();
    }

    pub(in crate::app) fn set_main_content_view(
        &mut self,
        view: MainContentView,
        cx: &mut Context<Self>,
    ) {
        self.main_content_view = view;
        self.rebuild_sidebar_tree_state(cx);
    }

    pub(in crate::app) fn set_active_thread_id(&mut self, thread_id: Option<String>) {
        let changed = thread_session::set_active_thread_id(&mut self.active_thread_id, thread_id);
        if changed {
            self.composer_edit_target = None;
            self.invalidate_active_thread_capability_projection();
            self.thread_members_thread_id = None;
            self.thread_members.clear();
            self.thread_members_loading = false;
            self.active_thread_resubscribe_pending = self.active_thread_id.is_some()
                && self.gateway.connection_state == GatewayConnectionState::Connected;
            self.reset_composer_model_selection_for_active_thread();
        }
    }

    pub(in crate::app) fn clear_active_thread_if_matches(&mut self, thread_id: &str) -> bool {
        let changed =
            thread_session::clear_active_thread_if_matches(&mut self.active_thread_id, thread_id);
        if changed {
            self.composer_edit_target = None;
            self.reset_composer_model_selection_for_active_thread();
        }
        changed
    }

    pub(in crate::app) fn set_draft_thread_id(&mut self, thread_id: Option<String>) {
        thread_session::set_draft_thread_id(&mut self.draft_thread_id, thread_id);
    }

    pub(in crate::app) fn clear_draft_thread_if_matches(&mut self, thread_id: &str) -> bool {
        thread_session::clear_draft_thread_if_matches(&mut self.draft_thread_id, thread_id)
    }

    pub(in crate::app) fn promote_thread_from_draft(&mut self, thread_id: &str) -> bool {
        if !thread_session::promote_thread_from_draft(&mut self.draft_thread_id, thread_id) {
            return false;
        }

        if self.current_active_thread_id() == Some(thread_id) {
            // A connection-owned draft has no persisted thread projection.
            // Materialization changes that without changing the thread id, so
            // force a fresh server snapshot instead of retaining the draft's
            // fail-closed empty projection.
            self.invalidate_active_thread_capability_projection();
        }
        self.request_thread_start_if_needed();
        true
    }

    pub(in crate::app) fn resolve_existing_draft_thread_id(&mut self) -> Option<String> {
        thread_session::resolve_existing_draft_thread_id(&mut self.draft_thread_id, |thread_id| {
            self.thread_coordinators.contains_key(thread_id)
        })
    }

    pub(in crate::app) fn set_preferred_workspace_id(&mut self, workspace_id: Option<String>) {
        self.preferred_workspace_id = workspace_id;
    }

    pub(in crate::app) fn load_thread_folder_expansion_for_workspace(
        &mut self,
        workspace_id: &str,
        cx: &mut Context<Self>,
    ) {
        self.thread_folder_expanded =
            state::thread_folders_expanded_for_workspace(cx, Some(workspace_id));
    }

    pub(in crate::app) fn set_workspaces(&mut self, workspaces: Vec<Workspace>) {
        self.workspaces = workspaces;
    }

    pub(in crate::app) fn set_workspaces_loading(&mut self, loading: bool) {
        self.workspaces_loading = loading;
    }

    pub(in crate::app) fn set_workspaces_error(&mut self, error: Option<String>) {
        self.workspaces_error = error;
    }

    pub(in crate::app) fn set_workspace_action_in_progress(&mut self, in_progress: bool) {
        self.workspace_action_in_progress = in_progress;
    }

    pub(in crate::app) fn remember_last_active_thread_for_workspace(
        &mut self,
        workspace_id: &str,
        thread_id: Option<String>,
    ) {
        thread_session::remember_thread_for_workspace(
            &mut self.last_active_thread_by_workspace,
            workspace_id,
            thread_id,
        );
    }

    pub(in crate::app) fn remember_draft_thread_for_workspace(
        &mut self,
        workspace_id: &str,
        thread_id: Option<String>,
    ) {
        thread_session::remember_thread_for_workspace(
            &mut self.draft_thread_by_workspace,
            workspace_id,
            thread_id,
        );
    }

    pub(in crate::app) fn remember_active_thread_draft(&mut self, cx: &Context<Self>) {
        let Some(thread_id) = self.active_thread_id.as_ref().map(ToOwned::to_owned) else {
            return;
        };

        let draft = ComposerDomainDraft {
            text: normalize_composer_draft_text(&self.composer_state.read(cx).value()),
            domain: self.composer_domain_state(),
        };
        let transition = reduce_composer_draft_lifecycle(
            &self.composer_draft_lifecycle,
            ComposerDraftLifecycleAction::RememberThread { thread_id, draft },
        );
        self.composer_draft_lifecycle = transition.state;
    }

    pub(in crate::app) fn clear_thread_draft(&mut self, thread_id: &str) {
        let transition = reduce_composer_draft_lifecycle(
            &self.composer_draft_lifecycle,
            ComposerDraftLifecycleAction::ClearThread {
                thread_id: thread_id.to_owned(),
            },
        );
        self.composer_draft_lifecycle = transition.state;
    }

    fn apply_composer_domain_draft(
        &mut self,
        draft: ComposerDomainDraft,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let ComposerDomainDraft { text, domain } = draft;
        self.composer_state
            .update(cx, move |state, cx| state.set_value(text, window, cx));
        self.reduce_composer_domain(ComposerDomainAction::Reset { defaults: domain });
    }

    pub(in crate::app) fn restore_thread_draft(
        &mut self,
        thread_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.composer_edit_target = None;
        let fallback = composer_thread_switch_fallback(self.composer_domain_state());
        let transition = reduce_composer_draft_lifecycle(
            &self.composer_draft_lifecycle,
            ComposerDraftLifecycleAction::SwitchThread {
                current_thread_id: None,
                current_draft: None,
                target_thread_id: thread_id.to_owned(),
                fallback,
            },
        );
        self.composer_draft_lifecycle = transition.state;
        if let Some(draft) = transition.restored_draft {
            self.apply_composer_domain_draft(draft, window, cx);
        }
    }

    pub(in crate::app) fn activate_thread_with_draft_restore(
        &mut self,
        thread_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current_thread_id = self.active_thread_id.clone();
        let current_draft = current_thread_id.as_ref().and_then(|_| {
            self.composer_edit_target
                .is_none()
                .then(|| ComposerDomainDraft {
                    text: normalize_composer_draft_text(&self.composer_state.read(cx).value()),
                    domain: self.composer_domain_state(),
                })
        });
        self.composer_edit_target = None;
        self.set_active_thread_id(Some(thread_id.clone()));
        let fallback = composer_thread_switch_fallback(self.composer_domain_state());
        let transition = reduce_composer_draft_lifecycle(
            &self.composer_draft_lifecycle,
            ComposerDraftLifecycleAction::SwitchThread {
                current_thread_id,
                current_draft,
                target_thread_id: thread_id,
                fallback,
            },
        );
        self.composer_draft_lifecycle = transition.state;
        if let Some(draft) = transition.restored_draft {
            self.apply_composer_domain_draft(draft, window, cx);
        }
    }

    pub(in crate::app) fn clear_composer(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.composer_edit_target = None;
        self.composer_state
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.reduce_composer_domain(ComposerDomainAction::ClearPayload);
        self.composer_upload_in_progress = false;
        self.composer_upload_error = None;
    }

    pub(in crate::app) fn clear_composer_payload_for_thread(&mut self, thread_id: &str) {
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.composer_edit_target = None;
            self.reduce_composer_domain(ComposerDomainAction::ClearPayload);
            self.composer_upload_in_progress = false;
            self.composer_upload_error = None;
        }
        self.clear_thread_draft(thread_id);
    }

    pub(in crate::app) fn reset_thread_start_state(&mut self) {
        client_state_reducers::reset_thread_start_coordinator(&mut self.thread_start);
        self.pending_thread_create_visibility = pioneer_protocol::ThreadVisibility::Private;
    }

    pub(in crate::app) fn thread_start_coordinator_mut(&mut self) -> &mut ThreadStartCoordinator {
        &mut self.thread_start
    }

    pub(in crate::app) fn enqueue_thread_start_request(&mut self) {
        thread_start::enqueue_thread_start_request(&mut self.thread_start_requested);
    }

    pub(in crate::app) fn dequeue_thread_start_request(&mut self) -> bool {
        thread_start::dequeue_thread_start_request(&mut self.thread_start_requested)
    }

    pub(in crate::app) fn clear_thread_start_queue(&mut self) {
        thread_start::clear_thread_start_request(&mut self.thread_start_requested);
    }

    pub(in crate::app) fn enqueue_turn_resume_thread(&mut self, thread_id: String) {
        thread_resume::enqueue_turn_resume_thread(
            &mut self.ready_turn_resume_threads,
            &mut self.ready_turn_resume_thread_set,
            thread_id,
        );
    }

    pub(in crate::app) fn dequeue_turn_resume_thread(&mut self) -> Option<String> {
        thread_resume::dequeue_turn_resume_thread(
            &mut self.ready_turn_resume_threads,
            &mut self.ready_turn_resume_thread_set,
        )
    }

    pub(in crate::app) fn clear_turn_resume_queue(&mut self) {
        thread_resume::clear_turn_resume_queue(
            &mut self.ready_turn_resume_threads,
            &mut self.ready_turn_resume_thread_set,
        );
    }

    pub(in crate::app) fn upsert_thread_coordinator(
        &mut self,
        thread_id: &str,
        workspace_id: &str,
    ) -> &mut ThreadCoordinator {
        client_state_reducers::upsert_thread_coordinator_in(
            &mut self.thread_coordinators,
            thread_id,
            workspace_id,
        )
    }

    pub(in crate::app) fn upsert_thread_snapshot(
        &mut self,
        thread: Thread,
    ) -> &mut ThreadCoordinator {
        let scope_changed = self
            .thread_coordinators
            .get(thread.id.as_str())
            .and_then(ThreadCoordinator::thread)
            .is_some_and(|current| current.visibility != thread.visibility);
        if scope_changed && self.current_active_thread_id() == Some(thread.id.as_str()) {
            self.thread_members_thread_id = None;
            self.invalidate_active_thread_capability_projection();
            self.thread_members.clear();
        }
        client_state_reducers::upsert_thread_snapshot_in(&mut self.thread_coordinators, thread)
    }

    pub(in crate::app) fn thread_conversation_mut(
        &mut self,
        thread_id: &str,
    ) -> Option<&mut Conversation> {
        self.thread_coordinators
            .get_mut(thread_id)
            .map(|coordinator| &mut coordinator.conversation)
    }

    pub(in crate::app) fn upsert_thread_conversation_mut(
        &mut self,
        thread_id: &str,
        workspace_id: &str,
    ) -> &mut Conversation {
        &mut self
            .upsert_thread_coordinator(thread_id, workspace_id)
            .conversation
    }

    pub(in crate::app) fn thread_coordinator_mut(
        &mut self,
        thread_id: &str,
    ) -> Option<&mut ThreadCoordinator> {
        self.thread_coordinators.get_mut(thread_id)
    }

    pub(in crate::app) fn queue_thread_list_refresh(&mut self) {
        thread_tree::queue_thread_tree_refresh(&mut self.thread_list_refresh_requested);
    }

    pub(in crate::app) fn take_thread_list_refresh_request(&mut self) -> bool {
        thread_tree::take_thread_tree_refresh_request(&mut self.thread_list_refresh_requested)
    }

    pub(in crate::app) fn remove_thread_conversation(&mut self, thread_id: &str) {
        self.thread_unread.remove(thread_id);
        let workspace_id = self.thread_workspace_id(thread_id).map(str::to_owned);
        client_state_reducers::remove_thread_scoped_entries(
            thread_id,
            &mut self.draft_thread_id,
            &mut self.thread_coordinators,
            &mut self.thread_placements,
        );
        self.clear_thread_draft(thread_id);
        if pioneer_client::timeline::semantic::remove_thread_semantic_timeline(
            &mut self.semantic_timelines,
            thread_id,
        ) {
            self.semantic_timeline_revision = self.semantic_timeline_revision.saturating_add(1);
        }
        self.semantic_timeline_in_flight
            .retain(|key| !semantic_request_key_matches_thread(key, thread_id));
        self.semantic_timeline_pending
            .retain(|key, _| !semantic_request_key_matches_thread(key, thread_id));
        if let Some(workspace_id) = workspace_id {
            self.pending_requests.apply(
                pioneer_client::cli_runtime::approvals::reduce_pending_request_thread_closed_cleanup(
                    workspace_id,
                    thread_id.to_owned(),
                ),
            );
        }
        self.cli_runtime_thread_bindings.remove(thread_id);
    }

    pub(in crate::app) fn clear_thread_conversations(&mut self) {
        self.thread_unread.clear();
        self.message_revision_dialog = None;
        self.message_revision_loading = false;
        self.message_mutation_pending = false;
        self.composer_edit_target = None;
        client_state_reducers::clear_thread_client_state(
            &mut self.draft_thread_id,
            &mut self.thread_coordinators,
            &mut self.thread_folders,
            &mut self.thread_placements,
            &mut self.thread_agents_doc_summaries,
            &mut self.thread_folder_expanded,
            &mut self.thread_tree_selected_node_id,
        );
        self.composer_draft_lifecycle = reduce_composer_draft_lifecycle(
            &self.composer_draft_lifecycle,
            ComposerDraftLifecycleAction::ClearAll,
        )
        .state;
        self.semantic_timelines = Default::default();
        self.semantic_timeline_revision = self.semantic_timeline_revision.saturating_add(1);
        self.semantic_timeline_in_flight.clear();
        self.semantic_timeline_pending.clear();
        let mut composer_defaults = self.composer_domain_state();
        composer_defaults.attachments.clear();
        composer_defaults.capabilities.clear();
        composer_defaults.skill_selections.clear();
        composer_defaults.selected_mode =
            pioneer_client::composer::model_selection::default_composer_turn_mode();
        composer_defaults.mode_manually_selected = false;
        composer_defaults.selected_provider = None;
        composer_defaults.capability_target =
            pioneer_client::composer::capabilities::ComposerCapabilityTarget::native();
        composer_defaults.selected_model = None;
        composer_defaults.selected_reasoning_effort = None;
        composer_defaults.selected_permission_mode =
            pioneer_client::composer::permissions::default_composer_permission_mode();
        composer_defaults.model_manually_selected = false;
        composer_defaults.reply_target = None;
        composer_defaults.selected_mentions.clear();
        self.reduce_composer_domain(ComposerDomainAction::Reset {
            defaults: composer_defaults,
        });
        self.composer_upload_in_progress = false;
        self.composer_upload_error = None;
        self.composer_model_display_cache.clear();
        self.composer_model_display_loading_key = None;
        self.pending_requests = Default::default();
        self.cli_runtime_thread_bindings.clear();
    }

    pub(in crate::app) fn clear_workspace_capability_projections(&mut self) {
        self.reset_thread_start_state();
        self.clear_thread_start_queue();
        self.clear_turn_resume_queue();
        self.providers.clear_for_workspace_switch();
        self.mcp_servers.clear();
        self.mcp_selected_server_id = None;
        self.mcp_server_details = None;
        self.mcp_loading = false;
        self.mcp_details_loading = false;
        self.mcp_error = None;
        self.mcp_refresh_requested = false;
        self.mcp_details_refresh_requested = false;
        self.mcp_pending_actions.clear();
        self.installed_skills.clear();
        self.skills_catalog.clear();
        self.skills_management = Default::default();
        self.skills_health_details.clear();
        self.skills_loading = false;
        self.skills_error = None;
        self.skills_refresh_requested = false;
        self.skills_pending_actions.clear();
        self.selected_skill_target = None;
        self.composer_capabilities.clear();
        self.composer_skill_selections.clear();
        self.composer_attachments.clear();
        self.composer_reply_target = None;
        self.composer_edit_target = None;
        self.composer_selected_mentions.clear();
        self.composer_turn_mode =
            pioneer_client::composer::model_selection::default_composer_turn_mode();
        self.composer_mode_manually_selected = false;
        self.composer_selected_provider = None;
        self.composer_selected_model = None;
        self.composer_selected_reasoning_effort = None;
        self.composer_model_display_cache.clear();
        self.composer_model_display_loading_key = None;
        if !matches!(
            self.main_content_view,
            MainContentView::Settings | MainContentView::Threads
        ) {
            self.main_content_view = MainContentView::Threads;
        }
    }

    /// Clears every server-authorized projection before a connection begins a
    /// new authorization epoch. Endpoint registry and device-session state are
    /// deliberately owned by the Gateway coordinator and remain untouched.
    pub(in crate::app) fn clear_authorization_epoch_cache(&mut self) {
        self.gateway.capability_snapshot = None;
        self.workspaces.clear();
        self.workspaces_error = None;
        self.set_active_thread_id(None);
        self.clear_thread_conversations();
        self.last_active_thread_by_workspace.clear();
        self.draft_thread_by_workspace.clear();
        self.task_thread_navigation_stack.clear();
        self.ready_turn_resume_threads.clear();
        self.ready_turn_resume_thread_set.clear();
        self.thread_artifacts = Default::default();
        self.show_thread_artifacts_sidebar = false;
        self.show_thread_members_sidebar = false;
        self.thread_members_thread_id = None;
        self.thread_members.clear();
        self.thread_members_loading = false;
        self.thread_member_items.clear();
        self.active_agents_doc_editor_scope = None;
        self.agents_doc_editor = None;
        self.thread_tree_selected_node_id = None;
        *self.thread_timeline_view_state.borrow_mut() = Default::default();
        self.thread_timeline_item_expanded.borrow_mut().clear();
        self.thread_timeline_terminal_item.borrow_mut().clear();
        *self.code_highlight_cache.borrow_mut() = Default::default();
        self.task_review_actions = Default::default();
        self.gateway.settings = None;
        self.clear_workspace_capability_projections();
    }

    pub(in crate::app) fn set_cli_runtime_thread_binding(
        &mut self,
        thread_id: String,
        binding: Option<CLIRuntimeThreadBinding>,
    ) {
        match binding {
            Some(binding) => {
                self.cli_runtime_thread_bindings.insert(thread_id, binding);
            }
            None => {
                self.cli_runtime_thread_bindings.remove(thread_id.as_str());
            }
        }
    }

    pub(in crate::app) fn set_thread_tree_snapshot(
        &mut self,
        folders: Vec<ThreadFolder>,
        placements: Vec<ThreadPlacement>,
        agents_docs: Vec<ThreadAgentsDocSummary>,
    ) {
        let normalized = thread_tree::normalize_thread_tree_snapshot(
            folders,
            placements,
            &self.thread_folder_expanded,
        );
        self.thread_folders = normalized.folders_by_id;
        self.thread_folder_expanded = normalized.folder_expanded;
        self.thread_placements = normalized.placements_by_thread_id;
        self.thread_agents_doc_summaries =
            client_state_reducers::thread_agents_doc_summaries_by_scope(agents_docs);
    }

    pub(in crate::app) fn toggle_thread_folder_expanded(
        &mut self,
        folder_id: &str,
        cx: &mut Context<Self>,
    ) {
        thread_tree::toggle_thread_folder_expanded(&mut self.thread_folder_expanded, folder_id);

        self.save_thread_folder_expansion_for_active_workspace(cx);
    }

    pub(in crate::app) fn set_thread_folder_expanded(
        &mut self,
        folder_id: &str,
        expanded: bool,
        cx: &mut Context<Self>,
    ) {
        thread_tree::set_thread_folder_expanded(
            &mut self.thread_folder_expanded,
            folder_id,
            expanded,
        );

        self.save_thread_folder_expansion_for_active_workspace(cx);
    }

    fn save_thread_folder_expansion_for_active_workspace(&self, cx: &mut Context<Self>) {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return;
        };

        if let Err(error) = state::set_thread_folders_expanded_for_workspace(
            cx,
            workspace_id.as_str(),
            self.thread_folder_expanded.clone(),
        ) {
            warn!(
                error = %format!("{error:#}"),
                "failed to save sidebar folder expansion state"
            );
        }
    }

    pub(in crate::app) fn set_thread_tree_selected_node_id(&mut self, node_id: Option<String>) {
        self.thread_tree_selected_node_id = node_id;
    }

    pub(in crate::app) fn tick_thread_conversations(&mut self) -> bool {
        self.thread_coordinators
            .values_mut()
            .fold(false, |changed, coordinator| {
                coordinator.conversation.tick() || changed
            })
    }
}

fn semantic_request_key_matches_thread(key: &SemanticTimelineRequestKey, thread_id: &str) -> bool {
    match key {
        SemanticTimelineRequestKey::ThreadNewest {
            thread_id: request_thread_id,
        }
        | SemanticTimelineRequestKey::ThreadBefore {
            thread_id: request_thread_id,
            ..
        }
        | SemanticTimelineRequestKey::ThreadAfter {
            thread_id: request_thread_id,
            ..
        }
        | SemanticTimelineRequestKey::TurnWorkInitial {
            thread_id: request_thread_id,
            ..
        }
        | SemanticTimelineRequestKey::TurnWorkBefore {
            thread_id: request_thread_id,
            ..
        }
        | SemanticTimelineRequestKey::TurnWorkAfter {
            thread_id: request_thread_id,
            ..
        }
        | SemanticTimelineRequestKey::TurnWorkItems {
            thread_id: request_thread_id,
            ..
        } => request_thread_id == thread_id,
    }
}
