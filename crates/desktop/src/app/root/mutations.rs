use super::*;
use crate::state;
use pioneer_client::composer::draft as composer_draft;
use pioneer_client::state::reducers as client_state_reducers;
use pioneer_client::threads::{
    resume as thread_resume, session as thread_session, start as thread_start, tree as thread_tree,
};
use tracing::warn;

impl PioneerDesktop {
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
            self.reset_composer_model_selection_for_active_thread();
        }
    }

    pub(in crate::app) fn clear_active_thread_if_matches(&mut self, thread_id: &str) -> bool {
        let changed =
            thread_session::clear_active_thread_if_matches(&mut self.active_thread_id, thread_id);
        if changed {
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

        let value =
            composer_draft::normalize_composer_draft_text(&self.composer_state.read(cx).value());
        let attachments = self.composer_attachments.clone();
        let capabilities = self.composer_capabilities.clone();
        let permission_mode = self.composer_permission_mode;
        composer_draft::remember_thread_composer_draft(
            thread_id.as_str(),
            value,
            attachments,
            capabilities,
            permission_mode,
            &mut self.thread_drafts,
            &mut self.thread_draft_attachments,
            &mut self.thread_draft_capabilities,
            &mut self.thread_draft_permission_modes,
        );
    }

    pub(in crate::app) fn clear_thread_draft(&mut self, thread_id: &str) {
        composer_draft::clear_thread_composer_draft(
            thread_id,
            &mut self.thread_drafts,
            &mut self.thread_draft_attachments,
            &mut self.thread_draft_capabilities,
            &mut self.thread_draft_permission_modes,
        );
    }

    pub(in crate::app) fn restore_thread_draft(
        &mut self,
        thread_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let draft = composer_draft::restore_thread_composer_draft(
            thread_id,
            &self.thread_drafts,
            &self.thread_draft_attachments,
            &self.thread_draft_capabilities,
            &self.thread_draft_permission_modes,
        );
        self.composer_state.update(cx, move |state, cx| {
            state.set_value(draft.text.clone(), window, cx)
        });
        self.composer_permission_mode = draft.permission_mode;
        if self.composer_selected_provider_is_cli_runtime() {
            self.composer_attachments = draft.attachments;
            self.composer_capabilities.clear();
        } else {
            self.composer_attachments = draft.attachments;
            self.composer_capabilities = draft.capabilities;
        }
    }

    pub(in crate::app) fn activate_thread_with_draft_restore(
        &mut self,
        thread_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.remember_active_thread_draft(cx);
        self.set_active_thread_id(Some(thread_id.clone()));
        self.restore_thread_draft(thread_id.as_str(), window, cx);
    }

    pub(in crate::app) fn clear_composer(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.composer_state
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.composer_attachments.clear();
        self.composer_capabilities.clear();
        self.composer_upload_in_progress = false;
        self.composer_upload_error = None;
    }

    pub(in crate::app) fn clear_composer_payload_for_thread(&mut self, thread_id: &str) {
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.composer_attachments.clear();
            self.composer_capabilities.clear();
            self.composer_upload_in_progress = false;
            self.composer_upload_error = None;
        }
        self.clear_thread_draft(thread_id);
    }

    pub(in crate::app) fn reset_thread_start_state(&mut self) {
        client_state_reducers::reset_thread_start_coordinator(&mut self.thread_start);
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
        let workspace_id = self.thread_workspace_id(thread_id).map(str::to_owned);
        client_state_reducers::remove_thread_scoped_entries(
            thread_id,
            &mut self.draft_thread_id,
            &mut self.thread_coordinators,
            &mut self.thread_drafts,
            &mut self.thread_draft_attachments,
            &mut self.thread_draft_capabilities,
            &mut self.thread_draft_permission_modes,
            &mut self.thread_placements,
        );
        if pioneer_client::timeline::semantic::remove_thread_semantic_timeline(
            &mut self.semantic_timelines,
            thread_id,
        ) {
            self.semantic_timeline_revision = self.semantic_timeline_revision.saturating_add(1);
        }
        self.semantic_timeline_in_flight
            .retain(|key| !semantic_request_key_matches_thread(key, thread_id));
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
        client_state_reducers::clear_thread_client_state(
            &mut self.draft_thread_id,
            &mut self.thread_coordinators,
            &mut self.thread_drafts,
            &mut self.thread_draft_attachments,
            &mut self.thread_draft_capabilities,
            &mut self.thread_draft_permission_modes,
            &mut self.composer_attachments,
            &mut self.composer_capabilities,
            &mut self.composer_permission_mode,
            &mut self.composer_upload_in_progress,
            &mut self.composer_upload_error,
            &mut self.composer_selected_provider,
            &mut self.composer_selected_model,
            &mut self.composer_model_selection_manually_selected,
            &mut self.thread_folders,
            &mut self.thread_placements,
            &mut self.thread_agents_doc_summaries,
            &mut self.thread_folder_expanded,
            &mut self.thread_tree_selected_node_id,
        );
        self.semantic_timelines = Default::default();
        self.semantic_timeline_revision = self.semantic_timeline_revision.saturating_add(1);
        self.semantic_timeline_in_flight.clear();
        self.clear_composer_reasoning_effort();
        self.composer_model_display_cache.clear();
        self.composer_model_display_loading_key = None;
        self.pending_requests = Default::default();
        self.cli_runtime_thread_bindings.clear();
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
        } => request_thread_id == thread_id,
    }
}
