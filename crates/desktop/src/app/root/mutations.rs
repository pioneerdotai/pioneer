use super::*;
use crate::state;
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
        let changed = self.active_thread_id != thread_id;
        self.active_thread_id = thread_id;
        if changed {
            self.reset_composer_model_selection_for_active_thread();
        }
    }

    pub(in crate::app) fn clear_active_thread_if_matches(&mut self, thread_id: &str) -> bool {
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.set_active_thread_id(None);
            return true;
        }
        false
    }

    pub(in crate::app) fn set_draft_thread_id(&mut self, thread_id: Option<String>) {
        self.draft_thread_id = thread_id;
    }

    pub(in crate::app) fn clear_draft_thread_if_matches(&mut self, thread_id: &str) -> bool {
        if self.draft_thread_id.as_deref() == Some(thread_id) {
            self.draft_thread_id = None;
            return true;
        }
        false
    }

    pub(in crate::app) fn promote_thread_from_draft(&mut self, thread_id: &str) -> bool {
        if !self.clear_draft_thread_if_matches(thread_id) {
            return false;
        }

        self.request_thread_start_if_needed();
        true
    }

    pub(in crate::app) fn resolve_existing_draft_thread_id(&mut self) -> Option<String> {
        let thread_id = self.draft_thread_id.clone()?;
        if self.thread_coordinators.contains_key(thread_id.as_str()) {
            return Some(thread_id);
        }
        self.draft_thread_id = None;
        None
    }

    pub(in crate::app) fn set_preferred_workspace_id(&mut self, workspace_id: Option<String>) {
        self.preferred_workspace_id = workspace_id;
    }

    pub(in crate::app) fn remember_active_thread_draft(&mut self, cx: &Context<Self>) {
        let Some(thread_id) = self.active_thread_id.as_ref().map(ToOwned::to_owned) else {
            return;
        };

        let value = self.composer_state.read(cx).value().trim_end().to_owned();
        let attachments = self.composer_attachments.clone();
        if value.is_empty() && attachments.is_empty() {
            self.thread_drafts.remove(thread_id.as_str());
            self.thread_draft_attachments.remove(thread_id.as_str());
        } else {
            self.thread_drafts.insert(thread_id.clone(), value);
            self.thread_draft_attachments.insert(thread_id, attachments);
        }
    }

    pub(in crate::app) fn clear_thread_draft(&mut self, thread_id: &str) {
        self.thread_drafts.remove(thread_id);
        self.thread_draft_attachments.remove(thread_id);
    }

    pub(in crate::app) fn restore_thread_draft(
        &mut self,
        thread_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let value = self
            .thread_drafts
            .get(thread_id)
            .cloned()
            .unwrap_or_default();
        let attachments = self
            .thread_draft_attachments
            .get(thread_id)
            .cloned()
            .unwrap_or_default();
        self.composer_state.update(cx, move |state, cx| {
            state.set_value(value.clone(), window, cx)
        });
        self.composer_attachments = attachments;
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
        self.composer_upload_in_progress = false;
        self.composer_upload_error = None;
    }

    pub(in crate::app) fn reset_thread_start_state(&mut self) {
        self.thread_start = ThreadStartCoordinator::default();
    }

    pub(in crate::app) fn thread_start_coordinator_mut(&mut self) -> &mut ThreadStartCoordinator {
        &mut self.thread_start
    }

    pub(in crate::app) fn enqueue_thread_start_request(&mut self) {
        self.thread_start_requested = true;
    }

    pub(in crate::app) fn dequeue_thread_start_request(&mut self) -> bool {
        if !self.thread_start_requested {
            return false;
        }
        self.thread_start_requested = false;
        true
    }

    pub(in crate::app) fn clear_thread_start_queue(&mut self) {
        self.thread_start_requested = false;
    }

    pub(in crate::app) fn enqueue_turn_resume_thread(&mut self, thread_id: String) {
        if self.ready_turn_resume_thread_set.insert(thread_id.clone()) {
            self.ready_turn_resume_threads.push_back(thread_id);
        }
    }

    pub(in crate::app) fn dequeue_turn_resume_thread(&mut self) -> Option<String> {
        let thread_id = self.ready_turn_resume_threads.pop_front()?;
        self.ready_turn_resume_thread_set.remove(thread_id.as_str());
        Some(thread_id)
    }

    pub(in crate::app) fn clear_turn_resume_queue(&mut self) {
        self.ready_turn_resume_threads.clear();
        self.ready_turn_resume_thread_set.clear();
    }

    pub(in crate::app) fn upsert_thread_coordinator(
        &mut self,
        thread_id: &str,
        workspace_id: &str,
    ) -> &mut ThreadCoordinator {
        let coordinator = self
            .thread_coordinators
            .entry(thread_id.to_owned())
            .or_insert_with(|| ThreadCoordinator::pending(thread_id, workspace_id));
        coordinator.set_workspace_id(workspace_id);
        coordinator
    }

    pub(in crate::app) fn upsert_thread_snapshot(
        &mut self,
        thread: Thread,
    ) -> &mut ThreadCoordinator {
        let thread_id = thread.id.clone();
        match self.thread_coordinators.entry(thread_id) {
            Entry::Occupied(mut occupied) => {
                occupied.get_mut().set_snapshot(thread);
                occupied.into_mut()
            }
            Entry::Vacant(vacant) => vacant.insert(ThreadCoordinator::new(thread)),
        }
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
        self.thread_list_refresh_requested = true;
    }

    pub(in crate::app) fn take_thread_list_refresh_request(&mut self) -> bool {
        if !self.thread_list_refresh_requested {
            return false;
        }
        self.thread_list_refresh_requested = false;
        true
    }

    pub(in crate::app) fn set_thread_history_loading(&mut self, thread_id: &str, loading: bool) {
        let Some(coordinator) = self.thread_coordinator_mut(thread_id) else {
            return;
        };
        coordinator.history_loading = loading;
    }

    pub(in crate::app) fn mark_thread_history_loaded(&mut self, thread_id: &str, loaded: bool) {
        let Some(coordinator) = self.thread_coordinator_mut(thread_id) else {
            return;
        };
        coordinator.history_loaded = loaded;
    }

    pub(in crate::app) fn remove_thread_conversation(&mut self, thread_id: &str) {
        self.clear_draft_thread_if_matches(thread_id);
        self.thread_coordinators.remove(thread_id);
        self.thread_drafts.remove(thread_id);
        self.thread_draft_attachments.remove(thread_id);
        self.thread_placements.remove(thread_id);
        self.turn_timeline_refresh
            .retain(|(refresh_thread_id, _), _| refresh_thread_id != thread_id);
    }

    pub(in crate::app) fn clear_thread_conversations(&mut self) {
        self.draft_thread_id = None;
        self.thread_coordinators.clear();
        self.thread_drafts.clear();
        self.thread_draft_attachments.clear();
        self.composer_attachments.clear();
        self.composer_upload_in_progress = false;
        self.composer_upload_error = None;
        self.composer_selected_provider = None;
        self.composer_selected_model = None;
        self.composer_model_selection_manually_selected = false;
        self.thread_folders.clear();
        self.thread_placements.clear();
        self.thread_agents_doc_summaries.clear();
        self.thread_folder_expanded.clear();
        self.thread_tree_selected_node_id = None;
        self.turn_timeline_refresh.clear();
    }

    pub(in crate::app) fn set_thread_tree_snapshot(
        &mut self,
        folders: Vec<ThreadFolder>,
        placements: Vec<ThreadPlacement>,
        agents_docs: Vec<ThreadAgentsDocSummary>,
    ) {
        self.thread_folders = folders
            .into_iter()
            .map(|folder| (folder.id.clone(), folder))
            .collect();

        let mut next_expanded = HashMap::new();
        for folder_id in self.thread_folders.keys() {
            let expanded = self
                .thread_folder_expanded
                .get(folder_id)
                .copied()
                .unwrap_or(false);
            next_expanded.insert(folder_id.clone(), expanded);
        }
        self.thread_folder_expanded = next_expanded;

        self.thread_placements = placements
            .into_iter()
            .map(|placement| (placement.thread_id.clone(), placement))
            .collect();
        self.thread_agents_doc_summaries = thread_agents_doc_summaries_by_scope(agents_docs);
    }

    pub(in crate::app) fn toggle_thread_folder_expanded(
        &mut self,
        folder_id: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(expanded) = self.thread_folder_expanded.get_mut(folder_id) {
            *expanded = !*expanded;
        } else {
            self.thread_folder_expanded
                .insert(folder_id.to_owned(), true);
        }

        if let Err(error) =
            state::set_thread_folders_expanded(cx, self.thread_folder_expanded.clone())
        {
            warn!(
                error = %format!("{error:#}"),
                "failed to save sidebar folder expansion state"
            );
        }
    }

    pub(in crate::app) fn set_thread_folder_expanded(
        &mut self,
        folder_id: &str,
        expanded: bool,
        cx: &mut Context<Self>,
    ) {
        self.thread_folder_expanded
            .insert(folder_id.to_owned(), expanded);

        if let Err(error) =
            state::set_thread_folders_expanded(cx, self.thread_folder_expanded.clone())
        {
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

pub(super) fn thread_agents_doc_summaries_by_scope(
    summaries: Vec<ThreadAgentsDocSummary>,
) -> HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary> {
    summaries
        .into_iter()
        .map(|summary| {
            (
                ThreadAgentsDocSummaryKey::from_folder_id(summary.folder_id.as_deref()),
                summary,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::ThreadAgentsDocStatus;

    fn summary(folder_id: Option<&str>, status: ThreadAgentsDocStatus) -> ThreadAgentsDocSummary {
        ThreadAgentsDocSummary {
            id: format!("agd_{}", folder_id.unwrap_or("root")),
            workspace_id: "ws_1".to_owned(),
            folder_id: folder_id.map(str::to_owned),
            status,
            content_sha256: "sha256:test".to_owned(),
            version: 1,
            char_count: 12,
            updated_at: 1_700_000_000,
        }
    }

    #[::core::prelude::v1::test]
    fn agents_doc_summary_key_handles_root_and_folder() {
        assert_eq!(
            ThreadAgentsDocSummaryKey::from_folder_id(None),
            ThreadAgentsDocSummaryKey::Root
        );
        assert_eq!(
            ThreadAgentsDocSummaryKey::from_folder_id(Some("fld_1")),
            ThreadAgentsDocSummaryKey::Folder("fld_1".to_owned())
        );
    }

    #[::core::prelude::v1::test]
    fn agents_doc_summaries_by_scope_stores_root_and_folder() {
        let summaries = thread_agents_doc_summaries_by_scope(vec![
            summary(None, ThreadAgentsDocStatus::Active),
            summary(Some("fld_1"), ThreadAgentsDocStatus::Draft),
        ]);

        assert_eq!(
            summaries
                .get(&ThreadAgentsDocSummaryKey::Root)
                .map(|summary| summary.status),
            Some(ThreadAgentsDocStatus::Active)
        );
        assert_eq!(
            summaries
                .get(&ThreadAgentsDocSummaryKey::Folder("fld_1".to_owned()))
                .map(|summary| summary.status),
            Some(ThreadAgentsDocStatus::Draft)
        );
    }
}
