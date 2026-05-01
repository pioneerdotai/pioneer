use super::*;

impl PioneerDesktop {
    pub(in crate::app) fn thread_tree_state(&self) -> &Entity<TreeState> {
        &self.thread_tree_state
    }

    pub(in crate::app) fn current_active_thread_id(&self) -> Option<&str> {
        self.active_thread_id.as_deref()
    }

    pub(in crate::app) fn draft_thread_id(&self) -> Option<&str> {
        self.draft_thread_id.as_deref()
    }

    pub(in crate::app) fn preferred_workspace_id(&self) -> Option<&str> {
        self.preferred_workspace_id.as_deref()
    }

    pub(in crate::app) fn thread_start_coordinator(&self) -> &ThreadStartCoordinator {
        &self.thread_start
    }

    pub(in crate::app) fn has_in_flight_thread_start(&self) -> bool {
        self.thread_start.in_progress || self.thread_start.pending_thread_id.is_some()
    }

    pub(in crate::app) fn thread_coordinator(&self, thread_id: &str) -> Option<&ThreadCoordinator> {
        self.thread_coordinators.get(thread_id)
    }

    pub(in crate::app) fn thread_conversation(&self, thread_id: &str) -> Option<&Conversation> {
        self.thread_coordinator(thread_id)
            .map(|coordinator| &coordinator.conversation)
    }

    pub(in crate::app) fn thread_workspace_id(&self, thread_id: &str) -> Option<&str> {
        self.thread_coordinator(thread_id)
            .map(|coordinator| coordinator.workspace_id.as_str())
    }

    pub(in crate::app) fn thread_folder(&self, folder_id: &str) -> Option<&ThreadFolder> {
        self.thread_folders.get(folder_id)
    }

    pub(in crate::app) fn thread_placement_folder_id(&self, thread_id: &str) -> Option<&str> {
        self.thread_placements
            .get(thread_id)
            .and_then(|placement| placement.folder_id.as_deref())
    }

    pub(in crate::app) fn is_thread_folder_expanded(&self, folder_id: &str) -> bool {
        self.thread_folder_expanded
            .get(folder_id)
            .copied()
            .unwrap_or(false)
    }

    pub(in crate::app) fn selected_thread_tree_node_id(&self) -> Option<&str> {
        self.thread_tree_selected_node_id.as_deref()
    }

    pub(in crate::app) fn sorted_thread_ids(&self) -> Vec<String> {
        let draft_thread_id = self.draft_thread_id();
        let mut thread_ids: Vec<String> = self
            .thread_coordinators
            .keys()
            .filter(|thread_id| Some(thread_id.as_str()) != draft_thread_id)
            .cloned()
            .collect();
        thread_ids.sort_by(|lhs, rhs| {
            let lhs_updated = self
                .thread_coordinators
                .get(lhs.as_str())
                .map(ThreadCoordinator::updated_at)
                .unwrap_or_default();
            let rhs_updated = self
                .thread_coordinators
                .get(rhs.as_str())
                .map(ThreadCoordinator::updated_at)
                .unwrap_or_default();
            rhs_updated.cmp(&lhs_updated).then_with(|| lhs.cmp(rhs))
        });
        thread_ids
    }

    pub(in crate::app) fn has_known_threads(&self) -> bool {
        !self.thread_coordinators.is_empty()
    }

    pub(in crate::app) fn is_thread_history_loading(&self, thread_id: &str) -> bool {
        self.thread_coordinator(thread_id)
            .is_some_and(|coordinator| coordinator.history_loading)
    }

    pub(in crate::app) fn is_thread_history_loaded(&self, thread_id: &str) -> bool {
        self.thread_coordinator(thread_id)
            .is_some_and(|coordinator| coordinator.history_loaded)
    }

    pub(in crate::app) fn active_thread_conversation(&self) -> Option<&Conversation> {
        self.current_active_thread_id()
            .and_then(|thread_id| self.thread_conversation(thread_id))
    }

    pub(in crate::app) fn has_any_in_flight_turn(&self) -> bool {
        self.thread_coordinators
            .values()
            .any(|coordinator| coordinator.conversation.in_flight_turn_id().is_some())
    }

    pub(in crate::app) fn in_flight_turn_id_for_thread(&self, thread_id: &str) -> Option<String> {
        self.thread_conversation(thread_id)
            .and_then(|conversation| conversation.in_flight_turn_id().map(str::to_owned))
    }
}
