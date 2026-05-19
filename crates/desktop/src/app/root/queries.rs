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

    pub(in crate::app) fn workspaces(&self) -> &[Workspace] {
        self.workspaces.as_slice()
    }

    pub(in crate::app) fn workspaces_loading(&self) -> bool {
        self.workspaces_loading
    }

    pub(in crate::app) fn workspaces_error(&self) -> Option<&str> {
        self.workspaces_error.as_deref()
    }

    pub(in crate::app) fn workspace_action_in_progress(&self) -> bool {
        self.workspace_action_in_progress
    }

    pub(in crate::app) fn workspace_by_id(&self, workspace_id: &str) -> Option<&Workspace> {
        self.workspaces
            .iter()
            .find(|workspace| workspace.id == workspace_id)
    }

    pub(in crate::app) fn active_workspace_id(&self) -> Option<&str> {
        resolve_active_workspace_id(self.preferred_workspace_id(), self.workspaces())
    }

    pub(in crate::app) fn active_workspace(&self) -> Option<&Workspace> {
        let workspace_id = self.active_workspace_id()?;
        self.workspace_by_id(workspace_id)
    }

    pub(in crate::app) fn last_active_thread_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Option<&str> {
        self.last_active_thread_by_workspace
            .get(workspace_id)
            .map(String::as_str)
    }

    pub(in crate::app) fn draft_thread_for_workspace(&self, workspace_id: &str) -> Option<&str> {
        self.draft_thread_by_workspace
            .get(workspace_id)
            .map(String::as_str)
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

    pub(in crate::app) fn model_selector_workspace_id(&self) -> String {
        self.active_workspace_id()
            .or_else(|| {
                self.current_active_thread_id()
                    .and_then(|thread_id| self.thread_workspace_id(thread_id))
            })
            .map(str::to_owned)
            .unwrap_or_default()
    }

    pub(in crate::app) fn thread_folder(&self, folder_id: &str) -> Option<&ThreadFolder> {
        self.thread_folders.get(folder_id)
    }

    pub(in crate::app) fn thread_folders_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Vec<&ThreadFolder> {
        thread_folders_for_workspace_from(&self.thread_folders, workspace_id)
    }

    pub(in crate::app) fn thread_agents_doc_summary(
        &self,
        folder_id: Option<&str>,
    ) -> Option<&ThreadAgentsDocSummary> {
        self.thread_agents_doc_summaries
            .get(&ThreadAgentsDocSummaryKey::from_folder_id(folder_id))
    }

    pub(in crate::app) fn thread_agents_doc_summary_for_workspace(
        &self,
        folder_id: Option<&str>,
        workspace_id: &str,
    ) -> Option<&ThreadAgentsDocSummary> {
        self.thread_agents_doc_summary(folder_id)
            .filter(|summary| summary.workspace_id == workspace_id)
    }

    pub(in crate::app) fn thread_placements_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Vec<&ThreadPlacement> {
        thread_placements_for_workspace_from(&self.thread_placements, workspace_id)
    }

    pub(in crate::app) fn selected_thread_tree_node_id(&self) -> Option<&str> {
        self.thread_tree_selected_node_id.as_deref()
    }

    pub(in crate::app) fn sorted_thread_ids_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Vec<String> {
        sorted_thread_ids_from_coordinators(
            &self.thread_coordinators,
            self.draft_thread_id(),
            Some(workspace_id),
        )
    }

    pub(in crate::app) fn has_known_threads_for_workspace(&self, workspace_id: &str) -> bool {
        has_known_threads_for_workspace_in(&self.thread_coordinators, workspace_id)
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

fn thread_folders_for_workspace_from<'a>(
    folders: &'a HashMap<String, ThreadFolder>,
    workspace_id: &str,
) -> Vec<&'a ThreadFolder> {
    folders
        .values()
        .filter(|folder| folder.workspace_id == workspace_id)
        .collect()
}

fn thread_placements_for_workspace_from<'a>(
    placements: &'a HashMap<String, ThreadPlacement>,
    workspace_id: &str,
) -> Vec<&'a ThreadPlacement> {
    placements
        .values()
        .filter(|placement| placement.workspace_id == workspace_id)
        .collect()
}

fn has_known_threads_for_workspace_in(
    coordinators: &HashMap<String, ThreadCoordinator>,
    workspace_id: &str,
) -> bool {
    coordinators
        .values()
        .any(|coordinator| coordinator.workspace_id == workspace_id)
}

fn sorted_thread_ids_from_coordinators(
    coordinators: &HashMap<String, ThreadCoordinator>,
    draft_thread_id: Option<&str>,
    workspace_id: Option<&str>,
) -> Vec<String> {
    let mut thread_ids: Vec<String> = coordinators
        .iter()
        .filter(|(thread_id, coordinator)| {
            Some(thread_id.as_str()) != draft_thread_id
                && workspace_id.is_none_or(|workspace_id| coordinator.workspace_id == workspace_id)
        })
        .map(|(thread_id, _)| thread_id.clone())
        .collect();
    thread_ids.sort_by(|lhs, rhs| {
        let lhs_updated = coordinators
            .get(lhs.as_str())
            .map(ThreadCoordinator::updated_at)
            .unwrap_or_default();
        let rhs_updated = coordinators
            .get(rhs.as_str())
            .map(ThreadCoordinator::updated_at)
            .unwrap_or_default();
        rhs_updated.cmp(&lhs_updated).then_with(|| lhs.cmp(rhs))
    });
    thread_ids
}

pub(in crate::app) fn resolve_active_workspace_id<'a>(
    persisted_workspace_id: Option<&str>,
    workspaces: &'a [Workspace],
) -> Option<&'a str> {
    if let Some(workspace_id) = persisted_workspace_id.and_then(|workspace_id| {
        let trimmed = workspace_id.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    }) {
        if let Some(workspace) = workspaces
            .iter()
            .find(|workspace| workspace.is_active && workspace.id == workspace_id)
        {
            return Some(workspace.id.as_str());
        }
    }

    workspaces
        .iter()
        .find(|workspace| workspace.is_active && workspace.is_current)
        .or_else(|| workspaces.iter().find(|workspace| workspace.is_active))
        .map(|workspace| workspace.id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus};

    fn workspace(id: &str, is_active: bool, is_current: bool) -> Workspace {
        Workspace {
            id: id.to_owned(),
            name: format!("{id} workspace"),
            is_active,
            is_current,
            created_at: 1,
            updated_at: 2,
        }
    }

    fn thread(thread_id: &str, workspace_id: &str, updated_at: i64) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Chat,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            created_at: updated_at,
            updated_at,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        }
    }

    fn coordinator(thread_id: &str, workspace_id: &str, updated_at: i64) -> ThreadCoordinator {
        ThreadCoordinator::new(thread(thread_id, workspace_id, updated_at))
    }

    fn folder(folder_id: &str, workspace_id: &str) -> ThreadFolder {
        ThreadFolder {
            id: folder_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            parent_folder_id: None,
            name: folder_id.to_owned(),
            created_at: 1,
            updated_at: 1,
        }
    }

    fn placement(thread_id: &str, workspace_id: &str, folder_id: Option<&str>) -> ThreadPlacement {
        ThreadPlacement {
            thread_id: thread_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            folder_id: folder_id.map(str::to_owned),
        }
    }

    fn sorted_ids(items: impl IntoIterator<Item = String>) -> Vec<String> {
        let mut ids: Vec<String> = items.into_iter().collect();
        ids.sort();
        ids
    }

    #[::core::prelude::v1::test]
    fn resolve_active_workspace_prefers_valid_persisted_id() {
        let workspaces = vec![
            workspace("ws_1", true, true),
            workspace("ws_2", true, false),
        ];

        assert_eq!(
            resolve_active_workspace_id(Some("ws_2"), workspaces.as_slice()),
            Some("ws_2")
        );
    }

    #[::core::prelude::v1::test]
    fn resolve_active_workspace_ignores_invalid_persisted_id_and_uses_current() {
        let workspaces = vec![
            workspace("ws_1", true, false),
            workspace("ws_2", true, true),
        ];

        assert_eq!(
            resolve_active_workspace_id(Some("missing"), workspaces.as_slice()),
            Some("ws_2")
        );
    }

    #[::core::prelude::v1::test]
    fn resolve_active_workspace_ignores_inactive_persisted_id_and_uses_current() {
        let workspaces = vec![
            workspace("ws_1", false, true),
            workspace("ws_2", true, true),
        ];

        assert_eq!(
            resolve_active_workspace_id(Some("ws_1"), workspaces.as_slice()),
            Some("ws_2")
        );
    }

    #[::core::prelude::v1::test]
    fn resolve_active_workspace_uses_current_without_persisted_id() {
        let workspaces = vec![
            workspace("ws_1", true, false),
            workspace("ws_2", true, true),
        ];

        assert_eq!(
            resolve_active_workspace_id(None, workspaces.as_slice()),
            Some("ws_2")
        );
    }

    #[::core::prelude::v1::test]
    fn resolve_active_workspace_uses_first_active_when_no_current() {
        let workspaces = vec![
            workspace("ws_inactive", false, true),
            workspace("ws_1", true, false),
            workspace("ws_2", true, false),
        ];

        assert_eq!(
            resolve_active_workspace_id(None, workspaces.as_slice()),
            Some("ws_1")
        );
    }

    #[::core::prelude::v1::test]
    fn resolve_active_workspace_returns_none_for_empty_list() {
        assert_eq!(resolve_active_workspace_id(None, &[]), None);
    }

    #[::core::prelude::v1::test]
    fn workspace_filter_sorted_thread_ids_ignores_other_workspace_and_draft() {
        let coordinators = HashMap::from([
            (
                "thread_a_old".to_owned(),
                coordinator("thread_a_old", "ws_a", 10),
            ),
            (
                "thread_a_new".to_owned(),
                coordinator("thread_a_new", "ws_a", 30),
            ),
            (
                "thread_a_draft".to_owned(),
                coordinator("thread_a_draft", "ws_a", 40),
            ),
            (
                "thread_b_newer".to_owned(),
                coordinator("thread_b_newer", "ws_b", 100),
            ),
        ]);

        assert_eq!(
            sorted_thread_ids_from_coordinators(
                &coordinators,
                Some("thread_a_draft"),
                Some("ws_a")
            ),
            vec!["thread_a_new".to_owned(), "thread_a_old".to_owned()]
        );
    }

    #[::core::prelude::v1::test]
    fn workspace_filter_known_threads_checks_requested_workspace_only() {
        let coordinators =
            HashMap::from([("thread_b".to_owned(), coordinator("thread_b", "ws_b", 10))]);

        assert!(!has_known_threads_for_workspace_in(&coordinators, "ws_a"));
        assert!(has_known_threads_for_workspace_in(&coordinators, "ws_b"));
    }

    #[::core::prelude::v1::test]
    fn workspace_filter_folders_and_placements_ignore_other_workspaces() {
        let folders = HashMap::from([
            ("folder_a".to_owned(), folder("folder_a", "ws_a")),
            ("folder_b".to_owned(), folder("folder_b", "ws_b")),
            (
                "folder_a_child".to_owned(),
                folder("folder_a_child", "ws_a"),
            ),
        ]);
        let placements = HashMap::from([
            (
                "thread_a".to_owned(),
                placement("thread_a", "ws_a", Some("folder_a")),
            ),
            (
                "thread_b".to_owned(),
                placement("thread_b", "ws_b", Some("folder_b")),
            ),
        ]);

        assert_eq!(
            sorted_ids(
                thread_folders_for_workspace_from(&folders, "ws_a")
                    .into_iter()
                    .map(|folder| folder.id.clone())
            ),
            vec!["folder_a".to_owned(), "folder_a_child".to_owned()]
        );
        assert_eq!(
            sorted_ids(
                thread_placements_for_workspace_from(&placements, "ws_a")
                    .into_iter()
                    .map(|placement| placement.thread_id.clone())
            ),
            vec!["thread_a".to_owned()]
        );
    }
}
