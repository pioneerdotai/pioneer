use super::*;
use pioneer_client::agents_doc::scope as agents_doc_scope;
use pioneer_client::state::selectors as client_selectors;
use pioneer_client::state::snapshot::{ClientSnapshot, ClientSnapshotInput};
use pioneer_client::threads::tree as thread_tree;
use pioneer_client::workspaces::selectors as workspace_selectors;

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
        workspace_selectors::workspace_by_id(self.workspaces(), workspace_id)
    }

    pub(in crate::app) fn active_workspace_id(&self) -> Option<&str> {
        workspace_selectors::resolve_active_workspace_id(
            self.preferred_workspace_id(),
            self.workspaces(),
        )
    }

    pub(in crate::app) fn active_workspace(&self) -> Option<&Workspace> {
        let workspace_id = self.active_workspace_id()?;
        self.workspace_by_id(workspace_id)
    }

    pub(in crate::app) fn last_active_thread_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Option<&str> {
        thread_tree::remembered_thread_for_workspace(
            &self.last_active_thread_by_workspace,
            workspace_id,
        )
    }

    pub(in crate::app) fn draft_thread_for_workspace(&self, workspace_id: &str) -> Option<&str> {
        thread_tree::remembered_thread_for_workspace(&self.draft_thread_by_workspace, workspace_id)
    }

    pub(in crate::app) fn thread_start_coordinator(&self) -> &ThreadStartCoordinator {
        &self.thread_start
    }

    pub(in crate::app) fn thread_coordinator(&self, thread_id: &str) -> Option<&ThreadCoordinator> {
        client_selectors::thread_coordinator_from(&self.thread_coordinators, thread_id)
    }

    pub(in crate::app) fn thread_conversation(&self, thread_id: &str) -> Option<&Conversation> {
        client_selectors::thread_conversation_from(&self.thread_coordinators, thread_id)
    }

    pub(in crate::app) fn thread_workspace_id(&self, thread_id: &str) -> Option<&str> {
        client_selectors::thread_workspace_id_from(&self.thread_coordinators, thread_id)
    }

    pub(in crate::app) fn model_selector_workspace_id(&self) -> String {
        client_selectors::model_selector_workspace_id_from(
            self.active_workspace_id(),
            self.current_active_thread_id(),
            &self.thread_coordinators,
        )
    }

    pub(in crate::app) fn thread_folder(&self, folder_id: &str) -> Option<&ThreadFolder> {
        self.thread_folders.get(folder_id)
    }

    pub(in crate::app) fn thread_folders_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Vec<&ThreadFolder> {
        thread_tree::thread_folders_for_workspace(&self.thread_folders, workspace_id)
    }

    pub(in crate::app) fn thread_agents_doc_summary_for_workspace(
        &self,
        folder_id: Option<&str>,
        workspace_id: &str,
    ) -> Option<&ThreadAgentsDocSummary> {
        agents_doc_scope::thread_agents_doc_summary_for_workspace(
            &self.thread_agents_doc_summaries,
            folder_id,
            workspace_id,
        )
    }

    pub(in crate::app) fn thread_placements_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Vec<&ThreadPlacement> {
        thread_tree::thread_placements_for_workspace(&self.thread_placements, workspace_id)
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
        client_selectors::has_known_threads_for_workspace(&self.thread_coordinators, workspace_id)
    }

    pub(in crate::app) fn is_thread_history_loading(&self, thread_id: &str) -> bool {
        client_selectors::is_thread_history_loading(&self.thread_coordinators, thread_id)
    }

    pub(in crate::app) fn is_thread_history_loaded(&self, thread_id: &str) -> bool {
        client_selectors::is_thread_history_loaded(&self.thread_coordinators, thread_id)
    }

    pub(in crate::app) fn active_thread_conversation(&self) -> Option<&Conversation> {
        client_selectors::active_thread_conversation(
            self.current_active_thread_id(),
            &self.thread_coordinators,
        )
    }

    pub(in crate::app) fn has_any_in_flight_turn(&self) -> bool {
        client_selectors::has_any_in_flight_turn_in(&self.thread_coordinators)
    }

    pub(in crate::app) fn in_flight_turn_id_for_thread(&self, thread_id: &str) -> Option<String> {
        client_selectors::in_flight_turn_id_for_thread_in(&self.thread_coordinators, thread_id)
    }

    pub(in crate::app) fn client_snapshot(&self) -> ClientSnapshot {
        ClientSnapshot::from_parts(ClientSnapshotInput {
            active_thread_id: self.current_active_thread_id(),
            draft_thread_id: self.draft_thread_id(),
            preferred_workspace_id: self.preferred_workspace_id(),
            workspaces: self.workspaces(),
            workspaces_loading: self.workspaces_loading(),
            workspaces_error: self.workspaces_error(),
            workspace_action_in_progress: self.workspace_action_in_progress(),
            thread_list_loading: self.thread_list_loading,
            thread_start_in_progress: self.thread_start.in_progress,
            pending_thread_id: self.thread_start.pending_thread_id.as_deref(),
            coordinators: &self.thread_coordinators,
            gateway_connected: self.gateway.connection_state == GatewayConnectionState::Connected,
        })
    }
}

#[cfg(test)]
fn thread_folders_for_workspace_from<'a>(
    folders: &'a HashMap<String, ThreadFolder>,
    workspace_id: &str,
) -> Vec<&'a ThreadFolder> {
    thread_tree::thread_folders_for_workspace(folders, workspace_id)
}

#[cfg(test)]
fn thread_placements_for_workspace_from<'a>(
    placements: &'a HashMap<String, ThreadPlacement>,
    workspace_id: &str,
) -> Vec<&'a ThreadPlacement> {
    thread_tree::thread_placements_for_workspace(placements, workspace_id)
}

#[cfg(test)]
fn has_known_threads_for_workspace_in(
    coordinators: &HashMap<String, ThreadCoordinator>,
    workspace_id: &str,
) -> bool {
    client_selectors::has_known_threads_for_workspace(coordinators, workspace_id)
}

fn sorted_thread_ids_from_coordinators(
    coordinators: &HashMap<String, ThreadCoordinator>,
    draft_thread_id: Option<&str>,
    workspace_id: Option<&str>,
) -> Vec<String> {
    thread_tree::sorted_thread_ids_from_coordinators(coordinators, draft_thread_id, workspace_id)
}

#[cfg(test)]
pub(in crate::app) fn resolve_active_workspace_id<'a>(
    persisted_workspace_id: Option<&str>,
    workspaces: &'a [Workspace],
) -> Option<&'a str> {
    workspace_selectors::resolve_active_workspace_id(persisted_workspace_id, workspaces)
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
