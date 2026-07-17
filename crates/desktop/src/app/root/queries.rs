use super::*;
use pioneer_client::agents_doc::scope as agents_doc_scope;
#[cfg(test)]
use pioneer_client::composer::capabilities::composer_capability_target_for_provider as shared_composer_capability_target_for_provider;
use pioneer_client::composer::capabilities::{
    plan_composer_submission, ComposerCapabilityTarget, ComposerSubmissionPlan,
};
use pioneer_client::providers::list as provider_list;
use pioneer_client::state::selectors as client_selectors;
use pioneer_client::state::snapshot::{ClientSnapshot, ClientSnapshotInput};
use pioneer_client::threads::tree as thread_tree;
use pioneer_client::workspaces::selectors as workspace_selectors;
#[cfg(test)]
use pioneer_protocol::RuntimeSummary;

#[cfg(test)]
pub(crate) fn composer_capability_target_for_provider(
    provider: Option<&str>,
    runtimes: &[RuntimeSummary],
) -> ComposerCapabilityTarget {
    shared_composer_capability_target_for_provider(provider, runtimes)
}

pub(crate) fn composer_submission_plan_for_provider(
    provider: Option<&str>,
    text: &str,
    has_attachments: bool,
    capabilities: &[ComposerCapability],
) -> ComposerSubmissionPlan {
    plan_composer_submission(provider, text, has_attachments, capabilities)
}

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

    pub(in crate::app) fn active_task_thread_navigation(
        &self,
    ) -> Option<&TaskThreadNavigationEntry> {
        let active_thread_id = self.current_active_thread_id()?;
        self.task_thread_navigation_stack
            .last()
            .filter(|entry| entry.child_thread_id == active_thread_id)
    }

    pub(in crate::app) fn preferred_workspace_id(&self) -> Option<&str> {
        self.preferred_workspace_id.as_deref()
    }

    pub(in crate::app) fn workspaces(&self) -> &[Workspace] {
        self.workspaces.as_slice()
    }

    pub(in crate::app) fn active_workspaces(&self) -> Vec<&Workspace> {
        workspace_selectors::active_workspaces(self.workspaces())
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

    pub(in crate::app) fn thread_workspace_id(&self, thread_id: &str) -> Option<&str> {
        client_selectors::thread_workspace_id_from(&self.thread_coordinators, thread_id).or_else(
            || {
                self.task_thread_navigation_stack
                    .iter()
                    .rev()
                    .find(|entry| entry.child_thread_id == thread_id)
                    .map(|entry| entry.workspace_id.as_str())
            },
        )
    }

    pub(in crate::app) fn cli_runtime_binding_for_thread(
        &self,
        thread_id: &str,
    ) -> Option<&CLIRuntimeThreadBinding> {
        self.cli_runtime_thread_bindings.get(thread_id)
    }

    pub(in crate::app) fn composer_selected_provider_is_cli_runtime(&self) -> bool {
        self.composer_selected_provider
            .as_deref()
            .and_then(provider_list::runtime_id_from_cli_runtime_provider_key)
            .is_some()
    }

    pub(in crate::app) fn composer_capability_target(&self) -> ComposerCapabilityTarget {
        self.composer_capability_target
    }

    pub(in crate::app) fn effective_composer_capabilities(&self) -> Vec<ComposerCapability> {
        self.composer_submission_plan("", false).capabilities
    }

    pub(in crate::app) fn composer_submission_plan(
        &self,
        text: &str,
        has_attachments: bool,
    ) -> ComposerSubmissionPlan {
        composer_submission_plan_for_provider(
            self.composer_selected_provider.as_deref(),
            text,
            has_attachments,
            self.composer_capabilities.as_slice(),
        )
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
        let mut thread_ids = thread_tree::sorted_thread_ids_from_coordinators(
            &self.thread_coordinators,
            self.draft_thread_id(),
            Some(workspace_id),
        );
        thread_ids.retain(|thread_id| {
            !self
                .task_thread_navigation_stack
                .iter()
                .any(|entry| entry.child_thread_id == *thread_id)
        });
        thread_ids
    }

    pub(in crate::app) fn has_known_threads_for_workspace(&self, workspace_id: &str) -> bool {
        client_selectors::has_known_threads_for_workspace(&self.thread_coordinators, workspace_id)
    }

    pub(in crate::app) fn is_thread_timeline_loading(&self, thread_id: &str) -> bool {
        self.semantic_timelines
            .thread(thread_id)
            .is_some_and(|thread| {
                matches!(
                    thread.top_level.request_status,
                    pioneer_client::timeline::semantic::TimelineRequestStatus::Loading { .. }
                )
            })
    }

    pub(in crate::app) fn active_thread_conversation(&self) -> Option<&Conversation> {
        client_selectors::active_thread_conversation(
            self.current_active_thread_id(),
            &self.thread_coordinators,
        )
    }

    pub(in crate::app) fn active_thread_pending_requests(&self) -> Vec<PendingRequest> {
        self.pending_requests
            .pending_for_scope(self.active_workspace_id(), self.current_active_thread_id())
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
