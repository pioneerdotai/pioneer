use super::*;
use crate::state;
use pioneer_client::composer::draft::{
    ComposerDomainDraft, ComposerDraftLifecycleAction, composer_thread_switch_fallback,
    normalize_composer_draft_text, reduce_composer_draft_lifecycle,
};
use pioneer_client::composer::state_machine::ComposerDomainAction;
use pioneer_client::state::reducers as client_state_reducers;
use pioneer_client::threads::{session as thread_session, tree as thread_tree};
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn thread_semantic_mutation(
        &self,
        id: &str,
    ) -> pioneer_client::threads::registry::ThreadTimelineMutation<'_> {
        self.gateway
            .client_runtime
            .client_core()
            .existing_thread_timeline_mutation(id)
            .expect("known thread scope")
    }

    pub(in crate::app) fn reconcile_composer_draft_with_capabilities(&mut self) {
        let Some(policy) = self
            .gateway
            .capability_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.workspace.as_ref())
            .map(|workspace| workspace.execution_draft_policy.clone())
        else {
            // Projection invalidation is a temporary presentation fence while
            // the replacement snapshot is fetched. Keep both the user's draft
            // and the fingerprint captured for any submission already in
            // flight. A fresh semantic policy will replace the fingerprint
            // below; the Gateway remains authoritative for admission.
            return;
        };

        let mut skill_ids = Vec::new();
        let mut mcp_server_ids = Vec::new();
        for capability in &self.composer_capabilities {
            match &capability.kind {
                ComposerCapabilityKind::Skill { skill_id, .. } => {
                    skill_ids.push(skill_id.as_str().to_owned());
                }
                ComposerCapabilityKind::McpServer { name, .. } => {
                    mcp_server_ids.push(name.clone());
                }
                ComposerCapabilityKind::McpTool { server_name, .. } => {
                    mcp_server_ids.push(server_name.clone());
                }
            }
        }
        for selection in &self.composer_skill_selections {
            match selection {
                ComposerSkillSelection::Skill { skill_id, .. } => {
                    skill_ids.push(skill_id.as_str().to_owned());
                }
                ComposerSkillSelection::SkillPack { pack_id } => {
                    skill_ids.push(pack_id.as_str().to_owned());
                }
            }
        }
        skill_ids.sort();
        skill_ids.dedup();
        mcp_server_ids.sort();
        mcp_server_ids.dedup();

        let reconciliation = pioneer_client::composer::reconciliation::reconcile_execution_draft(
            &pioneer_client::composer::reconciliation::ExecutionDraftSelection {
                policy_fingerprint: self.composer_authorization_fingerprint.clone(),
                provider: self.composer_selected_provider.clone(),
                model: self.composer_selected_model.clone(),
                permission_mode: Some(self.composer_permission_mode),
                skill_ids,
                mcp_server_ids,
                has_attachments: !self.composer_attachments.is_empty(),
            },
            &policy,
        );
        let allowed_skills = reconciliation
            .draft
            .skill_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        let allowed_mcp = reconciliation
            .draft
            .mcp_server_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        self.composer_capabilities
            .retain(|capability| match &capability.kind {
                ComposerCapabilityKind::Skill { skill_id, .. } => {
                    allowed_skills.contains(skill_id.as_str())
                }
                ComposerCapabilityKind::McpServer { name, .. } => {
                    allowed_mcp.contains(name.as_str())
                }
                ComposerCapabilityKind::McpTool { server_name, .. } => {
                    allowed_mcp.contains(server_name.as_str())
                }
            });
        self.composer_skill_selections
            .retain(|selection| match selection {
                ComposerSkillSelection::Skill { skill_id, .. } => {
                    allowed_skills.contains(skill_id.as_str())
                }
                ComposerSkillSelection::SkillPack { pack_id } => {
                    allowed_skills.contains(pack_id.as_str())
                }
            });
        if !reconciliation.draft.has_attachments {
            self.composer_attachments.clear();
        }
        self.composer_selected_provider = reconciliation.draft.provider;
        self.composer_selected_model = reconciliation.draft.model;
        if self.composer_selected_provider.is_none() || self.composer_selected_model.is_none() {
            self.composer_selected_reasoning_effort = None;
        }
        self.composer_authorization_fingerprint = reconciliation.draft.policy_fingerprint;
        if let Some(mode) = reconciliation.draft.permission_mode
            && mode != self.composer_permission_mode
        {
            self.reduce_composer_domain(ComposerDomainAction::SetPermissionMode { mode });
        }
        if reconciliation.reasons.iter().any(|reason| {
            reason.kind
                != pioneer_client::composer::reconciliation::ExecutionDraftReconciliationKind::PolicyGeneration
        }) {
            self.composer_upload_error =
                Some("Composer selections were updated to match the current policy".into());
        }
    }

    pub(in crate::app) fn reconcile_composer_permission_mode_with_capabilities(&mut self) {
        self.reconcile_composer_draft_with_capabilities();
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
        self.gateway
            .client_runtime
            .client_core()
            .activate_thread(thread_id.as_deref(), self.active_workspace_id());
        self.thread_bindings.select(thread_id.as_deref());
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

    pub(in crate::app) fn set_draft_thread_id(&mut self, thread_id: Option<String>) {
        if let Some(workspace) = self.active_workspace_id() {
            self.gateway
                .client_runtime
                .client_core()
                .remember_thread_draft(workspace, thread_id);
        }
    }

    pub(in crate::app) fn clear_draft_thread_if_matches(&mut self, thread_id: &str) -> bool {
        self.gateway
            .client_runtime
            .client_core()
            .promote_thread(thread_id)
    }

    pub(in crate::app) fn resolve_existing_draft_thread_id(&mut self) -> Option<String> {
        self.draft_thread_id()
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
        self.gateway
            .client_runtime
            .client_core()
            .remember_thread_last_active(workspace_id, thread_id);
    }

    pub(in crate::app) fn remember_draft_thread_for_workspace(
        &mut self,
        workspace_id: &str,
        thread_id: Option<String>,
    ) {
        self.gateway
            .client_runtime
            .client_core()
            .remember_thread_draft(workspace_id, thread_id);
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
        self.composer_authorization_fingerprint = None;
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
        client_state_reducers::reset_thread_start_coordinator(
            &mut self
                .gateway
                .client_runtime
                .client_core()
                .thread_start_mutation(),
        );
        self.pending_thread_create_visibility = pioneer_protocol::ThreadVisibility::Private;
    }

    pub(in crate::app) fn thread_start_coordinator_mut(
        &self,
    ) -> pioneer_client::threads::registry::ThreadStartMutation<'_> {
        self.gateway
            .client_runtime
            .client_core()
            .thread_start_mutation()
    }

    pub(in crate::app) fn enqueue_thread_start_request(&mut self) {
        self.gateway
            .client_runtime
            .client_core()
            .enqueue_thread_start();
    }

    pub(in crate::app) fn dequeue_thread_start_request(&mut self) -> bool {
        self.gateway
            .client_runtime
            .client_core()
            .take_thread_start()
    }

    pub(in crate::app) fn clear_thread_start_queue(&mut self) {
        self.gateway
            .client_runtime
            .client_core()
            .clear_thread_start_request();
    }

    pub(in crate::app) fn enqueue_turn_resume_thread(&mut self, thread_id: String) {
        self.gateway
            .client_runtime
            .client_core()
            .enqueue_thread_resume(thread_id);
    }

    pub(in crate::app) fn dequeue_turn_resume_thread(&mut self) -> Option<String> {
        self.gateway
            .client_runtime
            .client_core()
            .take_thread_resume()
    }

    pub(in crate::app) fn clear_turn_resume_queue(&mut self) {
        self.gateway
            .client_runtime
            .client_core()
            .clear_thread_resume_queue();
    }

    pub(in crate::app) fn upsert_thread_coordinator(
        &self,
        thread_id: &str,
        workspace_id: &str,
    ) -> pioneer_client::threads::registry::ThreadMutation<'_> {
        self.gateway
            .client_runtime
            .client_core()
            .thread_mutation(thread_id, workspace_id)
            .expect("known thread scope")
    }

    pub(in crate::app) fn upsert_thread_snapshot(&mut self, thread: Thread) {
        let scope_changed = self
            .thread_coordinator(&thread.id)
            .is_some_and(|coordinator| {
                coordinator
                    .thread()
                    .is_some_and(|current| current.visibility != thread.visibility)
            });
        if scope_changed && self.current_active_thread_id() == Some(thread.id.as_str()) {
            self.thread_members_thread_id = None;
            self.invalidate_active_thread_capability_projection();
            self.thread_members.clear();
        }
        self.thread_bindings
            .track_summary(&thread.workspace_id, &thread.id);
        self.gateway
            .client_runtime
            .client_core()
            .upsert_thread(thread);
    }

    pub(in crate::app) fn thread_coordinator_mut(
        &self,
        thread_id: &str,
    ) -> Option<pioneer_client::threads::registry::ThreadMutation<'_>> {
        self.gateway
            .client_runtime
            .client_core()
            .existing_thread_mutation(thread_id)
    }

    pub(in crate::app) fn queue_thread_list_refresh(&mut self) {
        thread_tree::queue_thread_tree_refresh(&mut self.thread_list_refresh_requested);
    }

    pub(in crate::app) fn take_thread_list_refresh_request(&mut self) -> bool {
        thread_tree::take_thread_tree_refresh_request(&mut self.thread_list_refresh_requested)
    }

    pub(in crate::app) fn remove_thread_conversation(&mut self, thread_id: &str) {
        self.thread_unread.remove(thread_id);
        let workspace_id = self.thread_workspace_id(thread_id);
        self.thread_bindings.remove(thread_id);
        self.gateway
            .client_runtime
            .client_core()
            .remove_thread_store(thread_id);
        self.gateway
            .client_runtime
            .client_core()
            .promote_thread(thread_id);
        self.thread_placements.remove(thread_id);
        self.clear_thread_draft(thread_id);

        if let Some(workspace_id) = workspace_id {
            self.gateway.client_runtime.client_core().apply_pending_requests(
                pioneer_client::cli_runtime::approvals::reduce_pending_request_thread_closed_cleanup(
                    workspace_id,
                    thread_id.to_owned(),
                ),
            );
        }
    }

    pub(in crate::app) fn clear_thread_conversations(&mut self) {
        self.thread_unread.clear();
        self.message_revision_dialog = None;
        self.message_revision_loading = false;
        self.message_mutation_pending = false;
        self.composer_edit_target = None;
        self.thread_bindings.clear();
        self.gateway
            .client_runtime
            .client_core()
            .clear_thread_stores();
        self.thread_folders.clear();
        self.thread_placements.clear();
        self.thread_agents_doc_summaries.clear();
        self.thread_folder_expanded.clear();
        self.thread_tree_selected_node_id = None;
        self.composer_draft_lifecycle = reduce_composer_draft_lifecycle(
            &self.composer_draft_lifecycle,
            ComposerDraftLifecycleAction::ClearAll,
        )
        .state;
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
    }

    pub(in crate::app) fn clear_workspace_capability_projections(&mut self) {
        self.reset_thread_start_state();
        self.clear_thread_start_queue();
        self.clear_turn_resume_queue();
        self.providers.clear_for_workspace_switch();
        self.sync_open_model_selector_cli_runtime_snapshot();
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
        self.composer_authorization_fingerprint = None;
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
        self.task_thread_navigation_stack.clear();
        self.gateway
            .client_runtime
            .client_core()
            .clear_thread_resume_queue();
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
        self.gateway
            .client_runtime
            .client_core()
            .tick_thread_conversations();
        false
    }
}
