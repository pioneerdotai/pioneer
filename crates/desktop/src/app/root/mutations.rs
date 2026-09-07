use super::*;
use crate::state;
use pioneer_client::composer::draft::{
    ComposerDomainDraft, ComposerDraftLifecycleAction, composer_thread_switch_fallback,
    normalize_composer_draft_text, reduce_composer_draft_lifecycle,
};
use pioneer_client::composer::state_machine::ComposerDomainAction;
use pioneer_client::state::reducers as client_state_reducers;
use pioneer_client::threads::tree as thread_tree;
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn navigation_intent(
        &mut self,
        intent: pioneer_client::navigation::NavigationIntent,
    ) {
        let core = self.gateway.client_runtime.client_core();
        core.navigate(intent, None);
        self.install_navigation_input(core.navigation_snapshot());
    }

    fn install_navigation_input(
        &mut self,
        input: std::sync::Arc<pioneer_client::navigation::ClientNavigationState>,
    ) -> bool {
        if std::sync::Arc::ptr_eq(&self.navigation_input, &input) {
            return false;
        }
        let changed = self.navigation_input.active_thread_id() != input.active_thread_id();
        self.thread_bindings.select(
            if matches!(
                input.destination(),
                pioneer_client::navigation::SemanticDestination::Threads
            ) {
                input.active_thread_id()
            } else {
                None
            },
        );
        self.navigation_input = input;
        if changed {
            self.composer_edit_target = None;
            self.invalidate_active_thread_capability_projection();
            self.thread_members_thread_id = None;
            self.thread_members.clear();
            self.thread_members_loading = false;
            self.active_thread_resubscribe_pending = self.current_active_thread_id().is_some()
                && self.gateway.connection_state == GatewayConnectionState::Connected;
            self.reset_composer_model_selection_for_active_thread();
        }
        true
    }

    pub(crate) fn apply_navigation_publication(
        &mut self,
        input: std::sync::Arc<pioneer_client::navigation::ClientNavigationState>,
        cx: &mut Context<Self>,
    ) {
        let workspace_changed = self.navigation_input.workspace_id() != input.workspace_id();
        self.read_workspace_catalog_output();
        if self.navigation_input.active_thread_id() != input.active_thread_id() {
            self.remember_active_thread_draft(cx);
        }
        let changed = self.install_navigation_input(input);
        if workspace_changed {
            if let Some(workspace) = self.navigation_input.workspace_id().map(str::to_owned) {
                self.persist_active_gateway_workspace_id(workspace);
            }
            self.refresh_workspace_bound_screens_after_switch(cx);
            self.refresh_current_principal(cx);
            self.refresh_configured_providers(cx);
            self.load_cli_provider_snapshot(cx);
        }

        self.reconcile_route_activity(cx);
        if changed {
            cx.notify();
        }
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
        use pioneer_client::navigation::{NavigationIntent, SemanticDestination};
        let destination = match view {
            MainContentView::Threads => SemanticDestination::Threads,
            MainContentView::AgentsDoc => SemanticDestination::AgentsDocument,
            MainContentView::Providers => SemanticDestination::Providers {
                filter: self.navigation_input.providers_route(),
            },
            MainContentView::Administration => SemanticDestination::Administration {
                route: self.navigation_input.administration_route(),
            },
            MainContentView::Settings => SemanticDestination::Settings {
                route: self.navigation_input.settings_route(),
            },
            MainContentView::Mcp => SemanticDestination::Mcp { server_id: None },
            MainContentView::McpDetails => SemanticDestination::Mcp {
                server_id: self.navigation_input.mcp_server_id().map(str::to_owned),
            },
            MainContentView::Skills => SemanticDestination::Skills { skill_id: None },
            MainContentView::SkillDetails => SemanticDestination::Skills {
                skill_id: self.navigation_input.skill_id().cloned(),
            },
        };
        self.navigation_intent(NavigationIntent::Navigate { destination });
        crate::client_runtime::DesktopRuntimeCoordinator::deliver_pending(cx);
        self.reconcile_route_activity(cx);
    }

    pub(in crate::app) fn set_active_thread_id(&mut self, thread_id: Option<String>) {
        let workspace_id = thread_id
            .as_deref()
            .and_then(|id| self.thread_workspace_id(id))
            .or_else(|| self.active_workspace_id().map(str::to_owned));
        self.navigation_intent(pioneer_client::navigation::NavigationIntent::SelectThread {
            workspace_id,
            thread_id,
        });
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
        self.navigation_intent(
            pioneer_client::navigation::NavigationIntent::SelectWorkspace { workspace_id },
        );
    }

    pub(in crate::app) fn read_workspace_catalog_output(&mut self) {
        self.workspace_catalog_input = self
            .gateway
            .client_runtime
            .client_core()
            .workspace_catalog();
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
        let Some(thread_id) = self.navigation_input.active_thread_id().map(str::to_owned) else {
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
        self.present_thread_draft(thread_id, true, window, cx);
    }

    pub(crate) fn present_workspace_agents_document(
        &mut self,
        scope: ThreadAgentsDocEditorScope,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_agents_doc_editor(scope, window, cx);
    }

    pub(crate) fn present_workspace_thread(
        &mut self,
        thread_id: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(thread_id) = thread_id {
            self.present_thread_draft(thread_id, false, window, cx);
        } else {
            self.remember_active_thread_draft(cx);
            self.clear_composer(window, cx);
        }
        self.apply_navigation_publication(
            self.gateway
                .client_runtime
                .client_core()
                .navigation_snapshot(),
            cx,
        );
        if let Some(thread_id) = self.current_active_thread_id().map(str::to_owned) {
            if let (Some(workspace_id), Some(connection_id)) = (
                self.active_workspace_id().map(str::to_owned),
                self.gateway.ws_connection_id,
            ) {
                self.ensure_thread_subscription(
                    thread_id.clone(),
                    workspace_id.clone(),
                    connection_id,
                    cx,
                );
                self.refresh_cli_runtime_thread_binding(
                    thread_id.clone(),
                    workspace_id,
                    connection_id,
                    cx,
                );
            }
            self.ensure_thread_semantic_timeline_loaded(&thread_id, cx);
        }
    }

    fn present_thread_draft(
        &mut self,
        thread_id: String,
        select: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current_thread_id = self.navigation_input.active_thread_id().map(str::to_owned);
        let current_draft = current_thread_id
            .as_ref()
            .filter(|id| select || *id != &thread_id)
            .and_then(|_| {
                self.composer_edit_target
                    .is_none()
                    .then(|| ComposerDomainDraft {
                        text: normalize_composer_draft_text(&self.composer_state.read(cx).value()),
                        domain: self.composer_domain_state(),
                    })
            });
        self.composer_edit_target = None;
        if select {
            self.set_active_thread_id(Some(thread_id.clone()));
        }
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
        if self.navigation_input.active_thread_id() == Some(thread_id) {
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

    pub(in crate::app) fn remove_thread_conversation(&mut self, thread_id: &str) {
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
        self.message_revision_dialog = None;
        self.message_revision_loading = false;
        self.message_mutation_pending = false;
        self.composer_edit_target = None;
        self.thread_bindings.clear();
        self.gateway
            .client_runtime
            .client_core()
            .clear_thread_stores();
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
        self.navigation_intent(pioneer_client::navigation::NavigationIntent::SetMcpRoute {
            server_id: None,
        });
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
        self.navigation_intent(
            pioneer_client::navigation::NavigationIntent::SetSkillsRoute { skill_id: None },
        );
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
            self.main_content_view(),
            MainContentView::Settings | MainContentView::Threads
        ) {
            self.navigation_intent(pioneer_client::navigation::NavigationIntent::Navigate {
                destination: pioneer_client::navigation::SemanticDestination::Threads,
            });
        }
    }

    /// Clears every server-authorized projection before a connection begins a
    /// new authorization epoch. Endpoint registry and device-session state are
    /// deliberately owned by the Gateway coordinator and remain untouched.
    pub(in crate::app) fn clear_authorization_epoch_cache(&mut self) {
        self.gateway.capability_snapshot = None;
        self.read_workspace_catalog_output();
        self.set_active_thread_id(None);
        self.clear_thread_conversations();
        self.navigation_intent(pioneer_client::navigation::NavigationIntent::ClearLineage);
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
        *self.thread_timeline_view_state.borrow_mut() = Default::default();
        self.thread_timeline_item_expanded.borrow_mut().clear();
        self.thread_timeline_terminal_item.borrow_mut().clear();
        *self.code_highlight_cache.borrow_mut() = Default::default();
        self.task_review_actions = Default::default();
        self.gateway.settings = None;
        self.clear_workspace_capability_projections();
    }

    pub(in crate::app) fn tick_thread_conversations(&mut self) -> bool {
        self.gateway
            .client_runtime
            .client_core()
            .tick_thread_conversations();
        false
    }
}
