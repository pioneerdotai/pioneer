use super::*;
use crate::state;
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
        self.navigation_input = input;
        true
    }

    pub(crate) fn apply_navigation_publication(
        &mut self,
        input: std::sync::Arc<pioneer_client::navigation::ClientNavigationState>,
        cx: &mut Context<Self>,
    ) {
        let workspace_changed = self.navigation_input.workspace_id() != input.workspace_id();
        self.read_workspace_catalog_output();
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

    pub(in crate::app) fn clear_thread_draft(&mut self, thread_id: &str) {
        let core = self.gateway.client_runtime.client_core();
        if let Some(input) = core.composer_snapshot(thread_id) {
            core.composer_intent(pioneer_client::composer::store::ComposerIntent::Clear {
                thread_id: thread_id.to_owned(),
                draft_id: input.draft_id(),
            });
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
        }
        self.apply_navigation_publication(
            self.gateway
                .client_runtime
                .client_core()
                .navigation_snapshot(),
            cx,
        );
    }

    fn present_thread_draft(
        &mut self,
        thread_id: String,
        select: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if select {
            self.set_active_thread_id(Some(thread_id));
        }
        cx.notify();
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
        self.gateway
            .client_runtime
            .client_core()
            .clear_thread_stores();
        self.gateway
            .client_runtime
            .client_core()
            .clear_composer_drafts();
    }

    pub(in crate::app) fn clear_workspace_capability_projections(&mut self) {
        self.invalidate_workspace_capability_projections();
        self.navigation_intent(pioneer_client::navigation::NavigationIntent::SetMcpRoute {
            server_id: None,
        });
        self.navigation_intent(
            pioneer_client::navigation::NavigationIntent::SetSkillsRoute { skill_id: None },
        );
        if !matches!(
            self.main_content_view(),
            MainContentView::Settings | MainContentView::Threads
        ) {
            self.navigation_intent(pioneer_client::navigation::NavigationIntent::Navigate {
                destination: pioneer_client::navigation::SemanticDestination::Threads,
            });
        }
    }

    pub(in crate::app) fn invalidate_workspace_capability_projections(&mut self) {
        self.reset_thread_start_state();
        self.clear_thread_start_queue();
        self.clear_turn_resume_queue();
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

        self.active_agents_doc_editor_scope = None;
        self.agents_doc_editor = None;

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
