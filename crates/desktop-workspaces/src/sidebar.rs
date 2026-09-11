pub(super) use crate::drag_preview::{SidebarTreeDragItem, SidebarTreeDragPayload};
use crate::{WorkspaceNavigationConfig, WorkspaceNavigationEvent, binding::Binding};
use gpui_kit::component::tree::TreeState;
use gpui_kit::{prelude::*, *};
pub(super) use pioneer_client::{
    agents_doc::scope::{
        AgentsDocEditorScope as ThreadAgentsDocEditorScope, ThreadAgentsDocSummaryKey,
    },
    threads::{coordinator::ThreadCoordinator, title::thread_display_title},
};
use pioneer_client::{
    core::{ClientCore, ClientScope},
    navigation::{ClientNavigationState, SemanticDestination},
    workspaces::{
        ThreadAgentsDocSummary, ThreadFolder, ThreadPlacement, Workspace,
        catalog::WorkspaceCatalogPublication, directory::ThreadTreePublication,
        intents::WorkspaceIntent,
    },
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{collections::HashMap, rc::Rc, sync::Arc};
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MainContentView {
    Threads,
    AgentsDoc,
    Other,
}
pub(super) struct ThreadSidebarView {
    pub client: Arc<ClientCore>,
    load_expansion: Rc<dyn Fn(&str, &mut App) -> HashMap<String, bool>>,
    save_expansion: Rc<dyn Fn(&str, HashMap<String, bool>, &mut App)>,
    bootstrap_connection: Option<u64>,
    bootstrap_authorization: Option<(u64, u64)>,
    pub rename_thread_dialog: crate::DialogPresenter,
    pub rename_folder_dialog: crate::DialogPresenter,
    pub rename_workspace_dialog: crate::DialogPresenter,
    pub create_workspace_dialog: crate::DialogPresenter,
    pub tree_structure: Vec<(String, usize, bool, bool)>,

    registrar: Arc<dyn ClientBindingRegistrar>,
    binding: Arc<Binding>,
    registrations: Vec<ClientBindingRegistration>,
    tree_registration: Option<ClientBindingRegistration>,
    composer_registration: Option<ClientBindingRegistration>,
    pub input: Option<Arc<ThreadTreePublication>>,
    pub catalog: Arc<WorkspaceCatalogPublication>,
    navigation: Arc<ClientNavigationState>,
    pub thread_tree_state: Entity<TreeState>,
    pub thread_folder_expanded: HashMap<String, bool>,
    selected_node: Option<String>,
    pub active_agents_doc_editor_scope: Option<ThreadAgentsDocEditorScope>,
    pub thread_scope_capabilities_thread_id: Option<String>,
    pub thread_scope_capabilities: pioneer_client::authorization::ThreadPresentationCapabilities,
    changes: Option<Task<()>>,
    tree_events: Option<Subscription>,
    pub context_locked: bool,
    presented_workspace: Option<String>,
    pending_new_thread: bool,
}
impl EventEmitter<WorkspaceNavigationEvent> for ThreadSidebarView {}
impl ThreadSidebarView {
    pub fn new(config: WorkspaceNavigationConfig, cx: &mut Context<Self>) -> Self {
        let binding = Arc::new(Binding::default());
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        let registrations = [
            ClientScope::Navigation,
            ClientScope::Session,
            ClientScope::GatewayDestinations,
            ClientScope::Administration { workspace_id: None },
            ClientScope::WorkspaceTree { workspace_id: None },
        ]
        .into_iter()
        .map(|scope| config.registrar.register(scope, Arc::downgrade(&sink)))
        .collect();
        let mut changes = binding.changed.subscribe();
        let task = cx.spawn(async move |view, cx| {
            while changes.changed().await.is_ok() {
                if view.update(cx, |view, cx| view.sync(cx)).is_err() {
                    return;
                }
            }
        });
        let navigation = config.client.navigation_snapshot();
        let catalog = config.client.workspace_catalog();
        let thread_tree_state = cx.new(|cx| TreeState::new(cx));
        let tree_events = cx.subscribe(
            &thread_tree_state,
            |view, _, event: &gpui_kit::component::tree::TreeEvent, cx| {
                use gpui_kit::component::tree::TreeEvent;
                let (id, expanded) = match event {
                    TreeEvent::Expanded(id) => (id, true),
                    TreeEvent::Collapsed(id) => (id, false),
                };
                if let pioneer_client::threads::tree::SidebarTreeNodeKey::Folder(folder) =
                    pioneer_client::threads::tree::parse_sidebar_tree_node_id(id.as_ref())
                {
                    view.thread_folder_expanded
                        .insert(folder.to_owned(), expanded);
                    if let Some((_, _, retained_expansion, _)) = view
                        .tree_structure
                        .iter_mut()
                        .find(|(node, _, _, _)| node == id.as_ref())
                    {
                        *retained_expansion = expanded;
                    }
                    if let Some(workspace) = view.active_workspace_id() {
                        (view.save_expansion)(workspace, view.thread_folder_expanded.clone(), cx);
                    }
                    cx.notify();
                }
            },
        );
        let mut view = Self {
            load_expansion: config.load_expansion,
            save_expansion: config.save_expansion,
            bootstrap_connection: None,
            bootstrap_authorization: None,
            rename_thread_dialog: config.rename_thread_dialog,
            rename_folder_dialog: config.rename_folder_dialog,
            rename_workspace_dialog: config.rename_workspace_dialog,
            create_workspace_dialog: config.create_workspace_dialog,
            tree_structure: vec![],
            client: config.client,
            registrar: config.registrar,
            binding,
            registrations,
            tree_registration: None,
            composer_registration: None,
            input: None,
            catalog,
            navigation,
            thread_tree_state,
            thread_folder_expanded: HashMap::new(),
            selected_node: None,
            active_agents_doc_editor_scope: None,
            thread_scope_capabilities_thread_id: None,
            thread_scope_capabilities: Default::default(),
            changes: Some(task),
            tree_events: Some(tree_events),
            context_locked: false,
            presented_workspace: None,
            pending_new_thread: false,
        };
        view.sync(cx);
        view
    }
    fn workspace_bootstrap_request(
        &self,
        session: &pioneer_client::gateway::session_controller::GatewaySessionPublication,
    ) -> Option<pioneer_client::workspaces::bootstrap::WorkspaceBootstrapRequest> {
        let registry = self.client.gateway_registry()?;
        let endpoint = session.startup.endpoint_id.as_deref()?;
        // Connection readiness is published before onboarding commits the active
        // registry entry. Wait for that commit so we use this gateway's preference.
        if registry.active_gateway_id() != Some(endpoint) {
            return None;
        }
        Some(
            pioneer_client::workspaces::bootstrap::WorkspaceBootstrapRequest {
                persisted_workspace_id: self
                    .client
                    .navigation_snapshot()
                    .workspace_id()
                    .or_else(|| registry.active_workspace_id())
                    .map(str::to_owned),
            },
        )
    }

    pub(super) fn sync(&mut self, cx: &mut Context<Self>) {
        let session = self.client.gateway_session();
        let authorization = self.client.authorization_permissions_epoch();
        let catalog = self.client.workspace_catalog();
        if session.startup.transport_ready
            && !session.startup.identity_pending
            && self.client.current_auth().is_some()
            && !catalog.is_loading()
            && (self.bootstrap_connection != session.startup.connection_id
                || (self.bootstrap_authorization != authorization
                    && catalog.workspaces().is_empty()))
            && let Some(request) = self.workspace_bootstrap_request(&session)
        {
            self.bootstrap_connection = session.startup.connection_id;
            self.bootstrap_authorization = authorization;
            self.client
                .request_workspace_bootstrap(request.persisted_workspace_id);
        }
        let navigation = self.client.navigation_snapshot();
        let workspace_changed = self.navigation.workspace_id() != navigation.workspace_id()
            || self.tree_registration.is_none();
        if workspace_changed {
            self.pending_new_thread = false;
            self.tree_registration.take();
            self.input = None;
            self.thread_folder_expanded = navigation
                .workspace_id()
                .map(|id| (self.load_expansion)(id, cx))
                .unwrap_or_default();
            self.selected_node = None;
            self.binding.publications.borrow_mut().retain(|scope, _| {
                !matches!(
                    scope,
                    ClientScope::WorkspaceTree {
                        workspace_id: Some(_)
                    }
                )
            });
            if let Some(workspace) = navigation.workspace_id() {
                let sink: Arc<dyn ClientPublicationSink> = self.binding.clone();
                self.tree_registration = Some(self.registrar.register(
                    ClientScope::WorkspaceTree {
                        workspace_id: Some(workspace.into()),
                    },
                    Arc::downgrade(&sink),
                ));
            }
        }
        if self.navigation.active_thread_id() != navigation.active_thread_id()
            || self.composer_registration.is_none()
        {
            self.composer_registration.take();
            self.binding
                .publications
                .borrow_mut()
                .retain(|scope, _| !matches!(scope, ClientScope::Composer { .. }));
            if let Some(thread) = navigation.active_thread_id() {
                let sink: Arc<dyn ClientPublicationSink> = self.binding.clone();
                self.composer_registration = Some(self.registrar.register(
                    ClientScope::Composer {
                        thread_id: thread.into(),
                    },
                    Arc::downgrade(&sink),
                ));
            }
        }
        if self.navigation.workspace_id() != navigation.workspace_id() {
            if let (Some(workspace), Some(endpoint)) = (
                navigation.workspace_id(),
                self.client.active_gateway_endpoint(),
            ) {
                self.client.onboarding_intent(
                    pioneer_client::gateway::onboarding_runtime::OnboardingIntent::SetWorkspace {
                        endpoint_id: endpoint.id,
                        workspace_id: Some(workspace.to_owned()),
                    },
                );
            }
        }
        self.active_agents_doc_editor_scope = navigation.agents_document_scope().cloned();
        if let Some(scope) = &self.active_agents_doc_editor_scope {
            self.selected_node =
                Some(pioneer_client::threads::tree::sidebar_agents_doc_node_id_for_scope(scope));
            if let Some(folder) = scope.folder_id() {
                self.thread_folder_expanded.insert(folder.to_owned(), true);
            }
        }
        self.navigation = navigation;
        self.catalog = self.client.workspace_catalog();
        self.input = self
            .navigation
            .workspace_id()
            .and_then(|workspace| self.client.workspace_tree(workspace));
        self.thread_scope_capabilities_thread_id =
            self.navigation.active_thread_id().map(str::to_owned);
        self.thread_scope_capabilities = self
            .client
            .authorization_snapshot(
                self.navigation.workspace_id(),
                self.navigation.active_thread_id(),
            )
            .map_or_else(Default::default, |snapshot| {
                pioneer_client::authorization::thread_presentation_capabilities(
                    snapshot.thread.as_ref().map(|thread| &thread.capabilities),
                )
            });
        self.context_locked = self.composer_context_locked();
        self.rebuild_sidebar_tree_state(cx);
        let workspace = self.navigation.workspace_id();
        let thread = self.navigation.active_thread_id();
        let pending_draft = self.pending_new_thread
            && workspace.is_some_and(|workspace| self.navigation.draft(workspace) == thread);
        if thread.is_some()
            && (self.presented_workspace.as_deref() != workspace || pending_draft)
            && self
                .input
                .as_ref()
                .is_some_and(|input| !input.is_loading() && input.error().is_none())
        {
            self.presented_workspace = workspace.map(str::to_owned);
            self.pending_new_thread = false;
            cx.emit(WorkspaceNavigationEvent::OpenThread {
                thread_id: thread.map(str::to_owned),
            });
        }
    }

    fn composer_context_locked(&self) -> bool {
        use pioneer_client::composer::store::ComposerOperationKind;
        self.navigation
            .active_thread_id()
            .and_then(|thread| self.client.composer_snapshot(thread))
            .and_then(|p| p.operation().cloned())
            .is_some_and(|operation| match operation.kind {
                ComposerOperationKind::Send | ComposerOperationKind::Voice => operation.pending(),
                _ => false,
            })
    }
    pub fn close(&mut self) {
        self.changes.take();
        self.tree_events.take();
        self.tree_registration.take();
        self.composer_registration.take();
        self.registrations.clear();
        self.input = None;
    }
    pub fn active_workspace_id(&self) -> Option<&str> {
        self.navigation.workspace_id()
    }
    pub fn current_active_thread_id(&self) -> Option<&str> {
        self.navigation.active_thread_id()
    }
    pub fn draft_thread_id(&self) -> Option<String> {
        self.active_workspace_id()
            .and_then(|workspace| self.navigation.draft(workspace))
            .map(str::to_owned)
    }
    pub fn active_task_thread_navigation(
        &self,
    ) -> Option<&pioneer_client::navigation::TaskThreadLineage> {
        self.navigation.lineage().last()
    }
    pub fn main_content_view(&self) -> MainContentView {
        match self.navigation.destination() {
            SemanticDestination::Threads => MainContentView::Threads,
            SemanticDestination::AgentsDocument => MainContentView::AgentsDoc,
            _ => MainContentView::Other,
        }
    }
    pub fn selected_thread_tree_node_id(&self) -> Option<&str> {
        self.selected_node.as_deref()
    }
    pub fn set_thread_tree_selected_node_id(&mut self, id: Option<String>) {
        self.selected_node = id;
    }
    pub fn thread_tree_state(&self) -> &Entity<TreeState> {
        &self.thread_tree_state
    }
    pub fn principal_presentation_capabilities(
        &self,
    ) -> pioneer_client::authorization::PrincipalPresentationCapabilities {
        self.client
            .authorization_snapshot(self.active_workspace_id(), None)
            .as_ref()
            .map(pioneer_client::authorization::principal_presentation_capabilities)
            .unwrap_or_default()
    }
    pub fn can_manage_thread_presentation(&self, id: &str) -> bool {
        self.principal_presentation_capabilities()
            .can_manage_all_threads
            || self
                .client
                .authorization_snapshot(self.active_workspace_id(), Some(id))
                .and_then(|snapshot| snapshot.thread)
                .is_some_and(|thread| thread.capabilities.can_manage)
    }
    pub fn thread_coordinator(&self, id: &str) -> Option<Arc<ThreadCoordinator>> {
        self.client.thread_coordinator_snapshot(id)
    }
    pub fn thread_folder(&self, id: &str) -> Option<&ThreadFolder> {
        self.input.as_ref()?.snapshot().folders_by_id.get(id)
    }
    pub fn thread_folders_for_workspace(&self, workspace: &str) -> Vec<&ThreadFolder> {
        self.input
            .as_ref()
            .filter(|p| p.snapshot().workspace_id == workspace)
            .map(|p| p.snapshot().folders_by_id.values().collect())
            .unwrap_or_default()
    }
    pub fn thread_placements_for_workspace(&self, workspace: &str) -> Vec<&ThreadPlacement> {
        self.input
            .as_ref()
            .filter(|p| p.snapshot().workspace_id == workspace)
            .map(|p| p.snapshot().placements_by_thread_id.values().collect())
            .unwrap_or_default()
    }
    pub fn sorted_thread_ids_for_workspace(&self, workspace: &str) -> Vec<String> {
        self.client.directory_thread_ids(workspace)
    }
    pub fn agents_doc_summaries(
        &self,
    ) -> HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary> {
        self.input
            .as_ref()
            .map(|p| {
                p.snapshot()
                    .agents_doc_summaries_by_folder_key
                    .values()
                    .map(|summary| {
                        (
                            summary
                                .folder_id
                                .as_ref()
                                .map_or(ThreadAgentsDocSummaryKey::Root, |folder| {
                                    ThreadAgentsDocSummaryKey::Folder(folder.clone())
                                }),
                            summary.clone(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn workspace_by_id(&self, id: &str) -> Option<&Workspace> {
        self.catalog
            .workspaces()
            .iter()
            .find(|workspace| workspace.id == id)
    }
    pub fn active_workspace(&self) -> Option<&Workspace> {
        self.workspace_by_id(self.active_workspace_id()?)
    }
    pub fn active_workspaces(&self) -> Vec<&Workspace> {
        self.catalog
            .workspaces()
            .iter()
            .filter(|w| w.is_active)
            .collect()
    }
    pub fn workspace_action_in_progress(&self) -> bool {
        self.catalog.is_action_pending()
    }
    pub fn workspaces_loading(&self) -> bool {
        self.catalog.is_loading()
    }
    pub fn workspaces_error(&self) -> Option<&str> {
        self.catalog.error()
    }
    fn command(&mut self, intent: WorkspaceIntent, _: &mut Context<Self>) -> bool {
        if self.context_locked
            && matches!(
                &intent,
                WorkspaceIntent::SelectWorkspace { .. }
                    | WorkspaceIntent::CreateWorkspace { .. }
                    | WorkspaceIntent::RenameWorkspace { .. }
            )
        {
            return false;
        }
        self.client
            .dispatch(pioneer_client::core::ClientIntent::Workspace { intent })
            .outcome()
            != pioneer_client::core::ClientTransitionOutcome::Rejected
    }
    pub fn open_thread_from_sidebar(
        &mut self,
        thread_id: String,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return;
        };
        if self.command(
            WorkspaceIntent::SelectThread {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
            },
            cx,
        ) {
            self.presented_workspace = Some(workspace_id);
            self.pending_new_thread = false;
            cx.emit(WorkspaceNavigationEvent::OpenThread {
                thread_id: Some(thread_id),
            });
        }
    }
    pub fn open_or_create_new_thread_from_sidebar(
        &mut self,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return;
        };
        if self.command(
            WorkspaceIntent::NewThread {
                workspace_id: workspace_id.clone(),
            },
            cx,
        ) {
            let thread = self
                .client
                .navigation_snapshot()
                .active_thread_id()
                .map(str::to_owned);
            self.presented_workspace = Some(workspace_id);
            self.pending_new_thread = thread.is_none();
            cx.emit(WorkspaceNavigationEvent::OpenThread {
                thread_id: self
                    .client
                    .navigation_snapshot()
                    .active_thread_id()
                    .map(str::to_owned),
            });
        }
    }
    pub fn open_agents_doc_editor(
        &mut self,
        scope: ThreadAgentsDocEditorScope,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.active_agents_doc_editor_scope = Some(scope.clone());
        cx.emit(WorkspaceNavigationEvent::OpenAgentsDocument { scope });
    }
    pub fn open_root_agents_doc_editor_from_sidebar(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) {
            self.open_agents_doc_editor(
                ThreadAgentsDocEditorScope::Root { workspace_id },
                window,
                cx,
            );
        }
    }
    pub fn open_folder_agents_doc_editor_from_sidebar(
        &mut self,
        folder_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(folder) = self.thread_folder(&folder_id) {
            self.open_agents_doc_editor(
                ThreadAgentsDocEditorScope::Folder {
                    workspace_id: folder.workspace_id.clone(),
                    folder_id,
                },
                window,
                cx,
            );
        }
    }
    pub fn delete_thread(&mut self, thread_id: &str, cx: &mut Context<Self>) {
        if let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) {
            self.command(
                WorkspaceIntent::DeleteThread {
                    workspace_id,
                    thread_id: thread_id.into(),
                },
                cx,
            );
        }
    }
    pub fn move_thread(
        &mut self,
        thread_id: &str,
        folder_id: Option<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) {
            self.command(
                WorkspaceIntent::MoveThread {
                    workspace_id,
                    thread_id: thread_id.into(),
                    folder_id,
                },
                cx,
            );
        }
    }
    pub fn rename_thread_from_sidebar(
        &mut self,
        thread_id: String,
        name: String,
        cx: &mut Context<Self>,
    ) {
        if let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) {
            self.command(
                WorkspaceIntent::RenameThread {
                    workspace_id,
                    thread_id,
                    name,
                },
                cx,
            );
        }
    }
    pub fn create_folder_from_sidebar(&mut self, cx: &mut Context<Self>) {
        if let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) {
            self.command(
                WorkspaceIntent::CreateFolder {
                    workspace_id,
                    name: t!("sidebar.folder.new").to_string(),
                },
                cx,
            );
        }
    }
    pub fn rename_folder_from_sidebar(
        &mut self,
        folder_id: String,
        name: String,
        cx: &mut Context<Self>,
    ) {
        if let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) {
            self.command(
                WorkspaceIntent::RenameFolder {
                    workspace_id,
                    folder_id,
                    name,
                },
                cx,
            );
        }
    }
    pub fn delete_folder_from_sidebar(&mut self, folder_id: String, cx: &mut Context<Self>) {
        if let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) {
            self.command(
                WorkspaceIntent::DeleteFolder {
                    workspace_id,
                    folder_id,
                },
                cx,
            );
        }
    }
    pub fn remove_folder_agents_doc_override_from_sidebar(
        &mut self,
        folder_id: String,
        cx: &mut Context<Self>,
    ) {
        self.remove_agents_doc(Some(folder_id), cx);
    }
    pub fn remove_root_agents_doc_override_from_sidebar(&mut self, cx: &mut Context<Self>) {
        self.remove_agents_doc(None, cx);
    }
    fn remove_agents_doc(&mut self, folder_id: Option<String>, cx: &mut Context<Self>) {
        if let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) {
            self.command(
                WorkspaceIntent::RemoveAgentsDocument {
                    workspace_id,
                    folder_id,
                },
                cx,
            );
        }
    }
    pub fn handle_sidebar_drop_to_folder(
        &mut self,
        payload: SidebarTreeDragPayload,
        folder: String,
        cx: &mut Context<Self>,
    ) {
        self.drop_to(payload, Some(folder), cx);
    }
    pub fn handle_sidebar_drop_to_root(
        &mut self,
        payload: SidebarTreeDragPayload,
        cx: &mut Context<Self>,
    ) {
        self.drop_to(payload, None, cx);
    }
    fn drop_to(
        &mut self,
        payload: SidebarTreeDragPayload,
        folder: Option<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) {
            let intent = match payload.item {
                SidebarTreeDragItem::Thread { thread_id } => WorkspaceIntent::MoveThread {
                    workspace_id,
                    thread_id,
                    folder_id: folder,
                },
                SidebarTreeDragItem::Folder { folder_id } => WorkspaceIntent::MoveFolder {
                    workspace_id,
                    folder_id,
                    parent_folder_id: folder,
                },
            };
            self.command(intent, cx);
        }
    }
    pub fn switch_workspace_from_popover(&mut self, workspace_id: String, cx: &mut Context<Self>) {
        self.command(WorkspaceIntent::SelectWorkspace { workspace_id }, cx);
    }
    pub fn rename_workspace_from_dialog(
        &mut self,
        workspace_id: String,
        name: String,
        cx: &mut Context<Self>,
    ) -> bool {
        self.command(WorkspaceIntent::RenameWorkspace { workspace_id, name }, cx)
    }
    pub fn create_workspace_from_dialog(&mut self, name: String, cx: &mut Context<Self>) -> bool {
        self.command(WorkspaceIntent::CreateWorkspace { name }, cx)
    }
}
impl Drop for ThreadSidebarView {
    fn drop(&mut self) {
        self.close();
    }
}
impl Render for ThreadSidebarView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_sidebar(cx)
    }
}

#[cfg(test)]
mod bootstrap_tests {
    use super::ThreadSidebarView;
    use gpui_kit::{AppContext, TestAppContext};
    use pioneer_client::core::{ClientCore, ClientScope};
    use pioneer_client::core::{
        ClientMutationAuthority, ClientRevisions, ContentRevision, DomainRevision,
        PresentationRevision, ScopedRevision,
    };
    use pioneer_client::gateway::{
        onboarding_effects::OnboardingEnvironment,
        registry::{GatewayRegistryConfig, default_registry},
        session_controller::GatewaySessionPublication,
        timings::*,
    };
    use pioneer_desktop_foundation::{
        ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
    };
    use std::cell::RefCell;
    use std::{collections::HashMap, sync::Arc};

    #[derive(Default)]
    struct Relay(RefCell<HashMap<ClientScope, std::sync::Weak<dyn ClientPublicationSink>>>);
    impl ClientBindingRegistrar for Relay {
        fn register(
            &self,
            scope: ClientScope,
            sink: std::sync::Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            self.0.borrow_mut().insert(scope, sink);
            ClientBindingRegistration::new(|| {})
        }
    }
    impl Relay {
        fn deliver(&self, core: &ClientCore, scope: ClientScope) {
            if let Some(sink) = self
                .0
                .borrow()
                .get(&scope)
                .and_then(std::sync::Weak::upgrade)
            {
                sink.publish(core.snapshot(&scope).unwrap());
            }
        }
    }

    #[gpui_kit::test]
    fn gateway_workspace_bootstrap_waits_for_registry_commit_and_restores_each_preference(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let core = pioneer_client::catalog_test_support::settings_client();
        let relay = Arc::new(Relay::default());
        let mut registry = default_registry(&GatewayRegistryConfig { local: None });
        registry.installation_id = Some("synthetic".into());
        registry.active_gateway_id = Some("a".into());
        registry.remotes = ["a", "b"]
            .map(|id| {
                serde_json::from_value(serde_json::json!({
                    "id":id,"name":id,"kind":"remote","gateway_base_url":"https://gateway.invalid",
                    "server_gateway_id":"G00000000000000000001","session_ref":"synthetic-reference",
                    "workspace_id":format!("workspace-{id}"),"service_name":null
                }))
                .unwrap()
            })
            .into();
        let install = |registry| {
            core.install_onboarding_environment_for_test(OnboardingEnvironment {
            registry, binding_journals: vec![], discard_unbound_remote_candidates:false,
            default_remote_name:"Remote".into(), remote_connect_timeout_min:std::time::Duration::ZERO,
            installation:serde_json::from_value(serde_json::json!({"installation_id":"synthetic","display_name":"Test","client_kind":"desktop","platform":null,"client_version":null})).unwrap(),
            timings:GatewayTimings::from_millis(10,10,10).unwrap(),
            ws_timings:GatewayWsTimings::from_millis(10,10,10,10,20,0).unwrap(),
            local_provisioned:false,local_install_required:false,local_update_required:false,
        })
        };
        install(registry.clone());
        let mut config = crate::context_tests::config(core.clone(), Default::default());
        config.registrar = relay.clone();
        let sidebar = cx.new(|cx| ThreadSidebarView::new(config, cx));
        let mut previous_connection = None;
        for (connection, endpoint) in [(1, "a"), (2, "b"), (3, "a"), (4, "b")] {
            // Auth replacement has cleared navigation, but the selected registry
            // entry still refers to the old gateway until its durable write finishes.
            core.activate_thread(None, None);
            let mut session = GatewaySessionPublication::default();
            session.startup.connection_id = Some(connection);
            session.startup.endpoint_id = Some(endpoint.into());
            session.startup.transport_ready = true;
            core.publish(
                &ClientMutationAuthority::for_test(),
                ClientScope::Session,
                ClientRevisions::new(
                    DomainRevision::new(connection),
                    PresentationRevision::new(connection),
                    ContentRevision::new(connection),
                    ScopedRevision::new(connection),
                ),
                Arc::new(session),
                vec![],
            );
            relay.deliver(&core, ClientScope::Session);
            cx.run_until_parked();
            if registry.active_gateway_id.as_deref() != Some(endpoint) {
                sidebar.read_with(cx, |sidebar, _| {
                    assert_eq!(
                        sidebar.bootstrap_connection, previous_connection,
                        "must wait for the selected gateway commit"
                    );
                    assert!(
                        sidebar
                            .workspace_bootstrap_request(&core.gateway_session())
                            .is_none()
                    );
                });
            }
            registry.active_gateway_id = Some(endpoint.into());
            install(registry.clone());
            relay.deliver(&core, ClientScope::GatewayDestinations);
            cx.run_until_parked();
            sidebar.read_with(cx, |sidebar, _| {
                assert_eq!(sidebar.bootstrap_connection, Some(connection));
                let request = sidebar
                    .workspace_bootstrap_request(&core.gateway_session())
                    .unwrap();
                assert_eq!(
                    request.persisted_workspace_id,
                    Some(format!("workspace-{endpoint}"))
                );
            });
            previous_connection = Some(connection);
        }
        // A transport reconnect of the same gateway keeps an in-session selection.
        core.activate_thread(None, Some("workspace-chosen"));
        sidebar.read_with(cx, |sidebar, _| {
            assert_eq!(
                sidebar
                    .workspace_bootstrap_request(&core.gateway_session())
                    .unwrap()
                    .persisted_workspace_id
                    .as_deref(),
                Some("workspace-chosen")
            );
        });
        core.shutdown();
    }
}
