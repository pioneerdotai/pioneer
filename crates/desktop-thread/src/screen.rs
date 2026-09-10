//! Retained presentation of the existing timeline inside one mounted thread.
use crate::{
    avatar::DesktopMemberAvatarState, binding::ThreadBindings, ports::*,
    timeline::TimelineAvatarActivities,
};
use gpui_kit::{prelude::*, *};
pub(crate) use pioneer_client::state::client_state::GatewayConnectionState;
use pioneer_client::{
    artifacts::store::ArtifactPublication,
    composer::{
        message_edit::ComposerMessageEditTarget, state_machine::ComposerDomainAction,
        store::ComposerPublication,
    },
    core::ClientCore,
    navigation::{ClientNavigationState, TaskThreadLineage},
    threads::{capabilities::ThreadCapabilityPublication, members::ThreadMemberPublication},
};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    sync::Arc,
};

pub(crate) enum ThreadScreenEvent {
    FocusComposer,
    ComposerDomain(ComposerDomainAction),
    EditMessage(pioneer_client::timeline::rows::UserMessagePresentation),
    CancelMessageEdit,
    MessageHistory {
        thread_id: String,
        turn_id: String,
    },
    OpenArtifact(String),
    OpenTaskThread {
        child_thread_id: String,
        title: String,
    },
    OpenMcpServer(String),
}

pub(crate) struct TimelineView {
    pub(crate) client: Arc<ClientCore>,
    pub(crate) thread_id: String,
    pub(crate) thread_bindings: Arc<ThreadBindings>,
    pub(crate) catalog_input:
        Option<Arc<pioneer_client::composer::catalog::ComposerCatalogPublication>>,
    pub(crate) composer_input: Option<Arc<ComposerPublication>>,
    pub(crate) thread_member_input: Option<Arc<ThreadMemberPublication>>,
    pub(crate) thread_capability_input: Option<Arc<ThreadCapabilityPublication>>,
    pub(crate) artifact_input: Option<Arc<ArtifactPublication>>,
    pub(crate) navigation_input: Arc<ClientNavigationState>,
    pub(crate) identity_input: Option<
        Arc<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>,
    >,
    pub(crate) connection_state: GatewayConnectionState,
    subscribed_workspace: Option<String>,
    workspace_input: Option<String>,
    pub(crate) avatar_http: Option<crate::avatar::ThreadAvatarClient>,
    pub(crate) member_avatar_state: DesktopMemberAvatarState,
    pub(crate) layout_store: crate::timeline::layout_store::TimelineLayoutStore,
    pub(crate) measurement_coordinator:
        crate::timeline::layout_store::TimelineMeasurementCoordinator,
    pub(crate) timeline_access_revoked: bool,
    pub(crate) retained_context: Option<(Pixels, u64, gpui_kit::TextStyle)>,
    pub(crate) row_registry: crate::timeline::row_registry::TimelineRowRegistry,
    pub(crate) thread_timeline_view_state: crate::timeline::state::TimelineViewState,
    pub(crate) avatar_activities: RefCell<TimelineAvatarActivities>,
    pub(crate) thread_timeline_terminal_item:
        RefCell<crate::timeline::terminal_registry::TerminalRegistry>,
    pub(crate) markdown_highlights:
        RefCell<HashMap<u64, crate::timeline::code_highlighting::LiveHighlight>>,
    pub(crate) pending_request_views:
        HashMap<(String, String), Entity<crate::approvals::PendingRequestView>>,
    pub(crate) task_review_views:
        HashMap<(String, String), Entity<crate::task_review::TaskReviewActionView>>,
    pub(crate) message_deletion_view: Option<Entity<crate::message_deletion::MessageDeletionView>>,
    pub(crate) files: Arc<dyn ThreadFilePort>,
    pub(crate) external: Arc<dyn ThreadExternalNavigationPort>,
    pub(crate) mount: u64,
    pub(crate) native_generation: Cell<u64>,
    _binding_task: Task<()>,
    _bounds: Subscription,
    _theme: Subscription,
    _activation: Subscription,
}
impl EventEmitter<ThreadScreenEvent> for TimelineView {}
impl Render for TimelineView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_timeline(window, cx)
    }
}
impl TimelineView {
    pub(crate) fn retire_timeline_rows(&mut self, cx: &mut Context<Self>) {
        self.set_row_activities_visible(&std::collections::HashSet::new(), cx);
        self.avatar_activities.borrow_mut().set_active(false, cx);
        crate::timeline::controller::DesktopTimelineController::exit(self);
        self.measurement_coordinator.cancel();
        self.retained_context = None;
        self.thread_timeline_view_state.prepared = None;
        self.thread_timeline_view_state.model = crate::timeline::TimelineRenderModel::empty();
        self.row_registry = Default::default();
        self.avatar_activities = Default::default();
        self.layout_store = Default::default();
        self.markdown_highlights.borrow_mut().clear();
        self.thread_timeline_terminal_item.borrow_mut().clear();
    }
    pub(crate) fn set_visible(
        &mut self,
        visible: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.thread_timeline_view_state.visible = visible;
        self.thread_bindings.set_active(visible);
        if visible {
            self.avatar_activities.borrow_mut().set_active(true, cx);
            self.synchronize_inputs(window, cx);
        } else {
            self.set_row_activities_visible(&std::collections::HashSet::new(), cx);
            crate::timeline::controller::DesktopTimelineController::exit(self);
            self.measurement_coordinator.cancel();
            self.retained_context = None;
            self.thread_timeline_view_state.prepared = None;
            self.markdown_highlights.borrow_mut().clear();
            self.thread_timeline_terminal_item.borrow_mut().clear();
            self.row_registry.clear_terminals();
            self.measurement_coordinator.draw = None;
            self.subscribed_workspace = None;
            self.pending_request_views.clear();
            self.task_review_views.clear();
            self.message_deletion_view = None;
            self.member_avatar_state.clear();
            self.avatar_activities.borrow_mut().set_active(false, cx);
        }
    }
    pub(crate) fn current_active_thread_id(&self) -> Option<&str> {
        Some(&self.thread_id)
    }
    pub(crate) fn active_task_thread_navigation(&self) -> Option<&TaskThreadLineage> {
        self.navigation_input
            .lineage()
            .last()
            .filter(|entry| entry.child_thread_id() == self.thread_id)
    }
    pub(crate) fn thread_workspace_id(&self, thread_id: &str) -> Option<String> {
        (thread_id == self.thread_id)
            .then(|| self.workspace_input.clone())
            .flatten()
    }
    pub(crate) fn thread_presentation_capabilities(
        &self,
        thread_id: &str,
    ) -> Option<pioneer_client::authorization::ThreadPresentationCapabilities> {
        let input = self
            .thread_capability_input
            .as_ref()
            .filter(|p| p.thread_id == thread_id)?;
        Some(
            pioneer_client::authorization::thread_presentation_capabilities(
                input
                    .snapshot
                    .as_ref()
                    .and_then(|s| s.thread.as_ref())
                    .map(|t| &t.capabilities),
            ),
        )
    }
    pub(crate) fn principal_presentation_capabilities(
        &self,
    ) -> pioneer_client::authorization::PrincipalPresentationCapabilities {
        self.identity_input
            .as_ref()
            .and_then(|identity| {
                identity
                    .capabilities
                    .snapshot(self.thread_workspace_id(&self.thread_id).as_deref(), None)
            })
            .as_ref()
            .map(pioneer_client::authorization::principal_presentation_capabilities)
            .unwrap_or_default()
    }
    pub(crate) fn active_artifact_presentation_policy(
        &self,
    ) -> pioneer_client::artifacts::presentation::ArtifactPresentationPolicy {
        let capabilities = self.thread_presentation_capabilities(&self.thread_id);
        pioneer_client::artifacts::presentation::artifact_presentation_policy(
            capabilities.is_some_and(|c| c.can_read_artifacts),
            capabilities.is_some_and(|c| c.can_write_artifacts && c.can_bind_artifacts),
            self.connection_state == GatewayConnectionState::Connected,
        )
    }
    pub(crate) fn composer_edit_target(&self) -> Option<&ComposerMessageEditTarget> {
        self.composer_input.as_ref().and_then(|p| p.message_edit())
    }
    pub(crate) fn composer_domain_intent(
        &mut self,
        action: ComposerDomainAction,
        cx: &mut Context<Self>,
    ) {
        cx.emit(ThreadScreenEvent::ComposerDomain(action));
    }
    pub(crate) fn start_composer_message_edit(
        &mut self,
        presentation: pioneer_client::timeline::rows::UserMessagePresentation,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        cx.emit(ThreadScreenEvent::EditMessage(presentation));
    }
    pub(crate) fn cancel_composer_message_edit(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        cx.emit(ThreadScreenEvent::CancelMessageEdit);
    }
    pub(crate) fn focus_composer(&self, cx: &mut Context<Self>) {
        cx.emit(ThreadScreenEvent::FocusComposer);
    }
    pub(crate) fn open_message_revision_history(
        &mut self,
        thread_id: String,
        turn_id: String,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if thread_id == self.thread_id {
            cx.emit(ThreadScreenEvent::MessageHistory { thread_id, turn_id });
        }
    }
    pub(crate) fn open_thread_artifact_in_sidebar(
        &mut self,
        artifact_id: String,
        cx: &mut Context<Self>,
    ) {
        if self.active_artifact_presentation_policy().can_open {
            cx.emit(ThreadScreenEvent::OpenArtifact(artifact_id));
        }
    }
    pub(crate) fn open_task_child_thread(
        &mut self,
        child_thread_id: String,
        title: String,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if child_thread_id != self.thread_id {
            cx.emit(ThreadScreenEvent::OpenTaskThread {
                child_thread_id,
                title,
            });
        }
    }
    pub(crate) fn open_mcp_server_details_from_timeline(
        &mut self,
        server_id: String,
        cx: &mut Context<Self>,
    ) {
        if self.principal_presentation_capabilities().can_use_mcp {
            let server_id = self
                .navigation_input
                .workspace_id()
                .and_then(|workspace| self.client.mcp_catalog_snapshot(workspace))
                .and_then(|catalog| {
                    catalog
                        .servers()
                        .iter()
                        .find(|server| server.id == server_id || server.name == server_id)
                        .map(|server| server.id.clone())
                })
                .unwrap_or(server_id);
            cx.emit(ThreadScreenEvent::OpenMcpServer(server_id));
        }
    }
    fn next_native_operation(&self) -> ThreadPresentationOperation {
        let generation = self
            .native_generation
            .get()
            .checked_add(1)
            .expect("thread presentation generation exhausted");
        self.native_generation.set(generation);
        ThreadPresentationOperation::new(self.thread_id.clone(), self.mount, generation)
    }
    pub(crate) fn active_thread_file_opener(&self, cx: &App) -> String {
        self.files
            .file_openers(&self.thread_id, cx)
            .selected()
            .id()
            .to_owned()
    }
    pub(crate) fn open_external_link(&self, url: &str) -> pioneer_client::ClientResult<()> {
        self.external
            .open_url(&ThreadExternalNavigationRequest::new(
                self.next_native_operation(),
                url.into(),
            ))
    }
    pub(crate) fn open_local_file(
        &self,
        opener: &str,
        target: &crate::file_opener::LocalFileTarget,
    ) -> pioneer_client::ClientResult<()> {
        self.files.open_file(&ThreadFileOpenRequest::new(
            self.next_native_operation(),
            opener.into(),
            pioneer_client::platform::ClientPath::new(target.path()),
            target.line(),
            target.column(),
        ))
    }
    pub(crate) fn request_thread_artifact_preview_load(
        &self,
        workspace_id: &str,
        artifact: &pioneer_client::artifacts::preview::ArtifactRef,
        _: &mut Context<Self>,
    ) {
        if self.active_artifact_presentation_policy().can_open
            && self.connection_state == GatewayConnectionState::Connected
        {
            self.client.observe_artifact_preview(
                &self.thread_id,
                workspace_id,
                artifact,
                self.files.runtime_root().into_path_buf(),
                self.files.preview_renderer(),
            );
        }
    }
    pub(crate) fn thread_artifact_preview_path(
        &self,
        artifact: &pioneer_client::artifacts::preview::ArtifactRef,
        detail: bool,
    ) -> Option<std::path::PathBuf> {
        let publication = self
            .artifact_input
            .as_ref()
            .filter(|p| p.thread_id == self.thread_id)?;
        let paths = publication
            .previews
            .iter()
            .find_map(|p| p.paths(artifact))?;
        let path = if detail {
            &paths.detail_path
        } else {
            &paths.square_path
        };
        path.is_file().then(|| path.clone())
    }
}

impl TimelineView {
    pub(crate) fn reconcile_pending_request_views(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            self.pending_request_views.clear();
            return;
        };
        let client = self.client.clone();
        let requests = client
            .thread_snapshot(&thread_id)
            .map(|p| {
                p.pending()
                    .iter()
                    .map(|request| request.request_id.clone())
                    .collect::<std::collections::HashSet<_>>()
            })
            .unwrap_or_default();
        self.pending_request_views
            .retain(|(thread, request), _| thread == &thread_id && requests.contains(request));
        for request_id in requests {
            let key = (thread_id.clone(), request_id.clone());
            if self.pending_request_views.contains_key(&key) {
                continue;
            }
            let view = crate::approvals::PendingRequestView::new(
                client.clone(),
                self.thread_bindings.registrar(),
                thread_id.clone(),
                request_id,
                window,
                cx,
            );
            self.pending_request_views.insert(key, view);
        }
    }
}

impl TimelineView {
    pub(crate) fn confirm_delete_message(
        &mut self,
        presentation: pioneer_client::timeline::rows::UserMessagePresentation,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use pioneer_client::threads::message_deletion::{
            MessageDeletionIntent, MessageDeletionState,
        };
        if self.thread_id != presentation.thread_id
            || self.client.composer_snapshot(&self.thread_id).is_some_and(|input| {
                input.operation().is_some_and(|operation| operation.pending()
                    && operation.kind == pioneer_client::composer::store::ComposerOperationKind::EditMessage)
            })
            || self
                .client
                .message_deletion_snapshot(&self.thread_id)
                .is_some_and(|p| p.state == MessageDeletionState::Pending)
        {
            return;
        }
        if self.composer_edit_target().is_some() {
            self.cancel_composer_message_edit(window, cx);
        }
        let result = self
            .client
            .message_deletion_intent(MessageDeletionIntent::Begin {
                thread_id: self.thread_id.clone(),
                turn_id: presentation.turn_id,
                expected_revision: presentation.revision,
            });
        if result.outcome() != pioneer_client::core::ClientTransitionOutcome::Changed {
            return;
        }
        if let Some(input) = self.client.message_deletion_snapshot(&self.thread_id) {
            self.message_deletion_view = Some(crate::message_deletion::MessageDeletionView::new(
                self.client.clone(),
                self.thread_bindings.registrar(),
                input.plan.clone(),
                window,
                cx,
            ));
        }
    }
}

impl TimelineView {
    pub(crate) fn new(
        client: Arc<ClientCore>,
        thread_id: String,
        thread_bindings: Arc<ThreadBindings>,
        files: Arc<dyn ThreadFilePort>,
        external: Arc<dyn ThreadExternalNavigationPort>,
        mount: u64,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx: &mut Context<Self>| {
            let mut changes = thread_bindings.watch();
            let binding = thread_bindings.clone();
            let binding_task = cx.spawn_in(window, async move |view, cx| {
                while changes.changed().await.is_ok() {
                    let changes = binding.drain();
                    if changes.is_empty() {
                        continue;
                    }
                    if view
                        .update_in(cx, |view, window, cx| {
                            if changes.iter().all(|publication| matches!(publication.scope(), pioneer_client::core::ClientScope::Timeline { .. })) {
                                if crate::timeline::controller::DesktopTimelineController::reconcile_publication(view, window, cx) { cx.notify(); }
                                return;
                            }
                            let only_composer_text = changes.iter().all(|publication| {
                                matches!(
                                    publication.scope(),
                                    pioneer_client::core::ClientScope::Composer { .. }
                                ) && publication.typed::<ComposerPublication>().is_some_and(
                                    |next| {
                                        next.payload().message_edit() == view.composer_edit_target()
                                    },
                                )
                            });
                            if only_composer_text {
                                view.composer_input =
                                    view.client.composer_snapshot(&view.thread_id);
                                return;
                            }
                            view.synchronize_inputs(window, cx);
                            cx.notify();
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let member_avatar_state =
                DesktopMemberAvatarState::new(client.clone(), thread_bindings.registrar(), cx);
            let mut view = Self {
                client,
                thread_id,
                thread_bindings,
                files,
                external,
                mount,
                catalog_input: None,
                composer_input: None,
                thread_member_input: None,
                thread_capability_input: None,
                artifact_input: None,
                identity_input: None,
                navigation_input: Arc::default(),
                connection_state: GatewayConnectionState::Disconnected,
                subscribed_workspace: None,
                workspace_input: None,
                avatar_http: None,
                member_avatar_state,
                thread_timeline_view_state: Default::default(),
                timeline_access_revoked: false,
                retained_context: None,
                row_registry: Default::default(),
                layout_store: Default::default(),
                measurement_coordinator: Default::default(),
                avatar_activities: RefCell::default(),
                thread_timeline_terminal_item: RefCell::default(),
                markdown_highlights: RefCell::default(),
                pending_request_views: HashMap::new(),
                task_review_views: HashMap::new(),
                message_deletion_view: None,
                native_generation: Cell::new(0),
                _binding_task: binding_task,
                _bounds: cx.observe_window_bounds(window, |view, window, cx| {
                    crate::timeline::controller::DesktopTimelineController::schedule(
                        view, window, cx,
                    )
                }),
                _activation: cx.observe_window_activation(window, |view, window, cx| {
                    if window.is_window_active() {
                        crate::timeline::controller::DesktopTimelineController::schedule(
                            view, window, cx,
                        );
                    } else {
                        crate::timeline::controller::DesktopTimelineController::exit(view);
                    }
                }),
                _theme: cx.observe_global_in::<gpui_kit::component::Theme>(
                    window,
                    |view, window, cx| {
                        view.layout_store.theme_revision += 1;
                        view.layout_store.context_changed();
                        crate::timeline::controller::DesktopTimelineController::reconcile(
                            view, window, cx,
                        );
                        cx.notify();
                    },
                ),
            };
            view.synchronize_inputs(window, cx);
            view
        })
    }
    fn synchronize_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use pioneer_client::core::ClientScope;
        let bindings = self.thread_bindings.clone();
        let payload = |scope: ClientScope| bindings.publication(&scope);
        self.composer_input = payload(ClientScope::Composer {
            thread_id: self.thread_id.clone(),
        })
        .and_then(|p| p.typed::<ComposerPublication>())
        .map(|p| p.payload());
        self.catalog_input = payload(ClientScope::ComposerCatalog {
            thread_id: self.thread_id.clone(),
        })
        .and_then(|p| p.typed::<pioneer_client::composer::catalog::ComposerCatalogPublication>())
        .map(|p| p.payload());
        self.artifact_input = payload(ClientScope::Artifact {
            thread_id: self.thread_id.clone(),
        })
        .and_then(|p| p.typed::<ArtifactPublication>())
        .map(|p| p.payload());
        self.thread_member_input = payload(ClientScope::ThreadMember {
            thread_id: self.thread_id.clone(),
        })
        .and_then(|p| p.typed::<ThreadMemberPublication>())
        .map(|p| p.payload());
        self.thread_capability_input = payload(ClientScope::ThreadCapability {
            thread_id: self.thread_id.clone(),
        })
        .and_then(|p| p.typed::<ThreadCapabilityPublication>())
        .map(|p| p.payload());
        self.navigation_input = payload(ClientScope::Navigation)
            .and_then(|p| p.typed::<ClientNavigationState>())
            .map_or_else(Arc::default, |p| p.payload());
        // A new child may have only navigation identity until subscription
        // loads its coordinator. Resolve its workspace from this same batch.
        self.workspace_input = self
            .client
            .thread_coordinator_snapshot(&self.thread_id)
            .map(|p| p.workspace_id.clone())
            .filter(|id| !id.is_empty())
            .or_else(|| self.navigation_input.workspace_id().map(str::to_owned));
        let identity = payload(ClientScope::Administration { workspace_id: None }).and_then(|p| p.typed::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>()).map(|p| p.payload());
        let previous_session = self
            .identity_input
            .as_ref()
            .map(|p| (p.endpoint_id.clone(), p.connection_generation));
        let next_session = identity
            .as_ref()
            .map(|p| (p.endpoint_id.clone(), p.connection_generation));
        let policy_changed = self
            .identity_input
            .as_ref()
            .map(|p| p.capabilities.accepted_revision())
            != identity
                .as_ref()
                .map(|p| p.capabilities.accepted_revision());
        let authorized = identity.as_ref().is_some_and(|p| p.current_auth.is_some());
        let lost_access = self
            .identity_input
            .as_ref()
            .is_some_and(|p| p.current_auth.is_some())
            && !authorized;
        if lost_access || (previous_session.is_some() && previous_session != next_session) {
            self.retire_timeline_rows(cx);
            self.timeline_access_revoked = true;
        }
        if authorized {
            self.timeline_access_revoked = false;
        }

        if previous_session != next_session || !authorized {
            self.member_avatar_state.clear();
            self.avatar_http = None;
        }
        self.identity_input = identity;
        let previous_connection = self.connection_state;
        self.connection_state = payload(ClientScope::Session)
            .and_then(|p| {
                p.typed::<pioneer_client::gateway::session_controller::GatewaySessionPublication>()
            })
            .and_then(|p| {
                p.payload()
                    .status
                    .as_ref()
                    .map(|status| status.connection_state)
            })
            .unwrap_or(GatewayConnectionState::Disconnected);
        if !authorized
            || self.connection_state != GatewayConnectionState::Connected
            || previous_session != next_session
            || policy_changed
        {
            self.subscribed_workspace = None;
        }
        // The new-thread surface uses the reserved creation identity immediately.
        // It is not yet a server thread to subscribe to or paginate.
        let awaiting_creation = self
            .client
            .thread_start_snapshot()
            .pending_thread_id
            .as_deref()
            == Some(self.thread_id.as_str());
        if authorized
            && !awaiting_creation
            && self.connection_state == GatewayConnectionState::Connected
        {
            if let Some(workspace) = self.thread_workspace_id(&self.thread_id) {
                if self.subscribed_workspace.as_ref() != Some(&workspace) {
                    // Record the mounted scope before scheduling, since the Client publishes
                    // the pending coordinator synchronously. Failure waits for an explicit
                    // connection/scope transition rather than retrying on publication.
                    self.subscribed_workspace = Some(workspace.clone());
                    self.client
                        .schedule_thread_subscription(&self.thread_id, &workspace);
                    self.client
                        .schedule_thread_cli_binding(&self.thread_id, &workspace);
                    if previous_session.is_some()
                        && previous_connection != GatewayConnectionState::Connected
                    {
                        self.reconcile_semantic_timeline_after_reconnect(cx);
                    } else {
                        self.request_semantic_thread_newest_page(self.thread_id.clone(), cx);
                    }
                }
            }
        }
        if authorized && self.avatar_http.is_none() {
            self.avatar_http = crate::avatar::ThreadAvatarClient::new(
                self.client.clone(),
                self.files.runtime_root().into_path_buf(),
            )
            .ok();
        }
        if !authorized {
            self.pending_request_views.clear();
            self.task_review_views.clear();
            self.message_deletion_view = None;
        } else {
            self.client.thread_capability_intent(
                pioneer_client::threads::capabilities::ThreadCapabilityIntent::Observe {
                    thread_id: self.thread_id.clone(),
                },
            );
            self.client.thread_member_intent(
                pioneer_client::threads::members::ThreadMemberIntent::Observe {
                    thread_id: self.thread_id.clone(),
                },
            );
            self.client.artifact_intent(
                pioneer_client::artifacts::store::ArtifactIntent::Observe {
                    thread_id: self.thread_id.clone(),
                },
            );
            if let Some(input) = &self.composer_input {
                self.client.composer_intent(
                    pioneer_client::composer::store::ComposerIntent::SyncModelSelection {
                        thread_id: self.thread_id.clone(),
                        draft_id: input.draft_id(),
                        reset: false,
                    },
                );
            }
            self.reconcile_pending_request_views(window, cx);
            self.reconcile_task_review_views(window, cx);
        }
        crate::timeline::controller::DesktopTimelineController::reconcile(self, window, cx);
    }
}
impl Drop for TimelineView {
    fn drop(&mut self) {
        crate::timeline::controller::DesktopTimelineController::exit(self);
        self.member_avatar_state.close();
        self.thread_bindings.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{ThreadBindings, TimelineView};
    use gpui_kit::{
        Context, Entity, Render, TestAppContext, Window, component::Root, div, prelude::*,
    };
    use pioneer_client::core::{ClientCore, ClientScope};
    use std::sync::Arc;
    struct Host(Option<Entity<TimelineView>>);
    impl Render for Host {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().size_full().children(self.0.clone())
        }
    }
    fn assert_new_child_workspace_on_first_publication(cx: &mut TestAppContext, initial: bool) {
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        crate::test_support::install_thread_timeline(&client, "parent", "Parent text");
        client.activate_thread(Some("parent"), Some("workspace"));
        client.navigate(
            pioneer_client::navigation::NavigationIntent::PushTaskThread {
                entry: pioneer_client::navigation::TaskThreadLineage::new(
                    "parent".into(),
                    "child".into(),
                    "workspace".into(),
                    "Task".into(),
                ),
            },
            None,
        );
        // A newly discovered child has navigation identity before its first
        // subscription response creates a retained domain snapshot.
        assert!(client.thread_coordinator_snapshot("child").is_none());
        let (registrar, deliver) = crate::test_support::binding_router(client.clone());
        let binding = ThreadBindings::new(registrar, "child", vec![]);
        if initial {
            deliver();
        }
        let (root, cx) = cx.add_window_view(|window, cx| {
            let screen = TimelineView::new(
                client.clone(),
                "child".into(),
                binding.clone(),
                Arc::new(crate::test_support::ThreadPorts),
                Arc::new(crate::test_support::ThreadPorts),
                1,
                window,
                cx,
            );
            Root::new(screen, window, cx)
        });
        if !initial {
            deliver();
            cx.run_until_parked();
        }
        let screen = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<TimelineView>().unwrap()
        });
        // No second publication, retry, reconnect or wall-clock wait may be
        // required to provide the workspace needed by subscription/bootstrap.
        screen.read_with(cx, |view, _| {
            assert_eq!(view.navigation_input.active_thread_id(), Some("child"));
            assert_eq!(
                view.thread_workspace_id("child").as_deref(),
                Some("workspace")
            );
        });
    }

    #[gpui_kit::test]
    fn new_child_resolves_workspace_from_the_first_navigation_publication(cx: &mut TestAppContext) {
        assert_new_child_workspace_on_first_publication(cx, true);
    }

    #[gpui_kit::test]
    fn new_child_resolves_workspace_when_navigation_arrives_after_mount(cx: &mut TestAppContext) {
        assert_new_child_workspace_on_first_publication(cx, false);
    }

    #[gpui_kit::test]
    fn mounted_screen_uses_client_rows_and_unmount_releases_the_stock_viewport(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        crate::test_support::install_thread_timeline(&client, "a", "A text");
        crate::test_support::install_thread_timeline(&client, "b", "B text");
        let (registrar, deliver) = crate::test_support::binding_router(client.clone());
        let binding = ThreadBindings::new(registrar, "a", vec![]);
        deliver();
        let (root, cx) = cx.add_window_view(|window, cx| {
            let screen = TimelineView::new(
                client.clone(),
                "a".into(),
                binding.clone(),
                Arc::new(crate::test_support::ThreadPorts),
                Arc::new(crate::test_support::ThreadPorts),
                1,
                window,
                cx,
            );
            let host = cx.new(|_| Host(Some(screen)));
            Root::new(host, window, cx)
        });
        cx.run_until_parked();
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<Host>().unwrap()
        });
        let screen = host.read_with(cx, |host, _| host.0.clone().unwrap());
        screen.read_with(cx, |view, _| {
            let model = view.thread_timeline_view_state.model.clone();
            assert_eq!(
                model
                    .item_presentations
                    .values()
                    .next()
                    .unwrap()
                    .content()
                    .unwrap()
                    .text,
                "A text"
            );
            assert!(view.thread_bindings.timeline_model(Some("b")).is_none());
        });
        let weak = screen.downgrade();
        drop(screen);
        host.update(cx, |host, cx| {
            host.0 = None;
            cx.notify();
        });
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
        deliver();
        assert!(
            binding
                .publication(&ClientScope::Timeline {
                    thread_id: "a".into()
                })
                .is_none()
        );
        assert!(binding.timeline_model(Some("a")).is_none());
    }
}
