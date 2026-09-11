use crate::{
    activity::LoadingIndicator, avatar::AdministrationAvatars, binding::AdministrationBinding,
    input::AdministrationInput, ports::*, sidebar::AdministrationSidebar,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    administration::{pages::*, types::*},
    authorization::{PrincipalPresentationCapabilities, principal_presentation_capabilities},
    core::{ClientCore, ClientScope},
    navigation::{AdministrationRoute, NavigationIntent, SemanticDestination},
    state::client_state::GatewayConnectionState,
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub struct AdministrationConfig {
    client: Arc<ClientCore>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    avatars: Arc<dyn AdministrationAvatarPort>,
    activation: Arc<dyn AdministrationActivationPort>,
    external: Arc<dyn AdministrationExternalNavigationPort>,
}
impl AdministrationConfig {
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        avatars: Arc<dyn AdministrationAvatarPort>,
        activation: Arc<dyn AdministrationActivationPort>,
        external: Arc<dyn AdministrationExternalNavigationPort>,
    ) -> Self {
        Self {
            client,
            registrar,
            avatars,
            activation,
            external,
        }
    }
}
pub(crate) struct GatewayInput {
    pub(crate) current_auth: Option<AuthMeResponse>,
    pub(crate) connection_state: GatewayConnectionState,
    pub(crate) capabilities: Option<AuthorizationCapabilitySnapshot>,
    permissions_epoch: Option<(u64, u64)>,
}
impl GatewayInput {
    fn read(client: &ClientCore) -> Self {
        let identity = client.snapshot(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| p.typed::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>())
            .map(|p| p.payload());
        let capabilities = identity
            .as_ref()
            .and_then(|p| p.capabilities.snapshot(None, None));
        Self {
            current_auth: identity.as_ref().and_then(|p| p.current_auth.clone()),
            permissions_epoch: identity
                .as_ref()
                .filter(|_| capabilities.is_some())
                .map(|p| (p.connection_generation, p.authorization_change_sequence)),
            capabilities,
            connection_state: client
                .gateway_session()
                .status
                .as_ref()
                .map_or(GatewayConnectionState::Disconnected, |status| {
                    status.connection_state
                }),
        }
    }
}
static NEXT_MOUNT: AtomicU64 = AtomicU64::new(1);
pub struct AdministrationView {
    pub(crate) client: Arc<ClientCore>,
    pub(crate) administration: AdministrationInput,
    pub(crate) gateway: GatewayInput,
    workspaces: Arc<pioneer_client::workspaces::catalog::WorkspaceCatalogPublication>,
    navigation: Arc<pioneer_client::navigation::ClientNavigationState>,
    binding: Arc<AdministrationBinding>,
    demanded: BTreeSet<AdministrationPage>,
    sidebar: Entity<AdministrationSidebar>,
    pub(crate) member_avatar_state: AdministrationAvatars,
    pub(crate) member_loading: Entity<LoadingIndicator>,
    pub(crate) invitation_loading: Entity<LoadingIndicator>,
    pub(crate) activation: Arc<dyn AdministrationActivationPort>,
    pub(crate) _external: Arc<dyn AdministrationExternalNavigationPort>,
    pub(crate) dialogs: Vec<Entity<crate::dialog_lifetime::DialogLifetime>>,
    _release: Subscription,
    pub(crate) mount: u64,
    pub(crate) operations: Vec<Task<()>>,
    _publication_task: Task<()>,
    window_active: bool,
}
impl AdministrationView {
    pub fn new(config: AdministrationConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        let mount = NEXT_MOUNT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .expect("administration mount exhausted");
        cx.new(|cx| {
            let binding = AdministrationBinding::new(config.registrar);
            let mut changed = binding.changed.subscribe();
            let window_handle = window.window_handle();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if window_handle
                        .update(cx, |_, window, cx| {
                            view.update(cx, |view, cx| view.sync_publications(window, cx))
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let sidebar = AdministrationSidebar::new(cx.weak_entity(), mount, cx);
            let client = config.client;
            let release = cx.on_release_in(window, |view: &mut Self, window, cx| {
                for dialog in &view.dialogs {
                    dialog.update(cx, |dialog, cx| dialog.invalidate(window, cx));
                }
            });
            let mut view = Self {
                dialogs: Vec::new(),
                _release: release,
                administration: AdministrationInput::read(&client),
                gateway: GatewayInput::read(&client),
                navigation: client.navigation_snapshot(),
                workspaces: client.workspace_catalog(),
                client,
                binding: binding.clone(),
                demanded: BTreeSet::new(),
                sidebar,
                member_avatar_state: AdministrationAvatars::new(config.avatars, binding),
                member_loading: LoadingIndicator::new(cx),
                invitation_loading: LoadingIndicator::new(cx),
                activation: config.activation,
                _external: config.external,
                mount,
                operations: Vec::new(),
                _publication_task: task,
                window_active: window.is_window_active(),
            };
            view.sync_publications(window, cx);
            view
        })
    }
    pub fn sidebar(&self) -> AnyView {
        self.sidebar.clone().into()
    }
    pub fn set_window_active(&mut self, active: bool, cx: &mut Context<Self>) {
        if self.window_active != active {
            self.window_active = active;
            self.sync_loading(cx);
        }
    }
    pub(crate) fn copy_activation(&self, generation: u64, value: &str, cx: &mut App) -> bool {
        use pioneer_client::{
            administration::operations::AdministrationPresentationIntent, core::*,
        };
        let transition = self.client.administration_presentation_intent(
            AdministrationPresentationIntent::CopyActivation { generation },
        );
        let Some(plan) = transition.effects().first() else {
            return false;
        };
        let identity = AdministrationEffectIdentity::from_plan(plan);
        let completion = self.activation.copy(identity.clone(), value, cx);
        if completion.identity() != &identity {
            return false;
        }
        self.client.complete_effect(ClientEffectCompletion::new(
            plan.operation_id().clone(),
            plan.generation(),
            if completion.succeeded() {
                ClientEffectResult::Completed
            } else {
                ClientEffectResult::Failed {
                    code: "clipboard_unavailable".into(),
                }
            },
        ));
        completion.succeeded()
    }
    pub(crate) fn ui_id(&self, list: &str, domain: &str, role: &str) -> SharedString {
        let endpoint = self.client.snapshot(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| p.snapshot().payload::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>())
            .and_then(|p| p.endpoint_id.clone()).unwrap_or_default();
        format!(
            "administration:{}:{endpoint}:{list}:{domain}:{role}",
            self.mount
        )
        .into()
    }
    pub(crate) fn workspaces(&self) -> &[Workspace] {
        self.workspaces.workspaces()
    }
    pub(crate) fn administration_content_view(&self) -> AdministrationRoute {
        self.navigation.administration_route()
    }
    pub(crate) fn principal_presentation_capabilities(&self) -> PrincipalPresentationCapabilities {
        self.gateway
            .capabilities
            .as_ref()
            .map(principal_presentation_capabilities)
            .unwrap_or_default()
    }
    pub(crate) fn authorized_invitation_role_options(
        &self,
    ) -> &[AuthorizationInvitationRoleOption] {
        self.gateway
            .capabilities
            .as_ref()
            .map(|p| p.global.invitation_role_options.as_slice())
            .unwrap_or_default()
    }
    pub(crate) fn visible(&self) -> bool {
        matches!(
            self.navigation.destination(),
            SemanticDestination::Administration { .. }
        )
    }
    pub(crate) fn members_loading(&self) -> bool {
        self.administration
            .members
            .as_ref()
            .is_some_and(|p| p.request == AdministrationLoadState::Loading)
    }
    pub(crate) fn invitations_loading(&self) -> bool {
        self.administration
            .invitations
            .as_ref()
            .is_some_and(|p| p.request == AdministrationLoadState::Loading)
    }
    pub(crate) fn member_workspaces_saving(&self) -> bool {
        self.administration.operation.as_ref().is_some_and(|p| p.request == AdministrationLoadState::Loading && matches!(p.action, Some(pioneer_client::administration::AdministrationAction::SetMemberWorkspaces { .. })))
    }
    pub(crate) fn workspace_members_loading(&self, id: &WorkspaceId) -> bool {
        self.administration.workspace_members(id).is_none()
    }
    pub(crate) fn members_error(&self) -> Option<String> {
        self.administration.members.as_ref().filter(|p| p.request == AdministrationLoadState::Failed).map(|_| t!("settings.members.load_failed").to_string())
            .or_else(|| self.administration.operation.as_ref().filter(|p| p.request == AdministrationLoadState::Failed && !matches!(p.action, Some(pioneer_client::administration::AdministrationAction::CreateInvitation | pioneer_client::administration::AdministrationAction::RevokeInvitation { .. }))).map(|_| t!("settings.members.action_failed").to_string()))
    }
    pub(crate) fn invitations_error(&self) -> Option<String> {
        self.administration
            .invitations
            .as_ref()
            .filter(|p| p.request == AdministrationLoadState::Failed)
            .map(|_| t!("settings.invitations.load_failed").to_string())
            .or_else(|| self.administration.operation.as_ref().filter(|p| p.request == AdministrationLoadState::Failed && matches!(p.action, Some(pioneer_client::administration::AdministrationAction::RevokeInvitation { .. }))).map(|_| t!("settings.invitations.revoke_failed").to_string()))
    }
    pub(crate) fn open_administration_content(
        &mut self,
        route: AdministrationRoute,
        _: &mut Context<Self>,
    ) {
        self.client.navigate(
            NavigationIntent::Navigate {
                destination: SemanticDestination::Administration { route },
            },
            None,
        );
    }
    pub(crate) fn refresh_members(&mut self, append: bool, _: &mut Context<Self>) {
        self.refresh_page(AdministrationPage::Members, append);
    }
    pub(crate) fn refresh_invitations(&mut self, append: bool, _: &mut Context<Self>) {
        self.refresh_page(AdministrationPage::Invitations, append);
    }
    fn refresh_page(&self, page: AdministrationPage, append: bool) {
        self.client.administration_page_intent(if append {
            AdministrationPageIntent::Next { page }
        } else {
            AdministrationPageIntent::Refresh { page }
        });
    }
    pub(crate) fn refresh_all_workspace_members(&mut self, _: &mut Context<Self>) {
        for page in &self.demanded {
            if matches!(page, AdministrationPage::WorkspaceMembers { .. }) {
                self.refresh_page(page.clone(), false);
            }
        }
    }
    pub(crate) fn own_dialog(
        &mut self,
        clear: impl Fn(&mut Window, &mut App) + 'static,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<crate::dialog_lifetime::DialogLifetime> {
        for dialog in &self.dialogs {
            dialog.update(cx, |dialog, cx| dialog.invalidate(window, cx));
        }
        self.dialogs.retain(|dialog| dialog.read(cx).open());
        let dialog = crate::dialog_lifetime::DialogLifetime::new(clear, cx);
        self.dialogs.push(dialog.clone());
        dialog
    }
    fn sync_loading(&self, cx: &mut Context<Self>) {
        let active = self.visible() && self.window_active;
        self.member_loading.update(cx, |view, cx| {
            view.set_active(active && self.members_loading(), cx)
        });
        self.invitation_loading.update(cx, |view, cx| {
            view.set_active(active && self.invitations_loading(), cx)
        });
    }
    fn presentation_key(&self) -> Option<AdministrationPresentation> {
        self.visible().then(|| {
            let caps = self.principal_presentation_capabilities();
            AdministrationPresentation {
                route: self.administration_content_view(),
                connected: self.gateway.connection_state,
                principal: self
                    .gateway
                    .current_auth
                    .as_ref()
                    .map(|auth| auth.principal.id.to_string()),
                capabilities: (
                    caps.can_view_member_directory,
                    caps.can_view_invitations,
                    caps.can_create_invitation,
                    caps.can_manage_member_lifecycle,
                    caps.can_add_workspace_member,
                    caps.can_remove_workspace_member,
                ),
                invitation_roles: self.authorized_invitation_role_options().to_vec(),
                members: self.administration.members.as_ref().map(|p| p.revision),
                invitations: self.administration.invitations.as_ref().map(|p| p.revision),
                workspaces: self
                    .workspaces
                    .workspaces()
                    .iter()
                    .map(|w| (w.id.clone(), w.name.clone()))
                    .collect(),
                membership: self
                    .administration
                    .workspaces
                    .iter()
                    .map(|(id, p)| (id.to_string(), p.revision))
                    .collect(),
                operation: self.administration.operation.as_ref().map(|p| p.revision),
                avatars: self.member_avatar_state.visible_paths(),
            }
        })
    }
    pub(crate) fn sync_publications(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let previous = self.presentation_key();
        let previous_policy = self.gateway.permissions_epoch;
        // A policy refresh is not a form dismissal. In particular it must not
        // erase an already-issued one-time credential. Client commands retain
        // their authorization fences; route and authenticated identity changes
        // still retire the presentation below.
        let previous_scope = (
            self.visible(),
            self.administration_content_view(),
            self.gateway
                .current_auth
                .as_ref()
                .map(|p| p.principal.id.clone()),
            self.gateway
                .current_auth
                .as_ref()
                .map(|auth| (auth.gateway.id.clone(), auth.session.id.clone())),
        );
        self.navigation = self.client.navigation_snapshot();
        self.workspaces = self.client.workspace_catalog();
        self.gateway = GatewayInput::read(&self.client);
        if previous_scope
            != (
                self.visible(),
                self.administration_content_view(),
                self.gateway
                    .current_auth
                    .as_ref()
                    .map(|p| p.principal.id.clone()),
                self.gateway
                    .current_auth
                    .as_ref()
                    .map(|auth| (auth.gateway.id.clone(), auth.session.id.clone())),
            )
        {
            for dialog in &self.dialogs {
                dialog.update(cx, |dialog, cx| dialog.invalidate(window, cx));
            }
        } else if previous_policy != self.gateway.permissions_epoch {
            for dialog in &self.dialogs {
                dialog.update(cx, |dialog, cx| dialog.policy_refreshed(window, cx));
            }
        }
        self.dialogs.retain(|dialog| dialog.read(cx).open());
        let mut wanted = BTreeSet::new();
        if self.visible() {
            match self.administration_content_view() {
                AdministrationRoute::Members => {
                    wanted.insert(AdministrationPage::Members);
                    for workspace in self.workspaces() {
                        if let Ok(workspace_id) = WorkspaceId::new(workspace.id.clone()) {
                            wanted.insert(AdministrationPage::WorkspaceMembers { workspace_id });
                        }
                    }
                }
                AdministrationRoute::Invitations => {
                    wanted.insert(AdministrationPage::Invitations);
                }
            }
        }
        for page in self.demanded.difference(&wanted) {
            self.client
                .administration_page_intent(AdministrationPageIntent::Release {
                    page: page.clone(),
                });
        }
        for page in wanted.difference(&self.demanded) {
            self.client
                .administration_page_intent(AdministrationPageIntent::Observe {
                    page: page.clone(),
                });
        }
        if wanted.is_empty() {
            self.operations.clear();
        }
        self.demanded = wanted;
        self.administration = AdministrationInput::read(&self.client);
        let mut scopes = vec![
            ClientScope::Navigation,
            ClientScope::Session,
            ClientScope::Administration { workspace_id: None },
            ClientScope::WorkspaceTree { workspace_id: None },
        ];
        if self.visible() {
            scopes.push(ClientScope::AdministrationOperation);
        }
        scopes.extend(
            self.demanded
                .iter()
                .map(|page| ClientScope::AdministrationPage { page: page.clone() }),
        );
        self.member_avatar_state.reconcile(
            self.administration.members().cloned().collect(),
            self.visible(),
            cx,
        );
        scopes.extend(self.member_avatar_state.scopes());
        self.binding.set_scopes(&scopes);
        self.sidebar.update(cx, |view, cx| {
            view.sync(
                self.ui_id("sidebar", "root", "scope").to_string(),
                self.administration_content_view(),
                self.principal_presentation_capabilities(),
                cx,
            )
        });
        self.sync_loading(cx);
        if previous != self.presentation_key() {
            cx.notify();
        }
    }
}
impl Drop for AdministrationView {
    fn drop(&mut self) {
        for page in &self.demanded {
            self.client
                .administration_page_intent(AdministrationPageIntent::Release {
                    page: page.clone(),
                });
        }
    }
}
impl Render for AdministrationView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_administration(window, cx)
    }
}

#[derive(PartialEq)]
struct AdministrationPresentation {
    route: AdministrationRoute,
    connected: GatewayConnectionState,
    principal: Option<String>,
    capabilities: (bool, bool, bool, bool, bool, bool),
    invitation_roles: Vec<AuthorizationInvitationRoleOption>,
    members: Option<u64>,
    invitations: Option<u64>,
    workspaces: Vec<(String, String)>,
    membership: Vec<(String, u64)>,
    operation: Option<u64>,
    avatars: Vec<(String, Option<std::path::PathBuf>)>,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{AdministrationConfig, AdministrationView};
    use crate::ports::*;
    use gpui_kit::component::Root;
    use gpui_kit::{App, Task, TestAppContext};
    use pioneer_client::{
        avatars::{AvatarCacheError, AvatarCacheRequest, AvatarCacheResult},
        core::{ClientCore, ClientScope},
        navigation::{AdministrationRoute, NavigationIntent, SemanticDestination},
    };
    use pioneer_desktop_foundation::{
        ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
    };
    use std::{cell::RefCell, collections::HashSet, rc::Rc, sync::Arc};
    struct Registrar(Rc<RefCell<HashSet<ClientScope>>>);
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            scope: ClientScope,
            _: std::sync::Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            self.0.borrow_mut().insert(scope.clone());
            let scopes = self.0.clone();
            ClientBindingRegistration::new(move || {
                scopes.borrow_mut().remove(&scope);
            })
        }
    }
    struct Ports;
    pub(crate) fn config(core: Arc<ClientCore>) -> AdministrationConfig {
        AdministrationConfig::new(
            core,
            Arc::new(Registrar(Rc::new(RefCell::new(HashSet::new())))),
            Arc::new(Ports),
            Arc::new(Ports),
            Arc::new(Ports),
        )
    }
    impl AdministrationActivationPort for Ports {
        fn copy(
            &self,
            _: AdministrationEffectIdentity,
            _: &str,
            _: &mut App,
        ) -> AdministrationEffectCompletion {
            panic!("unexpected platform effect")
        }
    }
    impl AdministrationExternalNavigationPort for Ports {
        fn open(
            &self,
            _: AdministrationEffectIdentity,
            _: &str,
            _: &mut App,
        ) -> AdministrationEffectCompletion {
            panic!("unexpected platform effect")
        }
    }
    impl AdministrationAvatarPort for Ports {
        fn resolve(
            &self,
            _: AvatarCacheRequest,
            _: tokio_util::sync::CancellationToken,
            _: &mut App,
        ) -> Task<Result<AvatarCacheResult, AvatarCacheError>> {
            panic!("unexpected avatar effect")
        }
    }
    #[gpui_kit::test]
    fn root_follows_client_route_and_retires_only_its_page_demands(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let core = Arc::new(ClientCore::new());
        core.navigate(
            NavigationIntent::Navigate {
                destination: SemanticDestination::Administration {
                    route: AdministrationRoute::Members,
                },
            },
            None,
        );
        let scopes = Rc::new(RefCell::new(HashSet::new()));
        let (root, cx) = cx.add_window_view(|window, cx| {
            Root::new(
                AdministrationView::new(
                    AdministrationConfig::new(
                        core.clone(),
                        Arc::new(Registrar(scopes.clone())),
                        Arc::new(Ports),
                        Arc::new(Ports),
                        Arc::new(Ports),
                    ),
                    window,
                    cx,
                ),
                window,
                cx,
            )
        });
        let view = root.read_with(cx, |root, _| {
            root.view()
                .clone()
                .downcast::<AdministrationView>()
                .unwrap()
        });
        let identity = view.read_with(cx, |view, _| view.ui_id("members", "principal", "actions"));
        let before = view.read_with(cx, |view, _| view.presentation_key());
        core.navigate(
            NavigationIntent::RememberDraft {
                workspace_id: "other".into(),
                thread_id: Some("draft".into()),
            },
            None,
        );
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        assert!(before == view.read_with(cx, |view, _| view.presentation_key()));
        assert_eq!(
            identity,
            view.read_with(cx, |view, _| view.ui_id("members", "principal", "actions"))
        );
        assert!(!scopes.borrow().iter().any(|scope| matches!(
            scope,
            ClientScope::Composer { .. }
                | ClientScope::ProviderRuntime { .. }
                | ClientScope::ProviderCollection { .. }
        )));
        core.navigate(
            NavigationIntent::SetAdministrationRoute {
                route: AdministrationRoute::Invitations,
            },
            None,
        );
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        assert_eq!(
            view.read_with(cx, |view, _| view.administration_content_view()),
            AdministrationRoute::Invitations
        );
        core.navigate(
            NavigationIntent::Navigate {
                destination: SemanticDestination::Threads,
            },
            None,
        );
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        assert!(
            !scopes
                .borrow()
                .iter()
                .any(|scope| matches!(scope, ClientScope::AdministrationPage { .. }))
        );
    }
}
