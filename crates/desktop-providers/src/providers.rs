use crate::{binding::ProviderBinding, input::ProviderInput, ports::*, sidebar::ProviderSidebar};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    authorization::{PrincipalPresentationCapabilities, principal_presentation_capabilities},
    core::{ClientCore, ClientScope},
    navigation::{ClientNavigationState, NavigationIntent, SemanticDestination},
    providers::{runtime::*, store::*},
};
pub(crate) use pioneer_client::{
    providers::selectors::ProviderFilter, state::client_state::GatewayConnectionState,
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
static NEXT_MOUNT: AtomicU64 = AtomicU64::new(1);
pub struct ProviderCatalogConfig {
    client: Arc<ClientCore>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    credential: Arc<dyn ProviderCredentialPort>,
    external: Arc<dyn ProviderExternalNavigationPort>,
}
impl ProviderCatalogConfig {
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        credential: Arc<dyn ProviderCredentialPort>,
        external: Arc<dyn ProviderExternalNavigationPort>,
    ) -> Self {
        Self {
            client,
            registrar,
            credential,
            external,
        }
    }
}
pub(crate) struct GatewayInput {
    pub(crate) endpoint_id: Option<String>,
    pub(crate) capabilities: PrincipalPresentationCapabilities,
    pub(crate) connection_state: GatewayConnectionState,
    pub(crate) ws_connection_id: Option<u64>,
    pub(crate) settings: Option<pioneer_client::providers::types::GatewaySettingsSnapshot>,
}
pub struct ProviderCatalogView {
    pub(crate) client: Arc<ClientCore>,
    pub(crate) providers: ProviderInput,
    pub(crate) gateway: GatewayInput,
    pub(crate) navigation_input: Arc<ClientNavigationState>,
    pub(crate) sidebar_width: Pixels,
    pub(crate) inline_inputs: std::collections::BTreeMap<
        (String, String),
        Entity<crate::view::CliRuntimeInlineInputState>,
    >,
    binding: Arc<ProviderBinding>,
    sidebar: Entity<ProviderSidebar>,
    demanded: Option<String>,
    runtime_demanded: Option<String>,
    pub(crate) refresh_button: Entity<crate::activity::RefreshButton>,
    pub(crate) credential: Arc<dyn ProviderCredentialPort>,
    pub(crate) external: Arc<dyn ProviderExternalNavigationPort>,
    pub(crate) dialogs: Vec<Entity<crate::dialog_lifetime::DialogLifetime>>,
    _release: Subscription,
    pub(crate) mount: u64,
    pub(crate) operation: Option<Task<()>>,
    pub(crate) window_active: bool,
    _publication_task: Task<()>,
}
impl ProviderCatalogView {
    pub fn new(config: ProviderCatalogConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        let mount = NEXT_MOUNT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .expect("provider mount exhausted");
        cx.new(|cx| {
            let binding = ProviderBinding::new(config.registrar);
            let mut changed = binding.changed.subscribe();
            let handle = window.window_handle();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if cx
                        .update_window(handle, |_, window, cx| {
                            view.update(cx, |view, cx| view.sync_publications(window, cx))
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let sidebar = ProviderSidebar::new(cx.weak_entity(), mount, cx);
            let refresh_button = crate::activity::RefreshButton::new(cx.weak_entity(), mount, cx);
            let release = cx.on_release_in(window, |view: &mut Self, window, cx| {
                for dialog in &view.dialogs {
                    dialog.update(cx, |dialog, cx| dialog.invalidate(window, cx));
                }
            });
            let mut view = Self {
                dialogs: Vec::new(),
                _release: release,
                navigation_input: config.client.navigation_snapshot(),
                client: config.client,
                providers: ProviderInput::default(),
                gateway: GatewayInput {
                    endpoint_id: None,
                    capabilities: PrincipalPresentationCapabilities::default(),
                    connection_state: GatewayConnectionState::Disconnected,
                    ws_connection_id: None,
                    settings: None,
                },
                inline_inputs: std::collections::BTreeMap::new(),
                binding,
                sidebar,
                refresh_button,
                runtime_demanded: None,
                sidebar_width: px(320.),
                demanded: None,
                credential: config.credential,
                external: config.external,
                mount,
                operation: None,
                window_active: window.is_window_active(),
                _publication_task: task,
            };
            view.sync_publications(window, cx);
            view
        })
    }
    pub fn sidebar(&self) -> AnyView {
        self.sidebar.clone().into()
    }
    pub fn set_sidebar_width(&mut self, width: Pixels, cx: &mut Context<Self>) {
        if self.sidebar_width != width {
            self.sidebar_width = width;
            if self.visible() {
                cx.notify();
            }
        }
    }
    pub fn set_window_active(&mut self, active: bool, cx: &mut Context<Self>) {
        if self.window_active != active {
            self.window_active = active;
            self.sync_loading(cx);
        }
    }
    pub(crate) fn visible(&self) -> bool {
        matches!(
            self.navigation_input.destination(),
            SemanticDestination::Providers { .. }
        )
    }
    pub(crate) fn active_workspace_id(&self) -> Option<&str> {
        self.navigation_input.workspace_id()
    }
    pub(crate) fn principal_presentation_capabilities(&self) -> PrincipalPresentationCapabilities {
        self.gateway.capabilities
    }
    pub(crate) fn set_provider_filter(&mut self, filter: ProviderFilter, _: &mut Context<Self>) {
        self.client
            .navigate(NavigationIntent::SetProvidersRoute { filter }, None);
    }
    pub(crate) fn refresh_configured_providers(&mut self, _: &mut Context<Self>) {
        if let Some(workspace) = self.active_workspace_id() {
            self.client
                .provider_collection_intent(ProviderCollectionIntent::Refresh {
                    key: ProviderCollectionKey::catalog(workspace),
                });
        }
    }
    pub(crate) fn load_cli_provider_snapshot(&mut self, _: &mut Context<Self>) {
        if let Some(workspace) = self.active_workspace_id() {
            self.client
                .provider_runtime_intent(ProviderRuntimeIntent::Refresh {
                    workspace_id: workspace.into(),
                });
        }
    }
    pub(crate) fn refresh_gateway_settings(&mut self, _: &mut Context<Self>) {
        let _ = self.client.request_gateway_settings();
    }
    pub(crate) fn ui_id(&self, domain: &str, role: &str) -> SharedString {
        format!(
            "providers:{}:{}:{}:{domain}:{role}",
            self.mount,
            self.gateway.endpoint_id.as_deref().unwrap_or_default(),
            self.active_workspace_id().unwrap_or_default()
        )
        .into()
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
        let connected = self.gateway.connection_state == GatewayConnectionState::Connected;
        let loading = self.visible()
            && self.window_active
            && (self.providers.loading() || self.providers.cli_loading());
        self.refresh_button
            .update(cx, |button, cx| button.sync(connected, loading, cx));
    }
    fn presentation_key(&self) -> Option<ProviderPresentation> {
        self.visible().then(|| ProviderPresentation {
            workspace: self.active_workspace_id().map(str::to_owned),
            route: self.navigation_input.providers_route(),
            connected: self.gateway.connection_state,
            settings: self
                .gateway
                .settings
                .as_ref()
                .map(|p| p.cli_runtimes.clone()),
            can_manage: self.gateway.capabilities.can_manage_capabilities,
            catalog: self.providers.catalog.as_ref().map(|p| p.revision()),
            runtimes: self.providers.runtimes.as_ref().map(|p| p.revision()),
            operation: self.providers.operation.as_ref().map(|p| p.revision()),
            connection: self.gateway.ws_connection_id,
        })
    }
    fn sync_publications(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let previous = self.presentation_key();
        let previous_parent = (
            self.gateway.endpoint_id.clone(),
            self.active_workspace_id().map(str::to_owned),
        );
        let previous_scope = (
            self.visible(),
            self.active_workspace_id().map(str::to_owned),
            self.gateway.ws_connection_id,
            self.gateway.capabilities.can_manage_capabilities,
        );
        self.navigation_input = self.client.navigation_snapshot();
        self.gateway.capabilities = self
            .client
            .authorization_snapshot(self.active_workspace_id(), None)
            .or_else(|| self.client.authorization_snapshot(None, None))
            .as_ref()
            .map(principal_presentation_capabilities)
            .unwrap_or_default();
        let session = self.client.gateway_session();
        self.gateway.connection_state = session
            .status
            .as_ref()
            .map_or(GatewayConnectionState::Disconnected, |s| s.connection_state);
        self.gateway.ws_connection_id = session.startup.connection_id;
        self.gateway.endpoint_id = session.startup.endpoint_id.clone();
        if previous_parent
            != (
                self.gateway.endpoint_id.clone(),
                self.active_workspace_id().map(str::to_owned),
            )
        {
            self.providers.clear_scope();
            self.inline_inputs.clear();
        }
        self.gateway.settings = self.client.gateway_settings().settings;
        if previous_scope
            != (
                self.visible(),
                self.active_workspace_id().map(str::to_owned),
                self.gateway.ws_connection_id,
                self.gateway.capabilities.can_manage_capabilities,
            )
        {
            self.operation = None;
            self.providers.login_message = None;
            self.providers.clear_cli_runtime_draft();
            self.inline_inputs.clear();
            for dialog in &self.dialogs {
                dialog.update(cx, |dialog, cx| dialog.invalidate(window, cx));
            }
        }
        self.dialogs.retain(|dialog| dialog.read(cx).open());
        let wanted = self
            .visible()
            .then(|| self.active_workspace_id().map(str::to_owned))
            .flatten();
        if wanted != self.demanded {
            if let Some(old) = self.demanded.take() {
                self.client
                    .provider_collection_intent(ProviderCollectionIntent::Release {
                        key: ProviderCollectionKey::catalog(&old),
                    });
                self.client.cancel_provider_operations(&old);
                self.operation = None;
                self.providers.clear_cli_runtime_draft();
                self.providers.login_message = None;
                self.inline_inputs.clear();
            }
            if let Some(workspace) = &wanted {
                self.client
                    .provider_collection_intent(ProviderCollectionIntent::Observe {
                        key: ProviderCollectionKey::catalog(workspace),
                    });
            }
            self.demanded = wanted;
        }
        let runtime_wanted = self.demanded.clone().filter(|_| {
            self.principal_presentation_capabilities()
                .can_manage_capabilities
        });
        if self.runtime_demanded != runtime_wanted {
            if let Some(workspace_id) = self.runtime_demanded.take() {
                self.client
                    .provider_runtime_intent(ProviderRuntimeIntent::Release { workspace_id });
                self.inline_inputs.clear();
            }
            if let Some(workspace_id) = &runtime_wanted {
                self.client
                    .provider_runtime_intent(ProviderRuntimeIntent::Observe {
                        workspace_id: workspace_id.clone(),
                    });
                if self.gateway.settings.is_none() {
                    let _ = self.client.request_gateway_settings();
                }
            }
            self.runtime_demanded = runtime_wanted;
        }
        let mut scopes = vec![
            ClientScope::Navigation,
            ClientScope::Session,
            ClientScope::Administration { workspace_id: None },
        ];
        if let Some(workspace) = &self.demanded {
            scopes.extend([
                ClientScope::ProviderCollection {
                    key: ProviderCollectionKey::catalog(workspace),
                },
                ClientScope::ProviderRuntime {
                    workspace_id: workspace.clone(),
                },
                ClientScope::ProviderOperation {
                    workspace_id: workspace.clone(),
                },
                ClientScope::Settings,
                ClientScope::Administration {
                    workspace_id: Some(workspace.clone()),
                },
            ]);
            self.providers.catalog = self
                .client
                .provider_collection_snapshot(&ProviderCollectionKey::catalog(workspace));
            self.providers.runtimes = self.client.provider_runtime_snapshot(workspace);
            let operation = self.client.provider_operation_snapshot(workspace);
            if self.providers.operation.is_some() && operation.is_none() {
                self.providers.login_message = None;
                self.operation = None;
            }
            self.providers.operation = operation;
            self.providers.error = self
                .providers
                .catalog
                .as_ref()
                .filter(|p| {
                    matches!(
                        p.request(),
                        ProviderLoadState::Failed | ProviderLoadState::Forbidden
                    )
                })
                .map(|_| t!("providers.error.load_failed").to_string())
                .or_else(|| {
                    self.providers
                        .operation
                        .as_ref()
                        .filter(|p| p.request() == ProviderLoadState::Failed)
                        .map(|_| t!("providers.error.save_failed").to_string())
                });
            self.providers.cli_error = self
                .providers
                .runtimes
                .as_ref()
                .filter(|p| *p.request() == ProviderRuntimeRequestState::Failed)
                .map(|_| t!("providers.error.load_failed").to_string());
        } else {
            self.providers.catalog = None;
            self.providers.runtimes = None;
            self.providers.operation = None;
        }
        self.binding.set_scopes(&scopes);
        let route = self.navigation_input.providers_route();
        let can_manage = self
            .principal_presentation_capabilities()
            .can_manage_capabilities;
        self.sidebar.update(cx, |view, cx| {
            view.sync(
                self.ui_id("sidebar", "scope").to_string(),
                route,
                can_manage,
                cx,
            )
        });
        self.sync_inline_inputs(window, cx);
        self.sync_loading(cx);
        if previous != self.presentation_key() {
            cx.notify();
        }
    }
}
impl Render for ProviderCatalogView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_providers(window, cx)
    }
}
impl Drop for ProviderCatalogView {
    fn drop(&mut self) {
        if let Some(workspace) = &self.demanded {
            self.client
                .provider_collection_intent(ProviderCollectionIntent::Release {
                    key: ProviderCollectionKey::catalog(workspace),
                });
            self.client.cancel_provider_operations(workspace);
        }
        if let Some(workspace_id) = &self.runtime_demanded {
            self.client
                .provider_runtime_intent(ProviderRuntimeIntent::Release {
                    workspace_id: workspace_id.clone(),
                });
        }
    }
}

#[derive(PartialEq)]
struct ProviderPresentation {
    workspace: Option<String>,
    route: ProviderFilter,
    connected: GatewayConnectionState,
    settings: Option<pioneer_client::providers::types::GatewayCliRuntimeSettings>,
    can_manage: bool,
    catalog: Option<u64>,
    runtimes: Option<u64>,
    operation: Option<u64>,
    connection: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::{ProviderCatalogConfig, ProviderCatalogView};
    use crate::ports::*;
    use gpui_kit::component::Root;
    use gpui_kit::{App, AppContext, TestAppContext};
    use pioneer_client::{
        core::{ClientCore, ClientScope},
        navigation::{NavigationIntent, SemanticDestination},
        providers::selectors::ProviderFilter,
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
    impl ProviderCredentialPort for Ports {
        fn copy(
            &self,
            _: ProviderEffectIdentity,
            _: &str,
            _: &mut App,
        ) -> ProviderEffectCompletion {
            panic!("unexpected platform effect")
        }
    }
    impl ProviderExternalNavigationPort for Ports {
        fn open_path(
            &self,
            _: ProviderEffectIdentity,
            _: &str,
            _: &mut App,
        ) -> ProviderEffectCompletion {
            panic!("unexpected platform effect")
        }
    }
    #[gpui_kit::test]
    fn retained_root_observes_only_its_scopes_and_identity_tracks_parent_not_position(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let core = Arc::new(ClientCore::new());
        core.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("one".into()),
            },
            None,
        );
        core.navigate(
            NavigationIntent::Navigate {
                destination: SemanticDestination::Providers {
                    filter: ProviderFilter::Api,
                },
            },
            None,
        );
        let scopes = Rc::new(RefCell::new(HashSet::new()));
        let (root, cx) = cx.add_window_view(|window, cx| {
            Root::new(
                ProviderCatalogView::new(
                    ProviderCatalogConfig::new(
                        core.clone(),
                        Arc::new(Registrar(scopes.clone())),
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
                .downcast::<ProviderCatalogView>()
                .unwrap()
        });
        let identity = view.read_with(cx, |view, _| view.ui_id("openai", "configure"));
        let old_key = view.read_with(cx, |view, _| view.presentation_key());
        core.navigate(
            NavigationIntent::RememberDraft {
                workspace_id: "other".into(),
                thread_id: Some("draft".into()),
            },
            None,
        );
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        assert!(old_key == view.read_with(cx, |view, _| view.presentation_key()));
        assert_eq!(
            identity,
            view.read_with(cx, |view, _| view.ui_id("openai", "configure"))
        );
        assert!(!scopes.borrow().iter().any(|scope| matches!(
            scope,
            ClientScope::Composer { .. }
                | ClientScope::Timeline { .. }
                | ClientScope::AdministrationPage { .. }
        )));
        view.update(cx, |view, _| {
            view.providers.toggle_cli_runtime_expanded("codex".into());
            view.providers.login_message = Some("synthetic login instruction".into());
        });
        core.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("two".into()),
            },
            None,
        );
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        assert_ne!(
            identity,
            view.read_with(cx, |view, _| view.ui_id("openai", "configure"))
        );
        assert!(view.read_with(cx, |view, _| {
            view.providers.expanded_cli_runtime_ids().is_empty()
                && view.providers.login_message.is_none()
        }));
        core.navigate(
            NavigationIntent::Navigate {
                destination: SemanticDestination::Threads,
            },
            None,
        );
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        assert!(scopes.borrow().iter().all(|scope| matches!(
            scope,
            ClientScope::Navigation
                | ClientScope::Session
                | ClientScope::Administration { workspace_id: None }
        )));
    }
}
