use crate::{binding::ProviderBinding, input::ProviderInput, ports::*, sidebar::ProviderSidebar};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    authorization::{PrincipalPresentationCapabilities, principal_presentation_capabilities},
    core::{ClientCore, ClientScope},
    navigation::{ClientNavigationState, NavigationIntent, SemanticDestination},
    providers::{operations::*, runtime::*, store::*},
    settings::runtime::SettingsIntent,
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
    model_picker: Arc<dyn ProviderModelPickerPort>,
}
impl ProviderCatalogConfig {
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        credential: Arc<dyn ProviderCredentialPort>,
        external: Arc<dyn ProviderExternalNavigationPort>,
        model_picker: Arc<dyn ProviderModelPickerPort>,
    ) -> Self {
        Self {
            client,
            registrar,
            credential,
            external,
            model_picker,
        }
    }
}
pub(crate) struct GatewayInput {
    pub(crate) endpoint_id: Option<String>,
    pub(crate) capabilities: PrincipalPresentationCapabilities,
    pub(crate) connection_state: GatewayConnectionState,
    pub(crate) ws_connection_id: Option<u64>,
    pub(crate) settings: Option<pioneer_client::providers::types::GatewaySettingsSnapshot>,
    pub(crate) settings_loading: bool,
    pub(crate) settings_error: Option<String>,
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
    pub(crate) model_picker: Arc<dyn ProviderModelPickerPort>,
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
                    settings_loading: false,
                    settings_error: None,
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
                model_picker: config.model_picker,
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
        self.client.settings_intent(SettingsIntent::Refresh);
    }
    pub(crate) fn settings_controls_available(&self) -> bool {
        self.active_workspace_id().is_some()
            && self.gateway.connection_state == GatewayConnectionState::Connected
            && self.gateway.capabilities.can_manage_capabilities
            && self.gateway.capabilities.can_manage_gateway_settings
            && self.gateway.settings.is_some()
            && !self.gateway.settings_loading
            && self.gateway.settings_error.as_deref() != Some("settings_load_failed")
            && !self.providers.loading()
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
            && (self.providers.loading()
                || self.providers.cli_loading()
                || self.gateway.settings_loading);
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
            can_manage_settings: self.gateway.capabilities.can_manage_gateway_settings,
            settings_loading: self.gateway.settings_loading,
            settings_error: self.gateway.settings_error.clone(),
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
            self.gateway.endpoint_id.clone(),
            self.gateway.ws_connection_id,
            self.gateway.connection_state,
            self.gateway.capabilities.can_manage_capabilities,
            self.gateway.capabilities.can_manage_gateway_settings,
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
        let settings = self.client.gateway_settings();
        self.gateway.settings = settings
            .settings
            .filter(|_| settings.workspace_id.as_deref() == self.active_workspace_id());
        self.gateway.settings_loading = settings.loading || settings.saving;
        self.gateway.settings_error = settings.error;
        let scope_changed = previous_scope
            != (
                self.visible(),
                self.active_workspace_id().map(str::to_owned),
                self.gateway.endpoint_id.clone(),
                self.gateway.ws_connection_id,
                self.gateway.connection_state,
                self.gateway.capabilities.can_manage_capabilities,
                self.gateway.capabilities.can_manage_gateway_settings,
            );
        if scope_changed {
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
            }
            self.runtime_demanded = runtime_wanted;
        }
        if self.visible()
            && self.demanded.is_some()
            && self.gateway.capabilities.can_manage_capabilities
            && self.gateway.capabilities.can_manage_gateway_settings
            && self.gateway.connection_state == GatewayConnectionState::Connected
            && (scope_changed
                || (self.gateway.settings.is_none()
                    && self.gateway.settings_error.is_none()
                    && !self.gateway.settings_loading))
        {
            self.refresh_gateway_settings(cx);
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
            let (api_error, cli_error) = self
                .providers
                .operation
                .as_deref()
                .map(provider_operation_errors)
                .unwrap_or_default();
            self.providers.error = api_error.or_else(|| {
                self.providers
                    .catalog
                    .as_ref()
                    .filter(|p| {
                        matches!(
                            p.request(),
                            ProviderLoadState::Failed | ProviderLoadState::Forbidden
                        )
                    })
                    .map(|_| t!("providers.error.load_failed").to_string())
            });
            self.providers.cli_error = cli_error
                .or_else(|| {
                    self.gateway
                        .settings_error
                        .as_ref()
                        .filter(|error| error.as_str() == "settings_load_failed")
                        .map(|_| t!("providers.cli.error.settings_load_failed").to_string())
                })
                .or_else(|| {
                    (!self.gateway.capabilities.can_manage_gateway_settings)
                        .then(|| t!("providers.cli.error.settings_forbidden").to_string())
                })
                .or_else(|| {
                    self.providers
                        .runtimes
                        .as_ref()
                        .filter(|p| *p.request() == ProviderRuntimeRequestState::Failed)
                        .map(|_| t!("providers.error.load_failed").to_string())
                });
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
    can_manage_settings: bool,
    settings_loading: bool,
    settings_error: Option<String>,
    catalog: Option<u64>,
    runtimes: Option<u64>,
    operation: Option<u64>,
    connection: Option<u64>,
}

fn provider_operation_errors(
    operation: &ProviderOperationPublication,
) -> (Option<String>, Option<String>) {
    if operation.request() != ProviderLoadState::Failed {
        return (None, None);
    }
    let (cli, key) = match operation.action() {
        ProviderActionKind::AcpSession => (true, "providers.cli.error.session_failed"),
        ProviderActionKind::Configure => (false, "providers.error.configure_failed"),
        ProviderActionKind::Disconnect => (false, "providers.error.disconnect_failed"),
        ProviderActionKind::Connect => (true, "providers.cli.error.login_failed"),
        ProviderActionKind::SetRuntimeProxy | ProviderActionKind::RemoveRuntimeProxy => {
            (true, "providers.cli.error.proxy_failed")
        }
        ProviderActionKind::SaveRuntime => (true, "providers.cli.error.save_failed"),
        ProviderActionKind::SetRuntimeEnabled => (true, "providers.cli.error.enabled_failed"),
    };
    let message = Some(
        t!(
            key,
            provider = operation.target(),
            runtime = operation.target()
        )
        .to_string(),
    );
    if cli {
        (None, message)
    } else {
        (message, None)
    }
}

#[cfg(test)]
mod tests {
    use super::{ProviderCatalogConfig, ProviderCatalogView};
    use crate::ports::*;
    use gpui_kit::component::{Root, input::InputState};
    use gpui_kit::{App, AppContext, TestAppContext, Window};
    use pioneer_client::{
        core::{
            ClientCore, ClientMutationAuthority, ClientRevisions, ClientScope, ContentRevision,
            DomainRevision, PresentationRevision, ScopedRevision,
        },
        gateway::session_controller::GatewaySessionPublication,
        navigation::{NavigationIntent, SemanticDestination},
        providers::credentials::provider_credential_read_for_test,
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
    impl ProviderModelPickerPort for Ports {
        fn open(&self, _: ProviderModelPickerRequest, _: &mut Window, _: &mut App) {
            panic!("model picker is outside this retained-publication fixture");
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
    fn initial_and_explicit_settings_refresh_queue_the_existing_worker(cx: &mut TestAppContext) {
        use pioneer_client::{
            core::ClientTransitionOutcome,
            settings::runtime::SettingsIntent,
            state::{
                client_state::{GatewayConnectionState, GatewayStatusLevel},
                reducers::{GatewayStatusProjection, GatewayStatusTextUpdate},
            },
        };
        cx.update(gpui_kit::init);
        for initial in [true, false] {
            let core = pioneer_client::catalog_test_support::settings_model_picker_client();
            core.navigate(
                NavigationIntent::Navigate {
                    destination: SemanticDestination::Providers {
                        filter: ProviderFilter::Cli,
                    },
                },
                None,
            );
            if initial {
                let mut session = GatewaySessionPublication::default();
                session.status = Some(GatewayStatusProjection {
                    status: GatewayStatusTextUpdate::KeepExisting,
                    status_level: GatewayStatusLevel::Connected,
                    connection_state: GatewayConnectionState::Connected,
                    clear_gateway_error: false,
                });
                let revision = core
                    .snapshot(&ClientScope::Session)
                    .map_or(1, |p| p.revisions().scoped().get() + 1);
                core.publish(
                    &ClientMutationAuthority::for_test(),
                    ClientScope::Session,
                    ClientRevisions::new(
                        DomainRevision::new(revision),
                        PresentationRevision::new(revision),
                        ContentRevision::new(revision),
                        ScopedRevision::new(revision),
                    ),
                    Arc::new(session),
                    vec![],
                );
            }
            let scopes = Rc::new(RefCell::new(HashSet::new()));
            let (view, cx) = cx.add_window_view(|window, cx| {
                Root::new(
                    ProviderCatalogView::new(
                        ProviderCatalogConfig::new(
                            core.clone(),
                            Arc::new(Registrar(scopes)),
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
            let view = view.read_with(cx, |root, _| {
                root.view()
                    .clone()
                    .downcast::<ProviderCatalogView>()
                    .unwrap()
            });
            assert!(!view.read_with(cx, |view, _| view.settings_controls_available()));
            if !initial {
                view.update(cx, |view, cx| view.refresh_gateway_settings(cx));
            }
            // No worker is started in this fixture. Noop proves that the view
            // queued Refresh, rather than only marking a request as loading.
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Noop
            );
            assert!(!core.gateway_settings().loading);
        }
    }

    #[test]
    fn failed_provider_operations_use_their_action_and_target() {
        use pioneer_client::providers::{operations::*, types::*};
        for command in [
            ProviderCommand::SetRuntimeEnabled {
                workspace_id: "workspace".into(),
                runtime_id: "codex-test".into(),
                enabled: false,
            },
            ProviderCommand::Configure(ProviderConfigureParams {
                workspace_id: "workspace".into(),
                provider: "openai".into(),
                api_key: Some("synthetic".into()),
                proxy_url: None,
                clear_proxy: false,
                base_url: None,
                clear_base_url: false,
            }),
            ProviderCommand::Disconnect(ProviderDeleteApiKeyParams {
                workspace_id: "workspace".into(),
                provider: "openai".into(),
            }),
        ] {
            let core = pioneer_client::catalog_test_support::client();
            let operation = core.prepare_provider_command(command).unwrap();
            // The fixture has no worker, so the command publishes a real failure.
            assert!(operation.execute(Arc::downgrade(&core)).is_err());
            let publication = core.provider_operation_snapshot("workspace").unwrap();
            let (api, cli) = super::provider_operation_errors(&publication);
            match publication.action() {
                ProviderActionKind::SetRuntimeEnabled => {
                    assert!(api.is_none());
                    assert_eq!(
                        cli,
                        Some(
                            t!("providers.cli.error.enabled_failed", runtime = "codex-test")
                                .to_string()
                        )
                    );
                }
                ProviderActionKind::Configure => {
                    assert!(cli.is_none());
                    assert_eq!(
                        api,
                        Some(
                            t!("providers.error.configure_failed", provider = "openai").to_string()
                        )
                    );
                }
                ProviderActionKind::Disconnect => {
                    assert!(cli.is_none());
                    assert_eq!(
                        api,
                        Some(
                            t!("providers.error.disconnect_failed", provider = "openai")
                                .to_string()
                        )
                    );
                }
                _ => unreachable!(),
            }
            core.cancel_provider_operations("workspace");
            assert_eq!(
                super::provider_operation_errors(
                    &core.provider_operation_snapshot("workspace").unwrap()
                ),
                (None, None)
            );
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
        let (lease, read, late_reply) = provider_credential_read_for_test();
        let (url_input, url_form) = cx.update(|window, cx| {
            let input = cx.new(|cx| {
                let mut input = InputState::new(window, cx);
                input.set_value("https://old.example/private", window, cx);
                input
            });
            let form = crate::credential_form::ProxyForm::new_with_pending_for_test(
                input.clone(),
                (lease, read),
                Some("https://old.example/private".into()),
                window,
                cx,
            );
            let weak = form.downgrade();
            let guard = crate::dialog_lifetime::DialogLifetime::new(
                move |window, cx| {
                    let _ = weak.update(cx, |form, cx| form.clear(window, cx));
                },
                cx,
            );
            view.update(cx, |view, _| view.dialogs.push(guard));
            (input, form)
        });
        core.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("two".into()),
            },
            None,
        );
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        let _ = late_reply.send(Ok(Some("https://late.example/private".into())));
        cx.run_until_parked();
        assert!(url_input.read_with(cx, |input, _| input.value().is_empty()));
        assert!(url_form.read_with(cx, |form, _| form.original().is_none()));
        assert_ne!(
            identity,
            view.read_with(cx, |view, _| view.ui_id("openai", "configure"))
        );
        assert!(view.read_with(cx, |view, _| {
            view.providers.expanded_cli_runtime_ids().is_empty()
                && view.providers.login_message.is_none()
        }));
        let publish_gateway = |endpoint: &str, revision: u64| {
            let mut session = GatewaySessionPublication::default();
            session.startup.endpoint_id = Some(endpoint.into());
            session.startup.connection_id = Some(revision);
            core.publish(
                &ClientMutationAuthority::for_test(),
                ClientScope::Session,
                ClientRevisions::new(
                    DomainRevision::new(revision),
                    PresentationRevision::new(revision),
                    ContentRevision::new(revision),
                    ScopedRevision::new(revision),
                ),
                Arc::new(session),
                vec![],
            );
        };
        let first_gateway_revision = core
            .snapshot(&ClientScope::Session)
            .map_or(1, |publication| publication.revisions().scoped().get() + 1);
        publish_gateway("gateway-a", first_gateway_revision);
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        let (gateway_lease, gateway_read, gateway_reply) = provider_credential_read_for_test();
        let (gateway_input, gateway_form) = cx.update(|window, cx| {
            let input = cx.new(|cx| {
                let mut state = InputState::new(window, cx);
                state.set_value("https://gateway-a.example/private", window, cx);
                state
            });
            let form = crate::credential_form::ProxyForm::new_with_pending_for_test(
                input.clone(),
                (gateway_lease, gateway_read),
                Some("https://gateway-a.example/private".into()),
                window,
                cx,
            );
            let weak = form.downgrade();
            view.update(cx, |view, cx| {
                view.own_dialog(
                    move |window, cx| {
                        let _ = weak.update(cx, |form, cx| form.clear(window, cx));
                    },
                    window,
                    cx,
                );
            });
            (input, form)
        });
        publish_gateway("gateway-b", first_gateway_revision + 1);
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        let _ = gateway_reply.send(Ok(Some("https://late.example/private".into())));
        cx.run_until_parked();
        assert!(gateway_input.read_with(cx, |input, _| input.value().is_empty()));
        assert!(gateway_form.read_with(cx, |form, _| form.original().is_none()));
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

    #[gpui_kit::test]
    fn permission_revocation_clears_base_url_form_and_retires_pending_read(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let core = pioneer_client::catalog_test_support::settings_client();
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
        let (root, cx) = cx.add_window_view(|window, cx| {
            Root::new(
                ProviderCatalogView::new(
                    ProviderCatalogConfig::new(
                        core.clone(),
                        Arc::new(Registrar(Rc::new(RefCell::new(HashSet::new())))),
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
                .downcast::<ProviderCatalogView>()
                .unwrap()
        });
        assert!(view.read_with(cx, |view, _| {
            view.gateway.capabilities.can_manage_capabilities
        }));
        let (lease, read, reply) = provider_credential_read_for_test();
        let (input, form) = cx.update(|window, cx| {
            let input = cx.new(|cx| {
                let mut state = InputState::new(window, cx);
                state.set_value("https://old.example/private", window, cx);
                state
            });
            let form = crate::credential_form::ProxyForm::new_with_pending_for_test(
                input.clone(),
                (lease, read),
                Some("https://old.example/private".into()),
                window,
                cx,
            );
            let weak = form.downgrade();
            view.update(cx, |view, cx| {
                let guard = view.own_dialog(
                    move |window, cx| {
                        let _ = weak.update(cx, |form, cx| form.clear(window, cx));
                    },
                    window,
                    cx,
                );
                guard.update(cx, |guard, cx| guard.track_form(&form, cx));
            });
            (input, form)
        });
        let mut capabilities = core.authorization_snapshot(None, None).unwrap();
        capabilities.authorization_revision += 1;
        capabilities.global.can_manage_capabilities = false;
        let (generation, connection) = core.current_auth_ticket();
        core.accept_authorization_projection(generation, connection, capabilities);
        cx.update(|window, cx| view.update(cx, |view, cx| view.sync_publications(window, cx)));
        let _ = reply.send(Ok(Some("https://late.example/private".into())));
        cx.run_until_parked();
        assert!(input.read_with(cx, |input, _| input.value().is_empty()));
        assert!(form.read_with(cx, |form, _| form.original().is_none()));
        assert_eq!(
            form.read_with(cx, |form, cx| form.mutation(cx)),
            (None, false)
        );
    }
}
