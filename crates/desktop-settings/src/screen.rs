use crate::{
    binding::SettingsBinding, platform::SettingsPlatform, progress::ProgressIndicatorView,
};
use gpui_kit::component::{
    input::InputState,
    tree::{TreeItem, TreeState},
};
use gpui_kit::{prelude::*, *};
use pioneer_client::settings::types::*;
pub(crate) use pioneer_client::settings::{
    memory::{MemoryModelSetting, MemorySettingToggle},
    self_improvement::SelfImprovementModelSetting,
};
use pioneer_client::{
    authorization::PrincipalPresentationCapabilities,
    core::{ClientCore, ClientIntent, ClientScope},
    settings::runtime::{SettingsIntent, SettingsPage},
};
pub(crate) use pioneer_client::{
    navigation::SettingsRoute as SettingsContentView, state::client_state::GatewayConnectionState,
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::{rc::Rc, sync::Arc};
pub(crate) const SETTINGS_CONTENT_GENERAL_NODE_ID: &str = "settings:general";
pub(crate) const SETTINGS_CONTENT_ACCOUNT_NODE_ID: &str = "settings:account";
pub(crate) const SETTINGS_CONTENT_MEMORY_NODE_ID: &str = "settings:memory";
pub(crate) const SETTINGS_CONTENT_SELF_IMPROVEMENT_NODE_ID: &str = "settings:self-improvement";
#[derive(PartialEq, Eq)]
pub(crate) enum VoiceInputEnableAction {
    Sent,
    NeedsSelection,
    Noop,
}
#[derive(Clone)]
pub struct SettingsConfig {
    pub client: Arc<ClientCore>,
    pub bindings: Arc<dyn ClientBindingRegistrar>,
    pub platform: Rc<dyn SettingsPlatform>,
    pub avatars: Rc<dyn crate::platform::SettingsAvatarPort>,
    pub photos: Rc<dyn crate::platform::SettingsPhotoPort>,
}
impl SettingsConfig {
    pub(crate) fn avatar_path(&self, principal: &str) -> Option<std::path::PathBuf> {
        use pioneer_client::avatars::{AvatarPublication, avatar_identity_key};
        let auth = self.client.current_auth()?;
        if auth.principal.id.as_str() != principal {
            return None;
        }
        let revision = auth.principal.avatar_revision.as_deref()?;
        let publication = self
            .client
            .snapshot(&ClientScope::Avatar {
                principal_id: avatar_identity_key(principal, revision),
            })?
            .typed::<AvatarPublication>()?;
        let avatar = publication.payload();
        if avatar.principal_id() != principal || avatar.avatar_revision() != revision {
            return None;
        }
        avatar.local_path().map(|path| path.as_path().to_path_buf())
    }
}
pub struct SettingsView {
    config: SettingsConfig,
    general_actions: Entity<crate::general_actions::GeneralActionsView>,
    pages: Vec<Entity<SettingsScreenView>>,
    sidebar: Entity<SettingsSidebarView>,
    route: SettingsContentView,
    workspace_id: Option<String>,
    active: bool,
    _binding: Arc<SettingsBinding>,
    _task: Task<()>,
}
impl SettingsView {
    pub fn new(config: SettingsConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        cx.new(|cx| {
            let binding = SettingsBinding::new(vec![ClientScope::Navigation], &config.bindings);
            let mut changed = binding.changed.subscribe();
            let handle = window.window_handle();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if handle
                        .update(cx, |_, _, cx| view.update(cx, |view, cx| view.sync(cx)))
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let pages = [
                SettingsContentView::Account,
                SettingsContentView::General,
                SettingsContentView::Memory,
                SettingsContentView::SelfImprovement,
            ]
            .into_iter()
            .map(|route| SettingsScreenView::new(config.clone(), route, None, window, cx))
            .collect();
            let sidebar = SettingsSidebarView::new(config.clone(), cx);
            Self {
                general_actions: crate::general_actions::GeneralActionsView::new(
                    config.clone(),
                    cx,
                ),
                route: config.client.navigation_snapshot().settings_route(),
                workspace_id: config
                    .client
                    .navigation_snapshot()
                    .workspace_id()
                    .map(str::to_owned),
                config,
                pages,
                sidebar,
                active: false,
                _binding: binding,
                _task: task,
            }
        })
    }
    pub fn general_actions_surface(&self) -> AnyView {
        self.general_actions.clone().into()
    }
    pub fn sidebar_surface(&self) -> AnyView {
        self.sidebar.clone().into()
    }
    pub fn set_active(&mut self, active: bool, cx: &mut Context<Self>) {
        if self.active != active {
            self.active = active;
            self.sync(cx);
        }
    }
    fn sync(&mut self, cx: &mut Context<Self>) {
        let workspace = self
            .config
            .client
            .navigation_snapshot()
            .workspace_id()
            .map(str::to_owned);
        if self.workspace_id != workspace {
            self.workspace_id = workspace;
            if self.active {
                self.config.client.settings_intent(SettingsIntent::Refresh);
            }
        }
        let route = self.config.client.navigation_snapshot().settings_route();
        let changed = self.route != route;
        self.route = route;
        for page in &self.pages {
            page.update(cx, |page, cx| {
                page.set_active(self.active && page.route == route, cx)
            });
        }
        if changed {
            cx.notify();
        }
    }
}
impl Render for SettingsView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.pages
            .iter()
            .find(|page| page.read(cx).route == self.route)
            .expect("settings page exists")
            .clone()
    }
}
pub(crate) struct GatewayInput {
    pub settings_workspace_id: Option<String>,
    pub auth_sessions: pioneer_client::gateway::identity_authorization::AuthSessionsStore,
    pub settings: Option<GatewaySettingsSnapshot>,
    pub settings_loading: bool,
    pub settings_error: Option<String>,
    pub current_auth: Option<AuthMeResponse>,
    pub capability_snapshot: Option<AuthorizationCapabilitySnapshot>,
    pub connection_state: GatewayConnectionState,
}
pub(crate) struct SettingsScreenView {
    pub config: SettingsConfig,
    pub route: SettingsContentView,
    pub page: Option<SettingsPage>,
    pub gateway: GatewayInput,
    pub remote_access_settings_expanded: bool,
    pub remote_access_key_input_revision: u64,
    pub remote_key: Entity<InputState>,
    remote_key_authority: u64,
    remote_key_configured: bool,
    pub voice_input_action_error: Option<String>,
    pub progress: Entity<ProgressIndicatorView>,
    pub pending_notification: Option<String>,
    pub confirmation: Option<Task<()>>,
    pub profile_subscription: Option<Subscription>,
    pub profile_editor: Option<Entity<crate::account::ProfileEditor>>,
    pub remote: Option<Entity<Self>>,
    pub voice: Option<Entity<Self>>,
    pub workspace_id: Option<String>,
    last_projection: serde_json::Value,
    active: bool,
    demand: Option<pioneer_client::settings::runtime::SettingsPageDemand>,
    sessions_demand: Option<pioneer_client::settings::runtime::AuthSessionsDemand>,
    _binding: Arc<SettingsBinding>,
    _task: Task<()>,
    _inputs: Vec<Subscription>,
}
impl SettingsScreenView {
    fn new(
        config: SettingsConfig,
        route: SettingsContentView,
        page: Option<SettingsPage>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            let actual = page.or(match route {
                SettingsContentView::General => Some(SettingsPage::General),
                SettingsContentView::Memory => Some(SettingsPage::Memory),
                SettingsContentView::SelfImprovement => Some(SettingsPage::SelfImprovement),
                _ => None,
            });
            let mut scopes = vec![
                ClientScope::Administration { workspace_id: None },
                ClientScope::Session,
                ClientScope::Navigation,
            ];
            if let Some(page) = actual {
                scopes.push(ClientScope::SettingsPage { page });
            } else {
                scopes.push(ClientScope::AuthSessions);
            }
            let binding = SettingsBinding::new(scopes, &config.bindings);
            let mut changed = binding.changed.subscribe();
            let handle = window.window_handle();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if handle
                        .update(cx, |_, window, cx| {
                            view.update(cx, |view, cx| view.sync(window, cx))
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let remote_key = cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(t!("settings.remote_access.key_placeholder").to_string())
                    .masked(true)
            });
            let inputs = vec![cx.subscribe(
                &remote_key,
                |view, input, event: &gpui_kit::component::input::InputEvent, cx| {
                    if matches!(
                        event,
                        gpui_kit::component::input::InputEvent::Blur
                            | gpui_kit::component::input::InputEvent::PressEnter { .. }
                    ) {
                        let reset_generation = view.config.client.snapshot(&ClientScope::SettingsPage { page: SettingsPage::RemoteAccess })
                            .and_then(|p| p.typed::<pioneer_client::settings::runtime::SettingsPagePublication>())
                            .map(|p| p.payload().input_reset_generation);
                        if view.remote_key_authority == view.config.client.authorization_connection_generation()
                            && reset_generation == Some(view.remote_access_key_input_revision)
                        {
                            let key = input.read(cx).value().to_string();
                            view.save_remote_access_key_inline(key, cx);
                        }
                    }
                },
            )];
            let remote = (route == SettingsContentView::General && page.is_none()).then(|| {
                Self::new(
                    config.clone(),
                    route,
                    Some(SettingsPage::RemoteAccess),
                    window,
                    cx,
                )
            });
            let voice = (route == SettingsContentView::General && page.is_none())
                .then(|| Self::new(config.clone(), route, Some(SettingsPage::Voice), window, cx));
            let gateway = Self::gateway_input(&config.client);
            Self {
                workspace_id: config
                    .client
                    .navigation_snapshot()
                    .workspace_id()
                    .map(str::to_owned),
                config: config.clone(),
                route,
                page: actual,
                gateway,
                remote_access_settings_expanded: false,
                remote_access_key_input_revision: 0,
                remote_key,
                remote_key_authority: config.client.authorization_connection_generation(),
                remote_key_configured: false,
                voice_input_action_error: None,
                progress: ProgressIndicatorView::new(cx),
                pending_notification: None,
                confirmation: None,
                profile_subscription: None,
                profile_editor: None,
                remote,
                voice,
                last_projection: serde_json::Value::Null,
                active: false,
                demand: None,
                sessions_demand: None,
                _binding: binding,
                _task: task,
                _inputs: inputs,
            }
        })
    }
    fn gateway_input(client: &Arc<ClientCore>) -> GatewayInput {
        let settings = client.gateway_settings();
        GatewayInput {
            settings_workspace_id: settings.workspace_id,
            auth_sessions: client.auth_sessions(),
            settings: settings.settings,
            settings_loading: settings.loading,
            settings_error: settings.error,
            current_auth: client.current_auth(),
            capability_snapshot: client.authorization_snapshot(None, None),
            connection_state: client
                .gateway_session()
                .status
                .as_ref()
                .map_or(GatewayConnectionState::Disconnected, |s| s.connection_state),
        }
    }
    fn sync(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.page.is_none() {
            self._binding.sync_avatar(&self.config, self.active, cx);
        }
        let page_scope = self
            .page
            .map(|page| ClientScope::SettingsPage { page })
            .unwrap_or(ClientScope::AuthSessions);
        let auth = self.config.client.current_auth();
        let principal = auth.as_ref().map(|auth| auth.principal.id.to_string());
        let projection = serde_json::json!({
          "page_revision":self.config.client.snapshot(&page_scope).map(|p|p.revisions().scoped().get()),
          "manager":self.config.client.authorization_snapshot(None,None).is_some_and(|s|s.global.can_manage_gateway_settings),
          "principal":principal,
          "account":if self.page.is_none(){auth.as_ref()}else{None},
          "avatar":if self.page.is_none(){principal.as_ref().and_then(|principal|self.config.avatar_path(principal))}else{None},
          "settings_workspace":(self.page==Some(SettingsPage::SelfImprovement)).then(||self.config.client.gateway_settings().workspace_id),
          "workspace":matches!(self.page,Some(SettingsPage::Memory|SettingsPage::SelfImprovement)).then(||self.config.client.navigation_snapshot().workspace_id().map(str::to_owned)),
          "secret_authority":(self.page==Some(SettingsPage::RemoteAccess)).then(||self.config.client.authorization_connection_generation()),
          "connection":self.config.client.gateway_session().status.as_ref().map(|s|s.connection_state),
        });
        if self.last_projection == projection && self.pending_notification.is_none() {
            return;
        }
        let authority_changed = self.last_projection.get("principal")
            != projection.get("principal")
            || self.last_projection.get("secret_authority") != projection.get("secret_authority");
        self.last_projection = projection;
        if authority_changed && self.page == Some(SettingsPage::RemoteAccess) {
            self.remote_key_authority = self.config.client.authorization_connection_generation();
            self.remote_key
                .update(cx, |input, cx| input.set_value("", window, cx));
        }
        if let Some(message) = self.pending_notification.take() {
            use gpui_kit::component::WindowExt;
            window.push_notification(
                gpui_kit::component::notification::Notification::error(message),
                cx,
            );
        }
        self.gateway = Self::gateway_input(&self.config.client);
        if self.page == Some(SettingsPage::RemoteAccess) {
            let configured = self
                .gateway
                .settings
                .as_ref()
                .is_some_and(|settings| settings.remote_access.has_key);
            if configured != self.remote_key_configured {
                self.remote_key_configured = configured;
                self.remote_key.update(cx, |input, cx| {
                    input.set_placeholder(
                        if configured {
                            t!("settings.remote_access.key_placeholder_configured")
                        } else {
                            t!("settings.remote_access.key_placeholder")
                        }
                        .to_string(),
                        window,
                        cx,
                    )
                });
            }
        }
        self.workspace_id = self
            .config
            .client
            .navigation_snapshot()
            .workspace_id()
            .map(str::to_owned);
        if let Some(page) = self.page {
            if let Some(p) = self
                .config
                .client
                .snapshot(&ClientScope::SettingsPage { page })
                .and_then(|p| {
                    p.typed::<pioneer_client::settings::runtime::SettingsPagePublication>()
                })
            {
                if p.payload().value.is_none() {
                    self.gateway.settings = None;
                }
                self.gateway.settings_loading = p.payload().loading;
                self.gateway.settings_error = p.payload().error.clone();
                if page == SettingsPage::Voice {
                    self.voice_input_action_error = p.payload().error.clone();
                }
                if page == SettingsPage::RemoteAccess
                    && p.payload().input_reset_generation > self.remote_access_key_input_revision
                {
                    self.remote_access_key_input_revision = p.payload().input_reset_generation;
                    self.remote_key
                        .update(cx, |input, cx| input.set_value("", window, cx));
                }
            }
        }
        if let Some(s) = &self.gateway.settings {
            let runtime = if self.page == Some(SettingsPage::Memory) {
                (
                    s.thread_episodic.vector_search.downloaded_bytes,
                    s.thread_episodic.vector_search.total_bytes,
                )
            } else {
                (
                    s.voice_input.runtime.downloaded_bytes,
                    s.voice_input.runtime.total_bytes,
                )
            };
            let value = runtime.1.filter(|total| *total > 0).map_or(0., |total| {
                runtime.0.unwrap_or_default() as f64 / total as f64 * 100.
            }) as f32;
            self.progress
                .update(cx, |progress, cx| progress.set_value(value, cx));
            if self.page == Some(SettingsPage::General) {
                self.config.platform.telemetry(s.general.telemetry_enabled);
            }
        }
        cx.notify();
    }
    fn set_active(&mut self, active: bool, cx: &mut Context<Self>) {
        if self.active == active {
            return;
        }
        self.active = active;
        if self.page.is_none() {
            self._binding.sync_avatar(&self.config, active, cx);
        }
        self.demand = if active {
            self.page
                .map(|page| self.config.client.acquire_settings_page(page))
        } else {
            None
        };
        self.sessions_demand =
            (active && self.page.is_none()).then(|| self.config.client.acquire_auth_sessions());
        for child in self.remote.iter().chain(self.voice.iter()) {
            child.update(cx, |child, cx| child.set_active(active, cx));
        }
    }

    pub fn signal(&self) {
        self._binding.changed.send_replace(());
    }
    pub fn settings_content_view(&self) -> SettingsContentView {
        self.route
    }
    pub fn principal_presentation_capabilities(&self) -> PrincipalPresentationCapabilities {
        self.gateway
            .capability_snapshot
            .as_ref()
            .map(pioneer_client::authorization::principal_presentation_capabilities)
            .unwrap_or_default()
    }
    pub fn active_workspace_id(&self) -> Option<&str> {
        self.workspace_id.as_deref()
    }
    pub fn model_selector_workspace_id(&self) -> String {
        self.config
            .client
            .navigation_snapshot()
            .workspace_id()
            .unwrap_or_default()
            .into()
    }
}
impl Render for SettingsScreenView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(editor) = &self.profile_editor {
            return editor.clone().into_any_element();
        }
        if self.page == Some(SettingsPage::SelfImprovement)
            && self.gateway.settings_workspace_id != self.workspace_id
        {
            return div()
                .p_6()
                .child(t!("settings.loading").to_string())
                .into_any_element();
        }
        if let Some(settings) = &self.gateway.settings {
            if self.page == Some(SettingsPage::RemoteAccess) {
                return self.render_remote_access_setting(
                    settings.remote_access.clone(),
                    self.remote_access_settings_expanded,
                    cx.entity(),
                    window,
                    cx,
                );
            }
            if self.page == Some(SettingsPage::Voice) {
                return self.render_voice_input_setting(
                    settings.voice_input.clone(),
                    self.voice_input_action_error.clone(),
                    cx.entity(),
                    window,
                    cx,
                );
            }
        }
        self.render_settings(window, cx)
    }
}
pub(crate) struct SettingsSidebarView {
    pub config: SettingsConfig,
    pub settings_tree_state: Entity<TreeState>,
    route: SettingsContentView,
    allowed: bool,
    initialized: bool,
    _binding: Arc<SettingsBinding>,
    _task: Task<()>,
}
impl SettingsSidebarView {
    fn new(config: SettingsConfig, cx: &mut App) -> Entity<Self> {
        cx.new(|cx| {
            let binding = SettingsBinding::new(
                vec![
                    ClientScope::Navigation,
                    ClientScope::Administration { workspace_id: None },
                ],
                &config.bindings,
            );
            let mut changed = binding.changed.subscribe();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if view.update(cx, |view, cx| view.sync(cx)).is_err() {
                        break;
                    }
                }
            });
            let mut view = Self {
                route: config.client.navigation_snapshot().settings_route(),
                allowed: false,
                initialized: false,
                config,
                settings_tree_state: cx.new(|cx| TreeState::new(cx)),
                _binding: binding,
                _task: task,
            };
            view.sync(cx);
            view
        })
    }
    fn sync(&mut self, cx: &mut Context<Self>) {
        let route = self.config.client.navigation_snapshot().settings_route();
        let allowed = self
            .config
            .client
            .authorization_snapshot(None, None)
            .is_some_and(|s| s.global.can_manage_gateway_settings);
        if self.initialized && self.route == route && self.allowed == allowed {
            return;
        }
        self.initialized = true;
        self.route = route;
        self.allowed = allowed;
        let mut items = vec![
            (
                SettingsContentView::Account,
                SETTINGS_CONTENT_ACCOUNT_NODE_ID,
            ),
            (
                SettingsContentView::General,
                SETTINGS_CONTENT_GENERAL_NODE_ID,
            ),
        ];
        if self.allowed {
            items.extend([
                (SettingsContentView::Memory, SETTINGS_CONTENT_MEMORY_NODE_ID),
                (
                    SettingsContentView::SelfImprovement,
                    SETTINGS_CONTENT_SELF_IMPROVEMENT_NODE_ID,
                ),
            ]);
        }
        let selected = items.iter().position(|(route, _)| *route == self.route);
        self.settings_tree_state.update(cx, |state, cx| {
            state.set_items(
                items
                    .into_iter()
                    .map(|(_, id)| TreeItem::new(id, id))
                    .collect::<Vec<_>>(),
                cx,
            );
            state.set_selected_index(selected, cx);
        });
        cx.notify();
    }
    pub fn open_settings_content_from_sidebar(
        &mut self,
        route: SettingsContentView,
        _: &mut Context<Self>,
    ) {
        self.config.client.dispatch(ClientIntent::Navigation {
            intent: pioneer_client::navigation::NavigationIntent::SetSettingsRoute { route },
            expected_revision: None,
        });
    }
}
impl Render for SettingsSidebarView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_settings_sidebar(cx)
    }
}

#[cfg(test)]
mod render_tests {
    use super::{SettingsConfig, SettingsContentView, SettingsPage, SettingsScreenView};
    use crate::account::{Native, Registrar};
    use gpui_kit::TestAppContext;
    use pioneer_client::settings::types::{
        GatewaySettingsSnapshot, GatewayThreadEpisodicVectorLocalModelStatus,
        GatewayVoiceInputRuntimePhase,
    };
    use std::{rc::Rc, sync::Arc};
    struct Publications {
        client: Arc<pioneer_client::core::ClientCore>,
        next: std::cell::Cell<usize>,
        routes: std::rc::Rc<
            std::cell::RefCell<
                std::collections::HashMap<
                    usize,
                    (
                        pioneer_client::core::ClientSubscription,
                        std::sync::Weak<dyn pioneer_desktop_foundation::ClientPublicationSink>,
                    ),
                >,
            >,
        >,
    }
    impl Publications {
        fn deliver(&self) {
            let mut deliveries = Vec::new();
            for (subscription, sink) in self.routes.borrow_mut().values_mut() {
                while let Some(event) = subscription.try_next() {
                    let publication = match event {
                        pioneer_client::core::ClientSubscriptionEvent::Publication {
                            publication,
                            ..
                        } => Some(publication),
                        pioneer_client::core::ClientSubscriptionEvent::ResnapshotRequired {
                            scope,
                            ..
                        } => self.client.snapshot(&scope),
                    };
                    if let Some(publication) = publication {
                        deliveries.push((sink.clone(), publication));
                    }
                }
            }
            deliveries.sort_by_key(|(_, publication)| publication.snapshot().sequence());
            for (sink, publication) in deliveries {
                if let Some(sink) = sink.upgrade() {
                    sink.publish(publication);
                }
            }
        }
    }
    impl pioneer_desktop_foundation::ClientBindingRegistrar for Publications {
        fn register(
            &self,
            scope: pioneer_client::core::ClientScope,
            sink: std::sync::Weak<dyn pioneer_desktop_foundation::ClientPublicationSink>,
        ) -> pioneer_desktop_foundation::ClientBindingRegistration {
            let id = self.next.get();
            self.next.set(id + 1);
            self.routes.borrow_mut().insert(
                id,
                (
                    self.client
                        .subscribe(scope, std::num::NonZeroUsize::new(64).unwrap()),
                    sink,
                ),
            );
            let routes = Rc::downgrade(&self.routes);
            pioneer_desktop_foundation::ClientBindingRegistration::new(move || {
                if let Some(routes) = routes.upgrade() {
                    routes.borrow_mut().remove(&id);
                }
            })
        }
    }

    #[derive(Default)]
    struct AvatarRequests(
        std::cell::RefCell<
            Vec<(
                pioneer_client::avatars::AvatarCacheRequest,
                tokio_util::sync::CancellationToken,
            )>,
        >,
    );
    impl crate::SettingsAvatarPort for AvatarRequests {
        fn resolve(
            &self,
            request: pioneer_client::avatars::AvatarCacheRequest,
            cancellation: tokio_util::sync::CancellationToken,
            cx: &mut gpui_kit::App,
        ) -> gpui_kit::Task<
            Result<
                pioneer_client::avatars::AvatarCacheResult,
                pioneer_client::avatars::AvatarCacheError,
            >,
        > {
            use gpui_kit::AppContext;
            self.0.borrow_mut().push((request, cancellation));
            cx.background_spawn(std::future::pending())
        }
    }

    fn account_avatar_replay(cx: &mut TestAppContext, warm: bool) {
        use pioneer_client::catalog_test_support::{
            publish_cached_avatar, set_current_avatar_revision, settings_client,
        };
        cx.update(gpui_kit::init);
        let client = settings_client();
        let principal = client.current_auth().unwrap().principal.id.to_string();
        set_current_avatar_revision(&client, Some("first"));
        if warm {
            publish_cached_avatar(&client, "first", "/tmp/account-first.png");
        }
        #[expect(
            clippy::arc_with_non_send_sync,
            reason = "The GPUI binding API uses Arc registrars on the UI thread."
        )]
        let bindings = Arc::new(Publications {
            client: client.clone(),
            next: Default::default(),
            routes: Default::default(),
        });
        let avatars = Rc::new(AvatarRequests::default());
        let config = SettingsConfig {
            client: client.clone(),
            bindings: bindings.clone(),
            platform: Rc::new(Native),
            photos: Rc::new(Native),
            avatars: avatars.clone(),
        };
        let (root, cx) = cx.add_window_view(|window, cx| {
            let account = SettingsScreenView::new(
                config.clone(),
                SettingsContentView::Account,
                None,
                window,
                cx,
            );
            account.update(cx, |page, cx| page.set_active(true, cx));
            gpui_kit::component::Root::new(account, window, cx)
        });
        let account: gpui_kit::Entity<SettingsScreenView> =
            root.read_with(cx, |root, _| root.view().clone().downcast().unwrap());
        pioneer_client::catalog_test_support::replay_account_requests(&client);
        bindings.deliver();
        cx.run_until_parked();
        assert_eq!(
            avatars.0.borrow().len(),
            usize::from(!warm),
            "Account must fetch only a missing avatar"
        );
        if !warm {
            // Opening and closing the editor during a load must leave Account's request alive.
            account.update_in(cx, |page, window, cx| page.open_profile_editor(window, cx));
            cx.run_until_parked();
            assert_eq!(avatars.0.borrow().len(), 1);
            account.update(cx, |page, _| {
                page.profile_editor = None;
            });
            cx.run_until_parked();
            assert!(!avatars.0.borrow()[0].1.is_cancelled());
            assert!(config.avatar_path(&principal).is_none());
            assert_eq!(avatars.0.borrow()[0].0.principal_id.as_str(), principal);
            publish_cached_avatar(&client, "first", "/tmp/account-first.png");
            bindings.deliver();
            cx.run_until_parked();
        }
        assert_eq!(
            config.avatar_path(&principal),
            Some("/tmp/account-first.png".into())
        );
        assert_eq!(
            account.read_with(cx, |page, _| page.last_projection["avatar"].clone()),
            "/tmp/account-first.png"
        );
        // Profile editor uses the same cache without issuing a second request.
        account.update_in(cx, |page, window, cx| page.open_profile_editor(window, cx));
        cx.run_until_parked();
        assert!(account.read_with(cx, |page, _| page.profile_editor.is_some()));
        assert_eq!(avatars.0.borrow().len(), usize::from(!warm));
        account.update(cx, |page, _| {
            page.profile_editor = None;
        });
        cx.run_until_parked();

        set_current_avatar_revision(&client, Some("second"));
        bindings.deliver();
        cx.run_until_parked();
        assert!(
            config.avatar_path(&principal).is_none(),
            "old avatar must not stand in for a new revision"
        );
        assert_eq!(
            avatars.0.borrow().last().unwrap().0.avatar_revision,
            "second"
        );
        publish_cached_avatar(&client, "first", "/tmp/stale-avatar.png");
        bindings.deliver();
        cx.run_until_parked();
        assert!(config.avatar_path(&principal).is_none());
        publish_cached_avatar(&client, "second", "/tmp/account-second.png");
        bindings.deliver();
        cx.run_until_parked();
        assert_eq!(
            account.read_with(cx, |page, _| page.last_projection["avatar"].clone()),
            "/tmp/account-second.png"
        );
        set_current_avatar_revision(&client, None);
        bindings.deliver();
        cx.run_until_parked();
        assert!(config.avatar_path(&principal).is_none());
        assert!(
            avatars
                .0
                .borrow()
                .iter()
                .all(|(_, token)| token.is_cancelled())
        );
        assert!(account.read_with(cx, |page, _| page.last_projection["avatar"].is_null()));

        set_current_avatar_revision(&client, Some("third"));
        bindings.deliver();
        cx.run_until_parked();
        account.update(cx, |page, cx| page.set_active(false, cx));
        assert!(
            avatars
                .0
                .borrow()
                .iter()
                .all(|(_, token)| token.is_cancelled())
        );
        assert_eq!(config.avatar_path("another-principal"), None);
        client.shutdown();
    }

    #[gpui_kit::test]
    fn account_loads_avatar_without_visiting_members(cx: &mut TestAppContext) {
        account_avatar_replay(cx, false);
    }

    #[gpui_kit::test]
    fn account_reuses_members_avatar_and_observes_replacement(cx: &mut TestAppContext) {
        account_avatar_replay(cx, true);
    }

    #[gpui_kit::test]
    fn account_done_reloads_devices_through_retained_surface_demand(cx: &mut TestAppContext) {
        use gpui_kit::Entity;
        use pioneer_client::{
            catalog_test_support::{
                replay_account_requests, revalidate_saved_profile, settings_client,
            },
            settings::profile::ProfileIntent,
        };
        cx.update(gpui_kit::init);
        let client = settings_client();
        let bindings = Arc::new(Publications {
            client: client.clone(),
            next: Default::default(),
            routes: Default::default(),
        });
        let config = SettingsConfig {
            client: client.clone(),
            bindings: bindings.clone(),
            platform: Rc::new(Native),
            photos: Rc::new(Native),
            avatars: Rc::new(Native),
        };
        let (root, cx) = cx.add_window_view(|window, cx| {
            let view = super::SettingsView::new(config, window, cx);
            view.update(cx, |view, cx| view.set_active(true, cx));
            gpui_kit::component::Root::new(view, window, cx)
        });
        let view: Entity<super::SettingsView> =
            root.read_with(cx, |root, _| root.view().clone().downcast().unwrap());
        let account = view.read_with(cx, |view, cx| {
            view.pages
                .iter()
                .find(|page| page.read(cx).route == SettingsContentView::Account)
                .unwrap()
                .clone()
        });
        assert_eq!(replay_account_requests(&client), (1, 0));
        bindings.deliver();
        cx.run_until_parked();
        assert_eq!(
            account.read_with(cx, |page, _| page.gateway.auth_sessions.sessions.len()),
            1
        );

        for name in ["First", "Second"] {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            let card = cx
                .debug_bounds("settings-current-principal")
                .unwrap()
                .center();
            cx.simulate_click(card, Default::default());
            cx.run_until_parked();
            assert!(account.read_with(cx, |page, _| page.profile_editor.is_some()));
            client.profile_intent(ProfileIntent::EditName {
                first_name: name.into(),
                last_name: "User".into(),
            });
            bindings.deliver();
            cx.run_until_parked();
            cx.update(|window, cx| window.draw(cx).clear(cx));
            let done = cx.debug_bounds("profile-editor-done").unwrap().center();
            cx.simulate_click(done, Default::default());
            assert!(client.profile().pending, "Done did not dispatch the save");
            assert_eq!(replay_account_requests(&client), (0, 1));
            if name == "First" {
                bindings.deliver();
                cx.run_until_parked();
                assert!(!account.read_with(cx, |page, _| page.profile_editor.is_some()));
            }
            // Gateway's post-save policy/member notifications evict sessions.
            // The same Account entity stays mounted; the test supplies no scope demand.
            revalidate_saved_profile(&client);
            bindings.deliver();
            cx.run_until_parked();
            assert_eq!(
                replay_account_requests(&client),
                (1, 0),
                "mounted Account lost device demand after Done"
            );
            assert!(!account.read_with(cx, |page, _| page.profile_editor.is_some()));
            bindings.deliver();
            cx.run_until_parked();
            assert_eq!(
                account.read_with(cx, |page, _| page.gateway.auth_sessions.sessions.len()),
                1
            );
            assert_eq!(
                client.current_auth().unwrap().principal.display_name,
                format!("{name} User")
            );
            cx.update(|window, cx| window.draw(cx).clear(cx));
            let row = cx
                .debug_bounds("devices-session-action-SAAAAAAAAAAAAAAAAAAAA")
                .expect("device action missing from rendered Account");
            assert!(row.size.width > gpui_kit::px(0.) && row.size.height > gpui_kit::px(0.));
            assert_eq!(replay_account_requests(&client), (0, 0));
        }
        view.update(cx, |view, cx| view.set_active(false, cx));
        revalidate_saved_profile(&client);
        assert_eq!(
            replay_account_requests(&client),
            (0, 0),
            "hidden Account still requests devices"
        );
        view.update(cx, |view, cx| view.set_active(true, cx));
        assert_eq!(replay_account_requests(&client), (1, 0));
        client.shutdown();
    }

    #[gpui_kit::test]
    fn settings_forms_render_retained_inputs_and_both_download_indicators(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        for (route, page, marker) in [
            (
                SettingsContentView::General,
                None,
                "settings-file-opener-form",
            ),
            (
                SettingsContentView::General,
                Some(SettingsPage::RemoteAccess),
                "settings-remote-access-form",
            ),
            (
                SettingsContentView::General,
                Some(SettingsPage::Voice),
                "settings-voice-input-download-progress",
            ),
            (
                SettingsContentView::Memory,
                None,
                "settings-vector-search-download-progress",
            ),
        ] {
            let config = SettingsConfig {
                client: pioneer_client::catalog_test_support::settings_model_picker_client(),
                bindings: Arc::new(Registrar(Arc::new(std::sync::atomic::AtomicUsize::new(0)))),
                platform: Rc::new(Native),
                photos: Rc::new(Native),
                avatars: Rc::new(Native),
            };
            let (_, window_cx) = cx.add_window_view(|window, cx| {
                let view = SettingsScreenView::new(config, route, page, window, cx);
                view.update(cx, |view, _| {
                    let mut settings = GatewaySettingsSnapshot {
                        general: Default::default(),
                        memory: Default::default(),
                        self_improvement: Default::default(),
                        self_improvement_status: None,
                        thread_episodic: Default::default(),
                        cli_runtimes: Default::default(),
                        remote_access: Default::default(),
                        voice_input: Default::default(),
                    };
                    settings.voice_input.enabled = true;
                    settings.voice_input.runtime.phase = GatewayVoiceInputRuntimePhase::Downloading;
                    settings.voice_input.runtime.downloaded_bytes = Some(10);
                    settings.voice_input.runtime.total_bytes = Some(20);
                    settings.thread_episodic.vector_search.enabled = true;
                    settings.thread_episodic.vector_search.local_model_status =
                        GatewayThreadEpisodicVectorLocalModelStatus::Downloading;
                    settings.thread_episodic.vector_search.downloaded_bytes = Some(10);
                    settings.thread_episodic.vector_search.total_bytes = Some(20);
                    view.gateway.settings = Some(settings);
                    view.remote_access_settings_expanded = true;
                });
                gpui_kit::component::Root::new(view, window, cx)
            });
            window_cx.update(|window, cx| window.draw(cx).clear(cx));
            assert!(
                window_cx.debug_bounds(marker).is_some(),
                "missing rendered {marker}"
            );
        }
    }
}
