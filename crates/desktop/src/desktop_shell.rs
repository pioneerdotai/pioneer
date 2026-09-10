//! Window composition through retained capability roots.
use crate::desktop_navigation::*;
use crate::{desktop_navigation::DesktopNavigationStore, shell_state::ShellStateStore};
use crate::{settings::WindowThemePreference, window};
use gpui_kit::component::{
    Icon, Sizable,
    button::{Button, ButtonVariants},
    h_flex,
    separator::Separator,
    theme::{Theme, ThemeMode},
};
use gpui_kit::component::{
    Root, WindowExt,
    resizable::{h_resizable, resizable_panel},
    theme::ActiveTheme,
    v_flex,
};
use gpui_kit::{prelude::*, *};
use std::sync::Arc;

struct DesktopUpdateApplyFailedNotification;

fn thread_view_target(
    navigation: &DesktopNavigationStore,
    client: &pioneer_client::core::ClientCore,
    window_active: bool,
) -> Option<String> {
    if navigation.activity(MainRoute::Threads, window_active) == RouteActivity::Dormant {
        return None;
    }
    navigation
        .snapshot()
        .navigation()
        .active_thread_id()
        .map(str::to_owned)
        .or_else(|| {
            navigation
                .is_visible(MainRoute::Threads)
                .then(|| client.prepare_thread_draft())
                .flatten()
        })
}

pub(crate) struct DesktopShellView {
    thread: Option<(String, Entity<pioneer_desktop_thread::ThreadView>)>,
    thread_events: Option<Subscription>,
    workspaces: Option<Entity<pioneer_desktop_workspaces::WorkspaceNavigationView>>,
    _workspace_events: Subscription,
    _task_notification_events: Subscription,
    _onboarding_events: Subscription,
    identity: Option<Arc<crate::gateway::IdentityAuthorizationBinding>>,
    identity_task: Option<Task<()>>,
    can_manage: bool,
    can_notify: bool,
    setup_required: bool,
    gateway_switcher: Option<AnyView>,
    general_actions: Option<AnyView>,
    onboarding: Option<Entity<pioneer_desktop_onboarding::OnboardingView>>,
    settings: Option<Entity<pioneer_desktop_settings::SettingsView>>,
    administration: Option<Entity<pioneer_desktop_administration::AdministrationView>>,
    providers: Option<Entity<pioneer_desktop_providers::ProviderCatalogView>>,
    mcp: Option<Entity<pioneer_desktop_mcp::McpCatalogView>>,
    skills: Option<Entity<pioneer_desktop_skills::SkillsCatalogView>>,
    agents_document: Option<Entity<pioneer_desktop_agents_doc::AgentsDocumentEditor>>,
    desktop_update: Option<Entity<pioneer_desktop_update::DesktopUpdateView>>,
    task_notifications: Option<Entity<pioneer_desktop_task_notifications::TaskNotificationView>>,
    action_region: FocusHandle,
    sidebar: Option<Entity<SidebarHostView>>,
    navigation: Arc<DesktopNavigationStore>,
    layout: Entity<ShellStateStore>,
    _layout_subscription: Subscription,
    _activation_subscription: Subscription,
    route_task: Option<Task<()>>,
    close_task: Option<Task<()>>,
    mounted_route: Arc<DesktopRouteSnapshot>,
}
impl DesktopShellView {
    fn selected_screen(&self) -> Option<AnyView> {
        let route = self.navigation.snapshot();
        if route.window_route() != WindowRoute::Main {
            self.onboarding.as_ref().map(|root| root.clone().into())
        } else {
            match route.route() {
                MainRoute::Threads => self.thread.as_ref().map(|(_, root)| root.clone().into()),
                MainRoute::AgentsDoc => self
                    .agents_document
                    .as_ref()
                    .map(|root| root.clone().into()),
                MainRoute::Providers => self.providers.as_ref().map(|root| root.clone().into()),
                MainRoute::Administration => {
                    self.administration.as_ref().map(|root| root.clone().into())
                }
                MainRoute::Mcp | MainRoute::McpDetails => {
                    self.mcp.as_ref().map(|root| root.clone().into())
                }
                MainRoute::Skills | MainRoute::SkillDetails => {
                    self.skills.as_ref().map(|root| root.clone().into())
                }
                MainRoute::Settings => self.settings.as_ref().map(|root| root.clone().into()),
            }
        }
    }

    fn sync_sidebar_width(&self, cx: &mut App) {
        let width = if self.layout.read(cx).sidebar_visible() {
            self.layout.read(cx).sidebar_width()
        } else {
            px(0.)
        };
        if let Some(view) = &self.providers {
            view.update(cx, |view, cx| view.set_sidebar_width(width, cx));
        }
        if let Some(view) = &self.mcp {
            view.update(cx, |view, cx| view.set_sidebar_width(width, cx));
        }
        if let Some(view) = &self.skills {
            view.update(cx, |view, cx| view.set_sidebar_width(width, cx));
        }
    }
    fn sync_activity(&self, active: bool, cx: &mut App) {
        let active = active && self.navigation.snapshot().window_route() == WindowRoute::Main;
        if let Some(view) = &self.providers {
            view.update(cx, |view, cx| view.set_window_active(active, cx));
        }
        if let Some(view) = &self.administration {
            view.update(cx, |view, cx| view.set_window_active(active, cx));
        }
        if let Some(view) = &self.mcp {
            view.update(cx, |view, cx| view.set_window_active(active, cx));
        }
        if let Some(view) = &self.skills {
            view.update(cx, |view, cx| view.set_window_active(active, cx));
        }
        if let Some(view) = &self.settings {
            view.update(cx, |view, cx| {
                view.set_active(
                    active && self.navigation.is_visible(MainRoute::Settings),
                    cx,
                )
            });
        }
    }
    fn sync_chrome(&mut self, cx: &mut Context<Self>) {
        let route = self.navigation.snapshot();
        let capabilities = self
            .identity
            .as_ref()
            .and_then(|binding| binding.publication())
            .and_then(|p| {
                p.capabilities
                    .snapshot(route.navigation().workspace_id(), None)
                    .or_else(|| p.capabilities.snapshot(None, None))
            })
            .map(|p| pioneer_client::authorization::principal_presentation_capabilities(&p))
            .unwrap_or_default();
        let next = (
            capabilities.can_manage_capabilities,
            capabilities.can_read_own_notifications,
        );
        if (self.can_manage, self.can_notify) != next {
            (self.can_manage, self.can_notify) = next;
            cx.notify();
        }
    }
    fn open_agents_document(
        &mut self,
        scope: pioneer_client::agents_doc::scope::AgentsDocEditorScope,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .core()
            .navigate(
                pioneer_client::navigation::NavigationIntent::OpenAgentsDocument { scope },
                None,
            );
        crate::client_runtime::DesktopRuntimeCoordinator::deliver_pending(cx);
        self.mount_agents_document(window, cx);
    }
    fn mount_agents_document(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let route = self.navigation.snapshot();
        let scope = route.navigation().agents_document_scope();
        if self
            .agents_document
            .as_ref()
            .map(|view| view.read(cx).scope())
            == scope
        {
            return;
        }
        if let Some(view) = self.agents_document.take() {
            view.update(cx, |view, cx| view.flush_pending_save(window, cx));
        }
        if let Some(scope) = scope {
            let runtime = cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>();
            self.agents_document = Some(pioneer_desktop_agents_doc::AgentsDocumentEditor::new(
                pioneer_desktop_agents_doc::AgentsDocumentConfig::new(
                    runtime.core(),
                    runtime.registrar(),
                    scope.clone(),
                ),
                window,
                cx,
            ));
        }
    }
    pub(crate) fn handle_invitation_url(
        &mut self,
        uri: String,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Ok(presentation) = pioneer_protocol::InvitationPresentation::parse(&uri) else {
            return;
        };
        if presentation.app_url_scheme()
            != pioneer_protocol::PioneerAppUrlScheme::for_current_build()
        {
            return;
        }
        cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .core()
            .onboarding_intent(
                pioneer_client::gateway::onboarding_runtime::OnboardingIntent::Invitation {
                    intent:
                        pioneer_client::gateway::invitation_controller::InvitationIntent::Open {
                            uri: pioneer_protocol::AuthSecretString::new(uri),
                        },
                },
            );
    }
    fn title_bar(&self, cx: &mut App) -> AnyElement {
        let invitation_active = self.navigation.snapshot().window_route()
            == crate::desktop_navigation::WindowRoute::InvitationJoin;

        let theme_icon = if cx.theme().mode.is_dark() {
            gpui_kit::component::IconName::Sun
        } else {
            gpui_kit::component::IconName::Moon
        };

        let is_gateway_setup_required = self.setup_required;
        let show_gateway_switcher = !invitation_active;
        let show_task_notifications = !is_gateway_setup_required && self.can_notify;

        gpui_kit::component::TitleBar::new()
            .child(
                h_flex()
                    .w_full()
                    .pr_4()
                    .justify_between()
                    .items_center()
                    .bg(cx.theme().title_bar)
                    .child(
                        h_flex()
                            .h_full()
                            .items_center()
                            .child(if show_gateway_switcher {
                                self.gateway_switcher
                                    .as_ref()
                                    .expect("mounted gateway switcher")
                                    .clone()
                                    .into_any_element()
                            } else {
                                div().into_any_element()
                            }),
                    )
                    .child(
                        h_flex()
                            .h_full()
                            .gap_1()
                            .items_center()
                            .child(if show_task_notifications {
                                div()
                                    .children(self.task_notifications.clone())
                                    .into_any_element()
                            } else {
                                div().into_any_element()
                            })
                            .child(if show_task_notifications {
                                Separator::vertical().h_4().mx_0p5().into_any_element()
                            } else {
                                div().into_any_element()
                            })
                            .child(
                                h_flex()
                                    .gap_1()
                                    .child(if !is_gateway_setup_required {
                                        self.general_actions
                                            .as_ref()
                                            .expect("mounted general actions")
                                            .clone()
                                            .into_any_element()
                                    } else {
                                        div().into_any_element()
                                    })
                                    .child(
                                        Button::new("toggle-theme")
                                            .ghost()
                                            .small()
                                            .compact()
                                            .child(Icon::new(theme_icon).size_3p5().opacity(0.6))
                                            .on_click(|_, window, cx| {
                                                let (mode, theme_preference) = if cx
                                                    .theme()
                                                    .mode
                                                    .is_dark()
                                                {
                                                    (ThemeMode::Light, WindowThemePreference::Light)
                                                } else {
                                                    (ThemeMode::Dark, WindowThemePreference::Dark)
                                                };
                                                Theme::change(mode, Some(window), cx);
                                                window::persist_theme_preference(
                                                    window,
                                                    theme_preference,
                                                    cx,
                                                );
                                            }),
                                    ),
                            ),
                    ),
            )
            .into_any_element()
    }
    pub(crate) fn new(
        navigation: Arc<DesktopNavigationStore>,
        layout: Entity<ShellStateStore>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let (client, registrar) = {
            let runtime = cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>();
            (runtime.core(), runtime.registrar())
        };
        let onboarding = pioneer_desktop_onboarding::OnboardingView::new(
            pioneer_desktop_onboarding::OnboardingConfig {
                client: client.clone(),
                bindings: registrar.clone(),
                photos: std::rc::Rc::new(crate::profile_photo::DesktopProfilePhotoPort),
            },
            window,
            cx,
        );
        let onboarding_events = cx.subscribe(&onboarding, |view, _, event, cx| match event {
            pioneer_desktop_onboarding::OnboardingEvent::NavigationChanged {
                invitation_active,
                setup_required,
            } => {
                let changed = view.setup_required != *setup_required;
                view.setup_required = *setup_required;
                if changed {
                    cx.notify();
                }
                view.navigation.set_window_route(if *invitation_active {
                    WindowRoute::InvitationJoin
                } else if *setup_required {
                    WindowRoute::GatewaySetup
                } else {
                    WindowRoute::Main
                });
            }
            pioneer_desktop_onboarding::OnboardingEvent::Authenticated { .. } => {}
        });
        let settings = pioneer_desktop_settings::SettingsView::new(
            crate::platform::settings::settings_config(client.clone(), cx),
            window,
            cx,
        );
        let providers = pioneer_desktop_providers::ProviderCatalogView::new(
            crate::platform::providers::provider_config(client.clone(), cx),
            window,
            cx,
        );
        let administration = pioneer_desktop_administration::AdministrationView::new(
            crate::platform::administration::administration_config(client.clone(), cx),
            window,
            cx,
        );
        let mcp = pioneer_desktop_mcp::McpCatalogView::new(
            pioneer_desktop_mcp::McpCatalogConfig::new(client.clone(), registrar.clone()),
            window,
            cx,
        );
        let skills = pioneer_desktop_skills::SkillsCatalogView::new(
            pioneer_desktop_skills::SkillsCatalogConfig::new(client.clone(), registrar.clone()),
            window,
            cx,
        );
        let identity = crate::gateway::IdentityAuthorizationBinding::new(registrar.as_ref());
        let mut identity_changes = identity.watch();
        let identity_task = cx.spawn(async move |view, cx| {
            while identity_changes.changed().await.is_ok() {
                if view.update(cx, |view, cx| view.sync_chrome(cx)).is_err() {
                    break;
                }
            }
        });
        let config = pioneer_desktop_task_notifications::TaskNotificationConfig::new(
            client.clone(),
            registrar.clone(),
            pioneer_desktop_task_notifications::TaskNotificationLabels::new(
                t!("tasks.notifications.title").to_string(),
                t!("tasks.notifications.task").to_string(),
                t!("tasks.notifications.loading").to_string(),
                t!("tasks.notifications.empty").to_string(),
                t!("tasks.notifications.completed").to_string(),
                t!("tasks.notifications.mark_read").to_string(),
            ),
        );
        let task_notifications =
            cx.new(|cx| pioneer_desktop_task_notifications::TaskNotificationView::new(config, cx));
        let task_notification_events =
            cx.subscribe_in(&task_notifications, window, |view, _, event, window, cx| {
                let pioneer_desktop_task_notifications::TaskNotificationEvent::OpenThread {
                    ..
                } = event;
                crate::client_runtime::DesktopRuntimeCoordinator::deliver_pending(cx);
                view.mount_thread(window, cx);
            });
        let desktop_update = crate::state::runtime_home_dir().ok().map(|runtime_home| {
            let config = pioneer_desktop_update::DesktopUpdateConfig::new(
                runtime_home,
                Arc::new(pioneer_desktop_update::NativeDesktopUpdatePort),
                t!("desktop_update.ready_title").to_string().into(),
                t!("desktop_update.downloading").to_string().into(),
                "icons/leaf.svg".into(),
                |details, window, cx| {
                    use gpui_kit::component::notification::{Notification, NotificationType};
                    let message =
                        t!("desktop_update.apply_failed", error = details.as_str()).to_string();
                    window.push_notification(
                        Notification::new()
                            .with_type(NotificationType::Warning)
                            .id1::<DesktopUpdateApplyFailedNotification>((
                                "desktop-update-apply-failed",
                                0u64,
                            ))
                            .content(move |_, _, _| {
                                v_flex()
                                    .child(
                                        div()
                                            .text_sm()
                                            .opacity(0.8)
                                            .line_height(relative(1.4))
                                            .whitespace_normal()
                                            .child(message.clone()),
                                    )
                                    .into_any_element()
                            }),
                        cx,
                    );
                },
            );
            let view = cx.new(|cx| pioneer_desktop_update::DesktopUpdateView::new(config, cx));
            view
        });
        let workspace_config = pioneer_desktop_workspaces::WorkspaceNavigationConfig::new(
            client.clone(),
            registrar.clone(),
            |workspace, cx| {
                crate::state::thread_folders_expanded_for_workspace(cx, Some(workspace))
            },
            |workspace, expansion, cx| {
                let _ = crate::state::set_thread_folders_expanded_for_workspace(
                    cx, workspace, expansion,
                );
            },
            |builder, window, cx| {
                window.open_dialog(cx, move |dialog, window, cx| builder(dialog, window, cx))
            },
            |builder, window, cx| {
                window.open_dialog(cx, move |dialog, window, cx| builder(dialog, window, cx))
            },
            |builder, window, cx| {
                window.open_dialog(cx, move |dialog, window, cx| builder(dialog, window, cx))
            },
            |builder, window, cx| {
                window.open_dialog(cx, move |dialog, window, cx| builder(dialog, window, cx))
            },
        );
        let workspaces = cx.new(|cx| {
            pioneer_desktop_workspaces::WorkspaceNavigationView::new(workspace_config, cx)
        });
        let workspace_events = cx.subscribe_in(
            &workspaces,
            window,
            |view, _, event, window, cx| match event {
                pioneer_desktop_workspaces::WorkspaceNavigationEvent::OpenThread { .. } => {
                    view.mount_thread(window, cx)
                }
                pioneer_desktop_workspaces::WorkspaceNavigationEvent::OpenAgentsDocument {
                    scope,
                } => view.open_agents_document(scope.clone(), window, cx),
            },
        );
        navigation.set_window_route(if client.onboarding_setup_required() {
            WindowRoute::GatewaySetup
        } else {
            WindowRoute::Main
        });
        let sidebar = cx.new(|cx| SidebarHostView {
            workspaces: workspaces.clone(),
            desktop_update: desktop_update.clone(),
            navigation: navigation.clone(),
            settings: settings.read(cx).sidebar_surface(),
            providers: providers.read(cx).sidebar(),
            administration: administration.read(cx).sidebar(),
            mcp: mcp.read(cx).sidebar(),
            skills: skills.read(cx).sidebar(),
        });
        let layout_subscription = cx.observe(&layout, |view, _, cx| {
            view.sync_sidebar_width(cx);
            cx.notify();
        });
        let mut changes = navigation.watch();
        let route_task = cx.spawn_in(window, async move |view, cx| {
            while changes.changed().await.is_ok() {
                if view
                    .update_in(cx, |view, window, cx| {
                        let route = view.navigation.snapshot();
                        let previous = &view.mounted_route;
                        let route_changed = previous.navigation().destination()
                            != route.navigation().destination()
                            || previous.route() != route.route()
                            || previous.window_route() != route.window_route()
                            || previous.navigation().workspace_id()
                                != route.navigation().workspace_id()
                            || (route.route() == MainRoute::Threads
                                && previous.navigation().active_thread_id()
                                    != route.navigation().active_thread_id());
                        if route_changed {
                            window.close_all_dialogs(cx);
                            window.close_sheet(cx);
                        }
                        view.mounted_route = route;
                        if let Some(sidebar) = &view.sidebar {
                            sidebar.update(cx, |_, cx| cx.notify());
                        }
                        view.sync_chrome(cx);
                        view.mount_agents_document(window, cx);
                        view.sync_activity(window.is_window_active(), cx);
                        view.mount_thread(window, cx);
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let activation_subscription = cx.observe_window_activation(window, |view, window, cx| {
            if let Some((_, thread)) = &view.thread {
                thread.update(cx, |thread, cx| {
                    thread.set_window_active(window.is_window_active(), cx)
                });
            }
            view.sync_activity(window.is_window_active(), cx);
        });
        let shell = cx.weak_entity();
        window.on_window_should_close(cx, move |window, cx| {
            shell
                .update(cx, |view, cx| view.request_close(window, cx))
                .unwrap_or(true)
        });
        let gateway_switcher = Some(onboarding.read(cx).gateway_switcher_surface());
        let general_actions = Some(settings.read(cx).general_actions_surface());
        let mut view = Self {
            thread: None,
            thread_events: None,
            workspaces: Some(workspaces),
            _workspace_events: workspace_events,
            _task_notification_events: task_notification_events,
            _onboarding_events: onboarding_events,
            identity: Some(identity),
            identity_task: Some(identity_task),
            can_manage: false,
            can_notify: false,
            setup_required: client.onboarding_setup_required(),
            gateway_switcher,
            general_actions,
            onboarding: Some(onboarding),
            settings: Some(settings),
            providers: Some(providers),
            administration: Some(administration),
            mcp: Some(mcp),
            skills: Some(skills),
            agents_document: None,
            desktop_update,
            task_notifications: Some(task_notifications),
            action_region: cx.focus_handle(),
            mounted_route: navigation.snapshot(),
            sidebar: Some(sidebar),
            navigation,
            layout,
            _layout_subscription: layout_subscription,
            _activation_subscription: activation_subscription,
            route_task: Some(route_task),
            close_task: None,
        };
        view.sync_sidebar_width(cx);
        view.sync_activity(window.is_window_active(), cx);
        view.sync_chrome(cx);
        view.mount_agents_document(window, cx);
        view.mount_thread(window, cx);
        view
    }
    fn mount_thread(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let visible = self.navigation.is_visible(MainRoute::Threads);
        let (client, registrar) = {
            let runtime = cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>();
            (runtime.core(), runtime.registrar())
        };
        let target = thread_view_target(&self.navigation, &client, window.is_window_active());
        if self.thread.as_ref().map(|(id, _)| id) == target.as_ref() {
            if let Some((_, thread)) = &self.thread {
                thread.update(cx, |thread, cx| thread.set_visible(visible, window, cx));
            }
            return;
        }
        self.thread_events.take();
        self.thread.take();
        let Some(thread_id) = target.filter(|_| visible) else {
            return;
        };
        let Ok(runtime_root) = crate::state::runtime_home_dir() else {
            return;
        };
        let config = pioneer_desktop_thread::ThreadViewConfig::new(
            client.clone(),
            thread_id.to_owned(),
            registrar,
            Arc::new(crate::thread_platform::DesktopThreadFilePort::new(
                client.clone(),
                pioneer_client::platform::ClientPath::new(runtime_root),
            )),
            Arc::new(crate::audio::thread_port::DesktopThreadAudioPort::new(
                client,
            )),
            Arc::new(crate::thread_platform::DesktopThreadExternalNavigationPort),
        );
        let thread = pioneer_desktop_thread::ThreadView::new(config, window, cx);
        self.thread_events = Some(cx.subscribe_in(
            &thread,
            window,
            |view, _, event: &pioneer_desktop_thread::ThreadNavigationEvent, window, cx| {
                use pioneer_client::navigation::{
                    NavigationIntent, SemanticDestination, TaskThreadLineage,
                };
                let core = cx
                    .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                    .core();
                match event {
                    pioneer_desktop_thread::ThreadNavigationEvent::OpenTaskThread {
                        parent_thread_id,
                        child_thread_id,
                        title,
                    } => {
                        if let Some(workspace) = core.navigation_snapshot().workspace_id() {
                            core.navigate(
                                NavigationIntent::PushTaskThread {
                                    entry: TaskThreadLineage::new(
                                        parent_thread_id.clone(),
                                        child_thread_id.clone(),
                                        workspace.to_owned(),
                                        title.clone(),
                                    ),
                                },
                                None,
                            );
                        }
                    }
                    pioneer_desktop_thread::ThreadNavigationEvent::CloseTaskThread => {
                        core.navigate(NavigationIntent::PopTaskThread, None);
                    }
                    pioneer_desktop_thread::ThreadNavigationEvent::OpenMcpServer { server_id } => {
                        core.navigate(
                            NavigationIntent::Navigate {
                                destination: SemanticDestination::Mcp {
                                    server_id: Some(server_id.clone()),
                                },
                            },
                            None,
                        );
                    }
                }
                crate::client_runtime::DesktopRuntimeCoordinator::deliver_pending(cx);
                view.mount_thread(window, cx);
            },
        ));
        self.thread = Some((thread_id.to_owned(), thread));
    }
    pub(crate) fn start_desktop_update(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(update) = &self.desktop_update {
            update.update(cx, |view, cx| view.check(window, cx));
        }
    }
    fn activate_navigation(&mut self, route: MainRoute, cx: &mut Context<Self>) {
        use pioneer_client::navigation::{NavigationIntent, SemanticDestination, SettingsRoute};
        let current = self.navigation.snapshot();
        if current.route() == route
            || (route == MainRoute::Threads && current.route() == MainRoute::AgentsDoc)
        {
            self.layout
                .update(cx, |layout, cx| layout.toggle_sidebar(cx));
            return;
        }
        let destination = match route {
            MainRoute::Threads => SemanticDestination::Threads,
            MainRoute::Providers => SemanticDestination::Providers {
                filter: current.navigation().providers_route(),
            },
            MainRoute::Administration => SemanticDestination::Administration {
                route: current.navigation().administration_route(),
            },
            MainRoute::Settings => SemanticDestination::Settings {
                route: SettingsRoute::Account,
            },
            MainRoute::Mcp if self.can_manage => SemanticDestination::Mcp { server_id: None },
            MainRoute::Skills if self.can_manage => SemanticDestination::Skills { skill_id: None },
            _ => return,
        };
        cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .core()
            .navigate(NavigationIntent::Navigate { destination }, None);
        crate::client_runtime::DesktopRuntimeCoordinator::deliver_pending(cx);
    }

    fn request_close(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.close_task.is_some() {
            return false;
        }
        let last_window = cx.windows().len() == 1;
        let scope = (!last_window)
            .then(|| {
                self.agents_document
                    .as_ref()
                    .map(|editor| editor.read(cx).scope().clone())
            })
            .flatten();
        if scope.is_none() && !last_window {
            self.close(window, cx);
            return true;
        }
        let core = cx
            .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .core();
        let handle = window.window_handle();
        self.close_task = Some(cx.spawn(async move |shell, cx| {
            let result = core
                .flush_agents_documents_before_close(scope.clone())
                .await;
            let _ = cx.update_window(handle, |_, window, cx| {
                shell.update(cx, |shell, cx| {
                    shell.close_task.take();
                    match result.and_then(|_| core.agents_documents_close_status(scope.as_ref())) {
                        Ok(true) => {
                            shell.close(window, cx);
                            window.remove_window();
                        }
                        Ok(false) => {
                            shell.request_close(window, cx);
                        }
                        Err(error) => shell.present_document_close_error(&error, window, cx),
                    }
                })
            });
        }));
        false
    }
    pub(crate) fn present_document_close_error(
        &mut self,
        error: &pioneer_client::agents_doc::controller::AgentsDocumentCloseError,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use pioneer_client::agents_doc::controller::AgentsDocumentCloseError;
        if let AgentsDocumentCloseError::SaveFailed(scope)
        | AgentsDocumentCloseError::Conflict(scope) = error
        {
            self.open_agents_document(scope.clone(), window, cx);
        }
        pioneer_desktop_agents_doc::present_close_error(error, window, cx);
    }

    pub(crate) fn close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.close_task.take();
        window.close_all_dialogs(cx);
        window.close_sheet(cx);
        self.layout
            .update(cx, |layout, cx| layout.persist(window, cx));
        self.route_task.take();
        self.thread_events.take();
        self.thread.take();
        self.identity_task.take();
        self.identity.take();
        self.gateway_switcher.take();
        self.general_actions.take();
        self.sync_activity(false, cx);
        self.agents_document.take();
        self.onboarding.take();
        self.settings.take();
        self.providers.take();
        self.administration.take();
        self.mcp.take();
        self.skills.take();
        if let Some(view) = self.task_notifications.take() {
            view.update(cx, |view, _| view.close());
        }
        if let Some(view) = self.desktop_update.take() {
            view.update(cx, |view, _| view.close());
        }
        if let Some(view) = self.workspaces.take() {
            view.update(cx, |view, cx| view.close(cx));
        }
        self.navigation.close();
        self.sidebar.take();
    }
}
impl Render for DesktopShellView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.onboarding.is_none() {
            return div().into_any_element();
        }
        let route = self.navigation.snapshot();
        let title = TitleBarSurface {
            content: self.title_bar(cx),
        };
        let selected = self.selected_screen();
        let thread_mounted = route.route() == MainRoute::Threads && self.thread.is_some();
        let screen = ScreenHostView { selected };
        let body = if route.window_route() == WindowRoute::Main {
            DesktopBody {
                sidebar: self
                    .sidebar
                    .as_ref()
                    .expect("live sidebar")
                    .clone()
                    .into_any_element(),
                thread_mounted,
                screen: screen.into_any_element(),
                bottom: BottomBarSurface {
                    content: crate::desktop_chrome::bottom_bar(
                        route.route(),
                        self.can_manage,
                        &self.action_region,
                        cx,
                    ),
                }
                .into_any_element(),
                layout: self.layout.clone(),
            }
            .into_any_element()
        } else {
            screen.into_any_element()
        };
        let sheets = Root::render_sheet_layer(window, cx);
        let dialogs = Root::render_dialog_layer(window, cx);
        let notifications = Root::render_notification_layer(window, cx);
        div()
            .size_full()
            .track_focus(&self.action_region)
            .key_context("DesktopShell")
            .on_action(cx.listener(|view, _: &OpenThreads, _, cx| {
                view.activate_navigation(MainRoute::Threads, cx)
            }))
            .on_action(cx.listener(|view, _: &OpenProviders, _, cx| {
                view.activate_navigation(MainRoute::Providers, cx)
            }))
            .on_action(
                cx.listener(|view, _: &OpenMcp, _, cx| {
                    view.activate_navigation(MainRoute::Mcp, cx)
                }),
            )
            .on_action(cx.listener(|view, _: &OpenSkills, _, cx| {
                view.activate_navigation(MainRoute::Skills, cx)
            }))
            .on_action(cx.listener(|view, _: &OpenAdministration, _, cx| {
                view.activate_navigation(MainRoute::Administration, cx)
            }))
            .on_action(cx.listener(|view, _: &OpenSettings, _, cx| {
                view.activate_navigation(MainRoute::Settings, cx)
            }))
            .child(
                v_flex().size_full().child(title).child(
                    div()
                        .flex_1()
                        .min_h_0()
                        .w_full()
                        .overflow_hidden()
                        .child(body),
                ),
            )
            .children(sheets)
            .children(dialogs)
            .children(notifications)
            .into_any_element()
    }
}
#[derive(IntoElement)]
struct TitleBarSurface {
    content: AnyElement,
}
impl RenderOnce for TitleBarSurface {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        self.content
    }
}
#[derive(IntoElement)]
struct BottomBarSurface {
    content: AnyElement,
}
impl RenderOnce for BottomBarSurface {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        self.content
    }
}
#[derive(IntoElement)]
struct ScreenHostView {
    selected: Option<AnyView>,
}
impl RenderOnce for ScreenHostView {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        div().size_full().children(self.selected)
    }
}
struct SidebarHostView {
    workspaces: Entity<pioneer_desktop_workspaces::WorkspaceNavigationView>,
    desktop_update: Option<Entity<pioneer_desktop_update::DesktopUpdateView>>,
    navigation: Arc<DesktopNavigationStore>,
    settings: AnyView,
    providers: AnyView,
    administration: AnyView,
    mcp: AnyView,
    skills: AnyView,
}
impl Render for SidebarHostView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let route = self.navigation.snapshot();
        if matches!(route.route(), MainRoute::Threads | MainRoute::AgentsDoc) {
            return v_flex()
                .size_full()
                .bg(cx.theme().sidebar)
                .gap_5()
                .child(
                    div()
                        .flex_1()
                        .min_h_0()
                        .w_full()
                        .overflow_hidden()
                        .child(self.workspaces.clone()),
                )
                .children(self.desktop_update.clone())
                .into_any_element();
        }
        let content = match route.route() {
            MainRoute::Settings => &self.settings,
            MainRoute::Providers => &self.providers,
            MainRoute::Administration => &self.administration,
            MainRoute::Mcp | MainRoute::McpDetails => &self.mcp,
            MainRoute::Skills | MainRoute::SkillDetails => &self.skills,
            MainRoute::Threads | MainRoute::AgentsDoc => unreachable!(),
        }
        .clone();
        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_hidden()
                    .child(content),
            )
            .into_any_element()
    }
}
#[derive(IntoElement)]
struct DesktopBody {
    sidebar: AnyElement,
    screen: AnyElement,
    thread_mounted: bool,
    bottom: AnyElement,
    layout: Entity<ShellStateStore>,
}
impl RenderOnce for DesktopBody {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let visible = self.layout.read(cx).sidebar_visible();
        let width = self.layout.read(cx).sidebar_width();
        let layout = self.layout.downgrade();
        let thread_mounted = self.thread_mounted;
        let screen = div().relative().size_full().child(
            div()
                .absolute()
                .top_0()
                .left_0()
                .right_0()
                .bottom(if thread_mounted { -rems(2.) } else { rems(0.) })
                .child(self.screen),
        );
        v_flex()
            .relative()
            .size_full()
            .pb_8()
            .child(
                div().flex_1().min_h_0().w_full().child(
                    h_resizable("desktop-layout")
                        .on_resize(move |state, _, cx| {
                            if let Some(width) = state.read(cx).sizes().first().copied() {
                                let _ = layout
                                    .update(cx, |layout, cx| layout.set_sidebar_width(width, cx));
                            }
                        })
                        .child(
                            resizable_panel()
                                .visible(visible)
                                .size(width)
                                .size_range(px(260.)..px(520.))
                                .child(self.sidebar),
                        )
                        .child(resizable_panel().child(screen)),
                ),
            )
            .child(
                div()
                    .absolute()
                    .bottom_0()
                    .left_0()
                    .right_0()
                    .child(self.bottom),
            )
    }
}

#[cfg(test)]
#[path = "shell_context_tests.rs"]
mod context_tests;
