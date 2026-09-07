//! Window composition. Feature state remains behind retained public handles.
use crate::desktop_navigation::*;
use crate::{
    app::LegacyScreenAdapter, desktop_navigation::DesktopNavigationStore,
    shell_state::ShellStateStore,
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

pub(crate) struct DesktopShellView {
    workspaces: Option<Entity<pioneer_desktop_workspaces::WorkspaceNavigationView>>,
    _workspace_events: Subscription,
    _task_notification_events: Subscription,
    _workspace_context: Subscription,
    desktop_update: Option<Entity<pioneer_desktop_update::DesktopUpdateView>>,
    task_notifications: Option<Entity<pioneer_desktop_task_notifications::TaskNotificationView>>,
    action_region: FocusHandle,
    legacy: Option<Entity<LegacyScreenAdapter>>,
    sidebar: Option<Entity<SidebarHostView>>,
    navigation: Arc<DesktopNavigationStore>,
    layout: Entity<ShellStateStore>,
    _layout_subscription: Subscription,
    _frame_subscription: Subscription,
    _activation_subscription: Subscription,
    route_task: Option<Task<()>>,
    mounted_route: Arc<DesktopRouteSnapshot>,
}
impl DesktopShellView {
    pub(crate) fn new(
        legacy: Entity<LegacyScreenAdapter>,
        navigation: Arc<DesktopNavigationStore>,
        layout: Entity<ShellStateStore>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let (client, registrar) = {
            let runtime = cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>();
            (runtime.core(), runtime.registrar())
        };
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
        let task_legacy = legacy.downgrade();
        let task_notification_events = cx.subscribe_in(
            &task_notifications,
            window,
            move |_,
                  _,
                  event: &pioneer_desktop_task_notifications::TaskNotificationEvent,
                  window,
                  cx| {
                let pioneer_desktop_task_notifications::TaskNotificationEvent::OpenThread {
                    thread_id,
                } = event;
                let _ = task_legacy.update(cx, |legacy, cx| {
                    legacy.present_workspace_thread(Some(thread_id.clone()), window, cx)
                });
            },
        );
        legacy.update(cx, |legacy, _| {
            legacy.task_notification_surface = Some(task_notifications.clone().into())
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
        let legacy_context = legacy.downgrade();
        let workspace_preference = legacy.downgrade();
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
            move |cx| {
                legacy_context
                    .upgrade()
                    .is_some_and(|legacy| legacy.read(cx).workspace_context_locked())
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
        )
        .with_workspace_preference(move |cx| {
            workspace_preference
                .upgrade()
                .and_then(|legacy| legacy.read(cx).persisted_workspace_preference())
        });
        let workspaces = cx.new(|cx| {
            pioneer_desktop_workspaces::WorkspaceNavigationView::new(workspace_config, cx)
        });
        let workspace_context_view = workspaces.downgrade();
        let workspace_context = cx.observe(&legacy, move |_, _, cx| {
            let _ = workspace_context_view.update(cx, |view, cx| view.refresh_presentation(cx));
        });
        let legacy_events = legacy.downgrade();
        let workspace_events = cx.subscribe_in(
            &workspaces,
            window,
            move |_,
                  _,
                  event: &pioneer_desktop_workspaces::WorkspaceNavigationEvent,
                  window,
                  cx| {
                let _ = legacy_events.update(cx, |legacy, cx| match event {
                    pioneer_desktop_workspaces::WorkspaceNavigationEvent::OpenThread {
                        thread_id,
                    } => legacy.present_workspace_thread(thread_id.clone(), window, cx),
                    pioneer_desktop_workspaces::WorkspaceNavigationEvent::OpenAgentsDocument {
                        scope,
                    } => legacy.present_workspace_agents_document(scope.clone(), window, cx),
                });
            },
        );
        navigation.set_window_route(legacy.read(cx).window_route());
        let sidebar = cx.new(|cx| SidebarHostView {
            workspaces: workspaces.clone(),
            desktop_update: desktop_update.clone(),
            legacy: legacy.downgrade(),
            navigation: navigation.clone(),
            _changes: cx.subscribe(&legacy, |_, _, _: &crate::app::SidebarChanged, cx| {
                cx.notify()
            }),
        });
        let frame_subscription =
            cx.subscribe(&legacy, |view, legacy, _: &crate::app::FrameChanged, cx| {
                view.navigation
                    .set_window_route(legacy.read(cx).window_route());
                cx.notify()
            });
        let layout_subscription = cx.observe(&layout, |_, _, cx| cx.notify());
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
                        if let Some(legacy) = &view.legacy {
                            let input = view.navigation.snapshot().navigation().clone();
                            legacy.update(cx, |legacy, cx| {
                                legacy.apply_navigation_publication(input, cx)
                            });
                        }
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let activation_subscription = cx.observe_window_activation(window, |view, window, cx| {
            if let Some(legacy) = &view.legacy {
                legacy.update(cx, |legacy, cx| {
                    legacy.set_window_active(window.is_window_active(), cx)
                });
            }
        });
        let shell = cx.weak_entity();
        window.on_window_should_close(cx, move |window, cx| {
            let _ = shell.update(cx, |view, cx| view.close(window, cx));
            true
        });
        Self {
            workspaces: Some(workspaces),
            _workspace_events: workspace_events,
            _task_notification_events: task_notification_events,
            _workspace_context: workspace_context,
            desktop_update,
            task_notifications: Some(task_notifications),
            action_region: cx.focus_handle(),
            mounted_route: navigation.snapshot(),
            legacy: Some(legacy),
            sidebar: Some(sidebar),
            navigation,
            layout,
            _layout_subscription: layout_subscription,
            _frame_subscription: frame_subscription,
            _activation_subscription: activation_subscription,
            route_task: Some(route_task),
        }
    }
    pub(crate) fn start_desktop_update(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(update) = &self.desktop_update {
            update.update(cx, |view, cx| view.check(window, cx));
        }
    }
    fn activate_navigation(&mut self, route: MainRoute, cx: &mut Context<Self>) {
        if let Some(legacy) = &self.legacy {
            legacy.update(cx, |legacy, cx| legacy.activate_navigation(route, cx));
        }
    }
    pub(crate) fn close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        window.close_all_dialogs(cx);
        window.close_sheet(cx);
        self.layout
            .update(cx, |layout, cx| layout.persist(window, cx));
        self.route_task.take();
        if let Some(legacy) = &self.legacy {
            legacy.update(cx, |legacy, cx| legacy.close_route_bindings(cx));
        }
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
        self.legacy.take();
    }
}
impl Render for DesktopShellView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(legacy) = self.legacy.as_ref() else {
            return div().into_any_element();
        };
        let route = self.navigation.snapshot();
        let title = TitleBarSurface {
            content: LegacyScreenAdapter::title_bar_surface(legacy, window, cx),
        };
        let screen = ScreenHostView {
            selected: legacy.clone(),
            route: route.clone(),
        };
        let body = if route.window_route() == WindowRoute::Main {
            DesktopBody {
                sidebar: self.sidebar.as_ref().expect("live sidebar").clone(),
                screen,
                bottom: BottomBarSurface {
                    content: LegacyScreenAdapter::bottom_bar_surface(
                        legacy,
                        &self.action_region,
                        cx,
                    ),
                },
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
            .on_action(cx.listener(
                |view, action: &pioneer_desktop_workspaces::RenameThread, window, cx| {
                    if let Some(workspaces) = &view.workspaces {
                        workspaces.update(cx, |workspaces, cx| {
                            workspaces.rename_thread(action, window, cx)
                        });
                    }
                },
            ))
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
    selected: Entity<LegacyScreenAdapter>,
    route: Arc<DesktopRouteSnapshot>,
}
impl RenderOnce for ScreenHostView {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        match self.route.route() {
            MainRoute::Threads
            | MainRoute::AgentsDoc
            | MainRoute::Providers
            | MainRoute::Administration
            | MainRoute::Mcp
            | MainRoute::McpDetails
            | MainRoute::Skills
            | MainRoute::SkillDetails
            | MainRoute::Settings => self.selected,
        }
    }
}
struct SidebarHostView {
    workspaces: Entity<pioneer_desktop_workspaces::WorkspaceNavigationView>,
    desktop_update: Option<Entity<pioneer_desktop_update::DesktopUpdateView>>,
    legacy: WeakEntity<LegacyScreenAdapter>,
    navigation: Arc<DesktopNavigationStore>,
    _changes: Subscription,
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
        let content = self
            .legacy
            .upgrade()
            .map(|legacy| LegacyScreenAdapter::sidebar_surface(&legacy, route.route(), cx));
        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_hidden()
                    .children(content),
            )
            .into_any_element()
    }
}
#[derive(IntoElement)]
struct DesktopBody {
    sidebar: Entity<SidebarHostView>,
    screen: ScreenHostView,
    bottom: BottomBarSurface,
    layout: Entity<ShellStateStore>,
}
impl RenderOnce for DesktopBody {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let visible = self.layout.read(cx).sidebar_visible();
        let width = self.layout.read(cx).sidebar_width();
        let layout = self.layout.downgrade();
        v_flex()
            .size_full()
            .child(
                div().flex_1().min_h_0().w_full().overflow_hidden().child(
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
                        .child(resizable_panel().child(self.screen)),
                ),
            )
            .child(self.bottom)
    }
}

#[cfg(test)]
#[path = "shell_context_tests.rs"]
mod context_tests;

#[cfg(test)]
mod tests {
    #[test]
    fn bootstrap_has_one_toolkit_root_and_shell_is_its_content() {
        let main = include_str!("main.rs");
        assert_eq!(main.matches("gpui_kit::init(cx)").count(), 1);
        assert_eq!(main.matches("Root::new(").count(), 1);
        assert!(main.contains("Root::new(shell, window, cx)"));
        assert!(main.contains("desktop = Some(legacy.downgrade())"));
        let shell = include_str!("desktop_shell.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        assert!(!shell.contains("Entity<Root>"));
        for forbidden in [
            "client_core()",
            "GatewayWsEvent",
            "ClientSubscription",
            "ClientRuntimePostEventSink",
            ".cached()",
        ] {
            assert!(!shell.contains(forbidden), "{forbidden}");
        }
        for surface in [
            "TitleBarSurface",
            "BottomBarSurface",
            "DesktopBody",
            "ScreenHostView",
        ] {
            assert!(shell.contains(&format!("impl RenderOnce for {surface}")));
            assert!(!shell.contains(&format!("Entity<{surface}>")));
        }
        let render = shell
            .split("impl Render for DesktopShellView")
            .nth(1)
            .unwrap();
        for forbidden in ["cx.new(", "cx.spawn(", "cx.subscribe(", "cx.focus_handle("] {
            assert!(!render.contains(forbidden), "{forbidden}");
        }
        for layer in [
            "render_sheet_layer",
            "render_dialog_layer",
            "render_notification_layer",
        ] {
            assert_eq!(shell.matches(layer).count(), 1);
        }
    }
    #[test]
    fn navigation_controls_dispatch_one_action_to_the_window_owner() {
        let buttons = include_str!("app/bottom_bar/view.rs");
        let shell = include_str!("desktop_shell.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        for action in [
            "OpenThreads",
            "OpenProviders",
            "OpenMcp",
            "OpenSkills",
            "OpenAdministration",
            "OpenSettings",
        ] {
            assert_eq!(
                buttons
                    .matches(&format!("dispatch_action(&{action}, window, cx)"))
                    .count(),
                1
            );
            assert_eq!(shell.matches(&format!("_: &{action}")).count(), 1);
        }
        assert!(shell.contains("key_context(\"DesktopShell\")"));
        assert!(!buttons.contains("set_main_content_view"));
        let layout = include_str!("shell_state.rs");
        assert!(!layout.contains("pioneer_client"));
        assert!(layout.contains("observe_window_bounds"));
        assert!(!include_str!("app/root/mod.rs").contains("sidebar_panel_width:"));
    }
    #[test]
    fn route_exit_preserves_presentation_identity_and_access_cleanup_uses_client_plan() {
        let lifecycle = include_str!("app/root/route_lifecycle.rs");
        let warm = lifecycle
            .split("if activity == crate::desktop_navigation::RouteActivity::Dormant")
            .next()
            .unwrap();
        assert!(warm.contains("set_active(thread_active, cx)"));
        assert!(!warm.contains("thread_timeline_view_state.borrow_mut()"));
        assert!(!warm.contains("thread_timeline_terminal_item.borrow_mut().clear()"));
        let main = include_str!("main.rs");
        assert_eq!(main.matches("LegacyScreenAdapter::new(").count(), 1);
        let access = include_str!("app/flow/ws_events_notifications.rs")
            .split("fn apply_access_changed_notification(")
            .nth(1)
            .unwrap()
            .split("fn apply_authorization_projection_changed_notification(")
            .next()
            .unwrap();
        assert!(access.contains("apply_thread_access_change(&notification)"));
        assert!(!access.contains("plan_access_changed("));
        assert!(access.contains("if plan.clear_active_thread"));
    }

    #[test]
    fn route_identity_closes_overlays_and_sidebar_publications_target_only_sidebar() {
        let shell = include_str!("desktop_shell.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        assert!(
            shell
                .split_whitespace()
                .collect::<String>()
                .contains("previous.navigation().destination()!=route.navigation().destination()")
        );
        assert!(shell.contains("track_focus(&self.action_region)"));
        let updater = include_str!("../../desktop-update/src/view.rs");
        assert!(!updater.contains("SidebarChanged"));
        assert!(!updater.contains("DesktopShellView"));
        let mcp = include_str!("app/mcp/lifecycle.rs");
        let details = mcp
            .split("fn apply_mcp_details_refresh_success_reduction(")
            .nth(1)
            .unwrap()
            .split("fn apply_mcp_details_refresh_failure_reduction(")
            .next()
            .unwrap();
        assert_eq!(
            details
                .matches("cx.emit(crate::app::SidebarChanged)")
                .count(),
            1
        );
        for poller in [mcp, include_str!("app/skills/lifecycle.rs")] {
            let ensure = poller.split("fn ensure_").last().unwrap();
            assert!(ensure.contains("RouteActivity::Active"));
        }
        let teardown = include_str!("app/root/route_lifecycle.rs")
            .split("fn close_route_bindings(")
            .nth(1)
            .unwrap();
        let controlled = teardown.find("composer_input_subscription.take()").unwrap();
        let highlight = teardown.find("code_highlight_cache.borrow_mut()").unwrap();
        let activity = teardown
            .find("running_indicator_views.borrow_mut()")
            .unwrap();
        let native = teardown
            .find("thread_timeline_terminal_item.borrow_mut()")
            .unwrap();
        let scroll = teardown
            .find("thread_timeline_view_state.borrow_mut()")
            .unwrap();
        assert!(
            controlled < highlight && highlight < activity && activity < native && native < scroll
        );
    }

    #[test]
    fn retained_legacy_route_owns_no_mutable_navigation_copy() {
        let owner = include_str!("app/root/mod.rs");
        let fields = owner.split("struct LegacyScreenAdapter {").nth(1).unwrap();
        for removed in [
            "active_thread_id:",
            "preferred_workspace_id:",
            "task_thread_navigation_stack:",
            "main_content_view:",
            "mcp_selected_server_id:",
            "selected_skill_target:",
        ] {
            assert!(!fields.contains(removed), "{removed}");
        }
        assert_eq!(owner.matches("struct LegacyScreenAdapter").count(), 1);
        let lifecycle = include_str!("app/root/route_lifecycle.rs");
        for release in [
            "mcp_poller.take()",
            "skills_poller.take()",
            "thread_bindings.clear()",
            "composer_input_subscription.take()",
            "thread_timeline_terminal_item.borrow_mut().clear()",
        ] {
            assert!(lifecycle.contains(release), "{release}");
        }
        let old_frame = include_str!("app/root/view.rs");
        assert!(!old_frame.contains("Root::"));
        assert!(!old_frame.contains("h_resizable"));
        assert!(!include_str!("app/root/state.rs").contains("install_window_state_persistence"));
    }
}
