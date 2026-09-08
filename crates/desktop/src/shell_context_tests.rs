use gpui_kit::component::{Root, WindowExt};
use gpui_kit::{
    AppContext, Context, FocusHandle, InteractiveElement, IntoElement, Render, TestAppContext,
    Window, div,
};

struct FocusOwner {
    focus: FocusHandle,
}

#[gpui_kit::test]
fn policy_refresh_keeps_members_dialog_open_before_and_after_result(cx: &mut TestAppContext) {
    use pioneer_client::navigation::{AdministrationRoute, NavigationIntent, SemanticDestination};
    cx.update(gpui_kit::init);
    cx.update(crate::client_runtime::DesktopRuntimeCoordinator::install_for_test);
    let core = cx.update(|cx| {
        cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .core()
    });
    let destination = SemanticDestination::Administration {
        route: AdministrationRoute::Members,
    };
    core.activate_thread(Some("thread"), Some("workspace"));
    core.navigate(
        NavigationIntent::Navigate {
            destination: destination.clone(),
        },
        None,
    );
    let (_root, cx) = cx.add_window_view(|window, cx| {
        let registrar = cx
            .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .registrar();
        let navigation = crate::desktop_navigation::DesktopNavigationStore::new(registrar.as_ref());
        let layout = cx.new(|cx| crate::shell_state::ShellStateStore::new(window, cx));
        let desktop = cx.new(|cx| {
            crate::app::LegacyScreenAdapter::new(
                window,
                cx,
                pioneer_observability::DesktopStartupTrace::start(),
                navigation.clone(),
                layout.clone(),
            )
        });
        let shell = cx.new(|cx| {
            crate::desktop_shell::DesktopShellView::new(desktop, navigation, layout, window, cx)
        });
        Root::new(shell, window, cx)
    });
    cx.run_until_parked();
    let result = cx.new(|_| None::<String>);
    cx.update(|window, cx| {
        let result = result.clone();
        window.open_dialog(cx, move |dialog, _, cx| {
            dialog.title(
                result
                    .read(cx)
                    .clone()
                    .unwrap_or_else(|| "Creating invitation".into()),
            )
        });
    });
    for revision in [7, 8] {
        core.invalidate_authorization_revision(revision);
        cx.run_until_parked();
        cx.update(|window, cx| {
            assert_eq!(core.navigation_snapshot().destination(), &destination);
            assert!(window.has_active_dialog(cx));
            if revision == 7 {
                result.update(cx, |result, cx| {
                    *result = Some("Synthetic invitation result".into());
                    cx.notify();
                });
            } else {
                assert!(result.read(cx).is_some());
            }
        });
    }
    // Genuine route changes still close the one-time credential presentation.
    core.navigate(
        NavigationIntent::Navigate {
            destination: SemanticDestination::Threads,
        },
        None,
    );
    cx.run_until_parked();
    cx.update(|window, cx| assert!(!window.has_active_dialog(cx)));
}

#[gpui_kit::test]
fn desktop_window_constructs_gateway_views_before_first_frame(cx: &mut TestAppContext) {
    cx.update(gpui_kit::init);
    cx.update(crate::client_runtime::DesktopRuntimeCoordinator::install_for_test);
    let window = cx.update(|cx| {
        cx.open_window(Default::default(), |window, cx| {
            crate::client_runtime::DesktopRuntimeCoordinator::install(cx);
            let registrar = cx
                .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                .registrar();
            let navigation =
                crate::desktop_navigation::DesktopNavigationStore::new(registrar.as_ref());
            let layout = cx.new(|cx| crate::shell_state::ShellStateStore::new(window, cx));
            let desktop = cx.new(|cx| {
                crate::app::LegacyScreenAdapter::new(
                    window,
                    cx,
                    pioneer_observability::DesktopStartupTrace::start(),
                    navigation.clone(),
                    layout.clone(),
                )
            });
            let shell = cx.new(|cx| {
                crate::desktop_shell::DesktopShellView::new(desktop, navigation, layout, window, cx)
            });
            cx.new(|cx| Root::new(shell, window, cx))
        })
        .unwrap()
    });
    let root = window.root(cx).unwrap();
    root.read_with(cx, |root, _| {
        assert!(
            root.view()
                .clone()
                .downcast::<crate::desktop_shell::DesktopShellView>()
                .is_ok()
        );
    });
}
impl Render for FocusOwner {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div().track_focus(&self.focus)
    }
}

#[gpui_kit::test]
fn root_nested_overlays_restore_the_retained_trigger(cx: &mut TestAppContext) {
    cx.update(gpui_kit::init);
    let (root, cx) = cx.add_window_view(|window, cx| {
        let child = cx.new(|cx| FocusOwner {
            focus: cx.focus_handle(),
        });
        Root::new(child, window, cx)
    });
    cx.update(|window, cx| {
        let owner = root
            .read(cx)
            .view()
            .clone()
            .downcast::<FocusOwner>()
            .unwrap();
        let focus = owner.read(cx).focus.clone();
        focus.focus(window, cx);
        window.open_dialog(cx, |dialog, _, _| dialog);
        let first_dialog = window.focused(cx).unwrap();
        window.open_dialog(cx, |dialog, _, _| dialog);
        window.close_dialog(cx);
        assert_eq!(window.focused(cx), Some(first_dialog));
        window.close_dialog(cx);
        assert_eq!(window.focused(cx), Some(focus.clone()));
        window.open_sheet(cx, |sheet, _, _| sheet);
        window.close_sheet(cx);
        assert_eq!(window.focused(cx), Some(focus));
    });
}

struct ActionOwner {
    focus: FocusHandle,
    calls: usize,
}
impl Render for ActionOwner {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use gpui_kit::{ParentElement, Styled};
        let target = self.focus.clone();
        div()
            .size_full()
            .track_focus(&self.focus)
            .key_context("DesktopShell")
            .on_action(
                cx.listener(|view, _: &crate::desktop_navigation::OpenThreads, _, cx| {
                    view.calls += 1;
                    cx.notify();
                }),
            )
            .child(
                gpui_kit::component::button::Button::new("open-threads")
                    .label("Threads")
                    .on_click(move |_, window, cx| {
                        target.dispatch_action(&crate::desktop_navigation::OpenThreads, window, cx)
                    }),
            )
    }
}

#[gpui_kit::test]
fn pointer_key_and_menu_dispatch_reach_one_action_handler(cx: &mut TestAppContext) {
    use crate::desktop_navigation::OpenThreads;
    use gpui_kit::{KeyBinding, Modifiers, point, px};
    cx.update(gpui_kit::init);
    let (root, cx) = cx.add_window_view(|window, cx| {
        let owner = cx.new(|cx| ActionOwner {
            focus: cx.focus_handle(),
            calls: 0,
        });
        Root::new(owner, window, cx)
    });
    let owner = root.read_with(cx, |root, _| {
        root.view().clone().downcast::<ActionOwner>().unwrap()
    });
    cx.update(|window, cx| {
        window.blur(cx);
        cx.bind_keys([KeyBinding::new(
            "ctrl-alt-t",
            OpenThreads,
            Some("DesktopShell"),
        )]);
    });
    cx.run_until_parked();
    cx.simulate_click(point(px(10.), px(10.)), Modifiers::default());
    cx.run_until_parked();
    assert_eq!(owner.read_with(cx, |owner, _| owner.calls), 1);
    cx.update(|window, cx| {
        let focus = owner.read(cx).focus.clone();
        focus.focus(window, cx);
    });
    cx.simulate_keystrokes("ctrl-alt-t");
    cx.run_until_parked();
    assert_eq!(owner.read_with(cx, |owner, _| owner.calls), 2);
    cx.dispatch_action(OpenThreads);
    cx.run_until_parked();
    assert_eq!(owner.read_with(cx, |owner, _| owner.calls), 3);
}

struct ThreadAllocationProbe {
    layout: gpui_kit::Entity<crate::shell_state::ShellStateStore>,
    clicks: std::rc::Rc<std::cell::Cell<usize>>,
}
impl Render for ThreadAllocationProbe {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        use gpui_kit::component::{button::Button, v_flex};
        use gpui_kit::{ParentElement, Styled};
        let clicks = self.clicks.clone();
        super::DesktopBody {
            layout: self.layout.clone(),
            thread_mounted: true,
            sidebar: div()
                .size_full()
                .debug_selector(|| "allocation-sidebar".into())
                .into_any_element(),
            screen: v_flex()
                .size_full()
                .debug_selector(|| "allocation-thread".into())
                .child(div().flex_1().min_h_0())
                .child(
                    div()
                        .h_8()
                        .flex_none()
                        .debug_selector(|| "allocation-thread-footer".into())
                        .child(
                            Button::new("allocation-action")
                                .label("Panel")
                                .on_click(move |_, _, _| clicks.set(clicks.get() + 1)),
                        ),
                )
                .into_any_element(),
            bottom: div()
                .h_8()
                .debug_selector(|| "allocation-bottom".into())
                .into_any_element(),
        }
    }
}
#[gpui_kit::test]
fn thread_footer_shares_the_existing_bottom_bar_allocation_and_receives_pointer_events(
    cx: &mut TestAppContext,
) {
    cx.update(gpui_kit::init);
    let clicks = std::rc::Rc::new(std::cell::Cell::new(0));
    let (_, cx) = cx.add_window_view(|window, cx| {
        let layout = cx.new(|cx| crate::shell_state::ShellStateStore::new(window, cx));
        let probe = cx.new(|_| ThreadAllocationProbe {
            layout,
            clicks: clicks.clone(),
        });
        Root::new(probe, window, cx)
    });
    cx.run_until_parked();
    let sidebar = cx.debug_bounds("allocation-sidebar").unwrap();
    let thread = cx.debug_bounds("allocation-thread").unwrap();
    let footer = cx.debug_bounds("allocation-thread-footer").unwrap();
    let bottom = cx.debug_bounds("allocation-bottom").unwrap();
    assert_eq!(footer.origin.y, bottom.origin.y);
    assert_eq!(footer.size.height, bottom.size.height);
    assert_eq!(sidebar.bottom(), bottom.top());
    assert_eq!(thread.bottom(), bottom.bottom());
    cx.simulate_click(
        footer.origin + gpui_kit::point(gpui_kit::px(20.), gpui_kit::px(12.)),
        gpui_kit::Modifiers::default(),
    );
    assert_eq!(clicks.get(), 1);
}
