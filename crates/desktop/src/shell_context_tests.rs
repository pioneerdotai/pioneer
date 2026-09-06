use gpui_kit::component::{Root, WindowExt};
use gpui_kit::{
    AppContext, Context, FocusHandle, InteractiveElement, IntoElement, Render, TestAppContext,
    Window, div,
};

struct FocusOwner {
    focus: FocusHandle,
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
