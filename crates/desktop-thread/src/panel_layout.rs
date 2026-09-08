//! Window-local thread panel layout and its existing footer controls.

use crate::assets::PioneerIconName;
use gpui_kit::component::{button::*, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThreadPanelKind {
    Artifacts,
    Members,
}

pub(crate) struct ThreadPanelOpened(pub ThreadPanelKind);

pub(crate) struct ThreadPanelLayoutStore {
    selected: ThreadPanelKind,
    open: bool,
    width: Pixels,
}

impl Default for ThreadPanelLayoutStore {
    fn default() -> Self {
        Self {
            selected: ThreadPanelKind::Artifacts,
            open: false,
            width: px(340.),
        }
    }
}
impl EventEmitter<ThreadPanelOpened> for ThreadPanelLayoutStore {}

/// Retains only the layout entity for each live window. Thread roots hold a
/// handle, so dropping A before mounting B does not reset window presentation.
struct WindowPanelLayouts {
    layouts: std::collections::HashMap<WindowId, Entity<ThreadPanelLayoutStore>>,
    _closed: Subscription,
}
impl Global for WindowPanelLayouts {}

impl ThreadPanelLayoutStore {
    pub(crate) fn for_window(window: &Window, cx: &mut App) -> Entity<Self> {
        if !cx.has_global::<WindowPanelLayouts>() {
            let closed = cx.on_window_closed(|cx, id| {
                cx.update_global::<WindowPanelLayouts, _>(|state, _| {
                    state.layouts.remove(&id);
                });
            });
            cx.set_global(WindowPanelLayouts {
                layouts: Default::default(),
                _closed: closed,
            });
        }
        let id = window.window_handle().window_id();
        if let Some(layout) = cx.global::<WindowPanelLayouts>().layouts.get(&id) {
            return layout.clone();
        }
        let layout = cx.new(|_| Self::default());
        cx.update_global::<WindowPanelLayouts, _>(|state, _| {
            state.layouts.insert(id, layout.clone());
        });
        layout
    }

    pub(crate) fn selected(&self) -> ThreadPanelKind {
        self.selected
    }
    pub(crate) fn is_open(&self) -> bool {
        self.open
    }
    pub(crate) fn is_visible(&self, kind: ThreadPanelKind) -> bool {
        self.open && self.selected == kind
    }
    pub(crate) fn width(&self) -> Pixels {
        self.width
    }

    fn select(&mut self, kind: ThreadPanelKind, open: bool) -> bool {
        if self.selected == kind && self.open == open {
            return false;
        }
        self.selected = kind;
        self.open = open;
        true
    }
    pub(crate) fn open(&mut self, kind: ThreadPanelKind, cx: &mut Context<Self>) {
        if self.select(kind, true) {
            cx.notify();
        }
        cx.emit(ThreadPanelOpened(kind));
    }
    fn toggle(&mut self, kind: ThreadPanelKind, cx: &mut Context<Self>) {
        let open = !self.is_visible(kind);
        if self.select(kind, open) {
            cx.notify();
        }
        if open {
            cx.emit(ThreadPanelOpened(kind));
        }
    }
    pub(crate) fn resize(&mut self, width: Pixels, cx: &mut Context<Self>) {
        if self.width != width {
            self.width = width;
            cx.notify();
        }
    }
}

actions!(thread_panels, [ToggleThreadArtifacts, ToggleThreadMembers]);

/// Owns its action/focus region and retained observation. Shell mounts this
/// opaque child; it never reads panel layout to paint selected buttons.
pub(crate) struct ThreadPanelControlsView {
    layout: Entity<ThreadPanelLayoutStore>,
    focus: FocusHandle,
    _layout_changed: Subscription,
}

impl ThreadPanelControlsView {
    pub(crate) fn new(layout: Entity<ThreadPanelLayoutStore>, cx: &mut Context<Self>) -> Self {
        Self {
            focus: cx.focus_handle(),
            _layout_changed: cx.observe(&layout, |_, _, cx| cx.notify()),
            layout,
        }
    }
    fn toggle_artifacts(
        &mut self,
        _: &ToggleThreadArtifacts,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.layout.update(cx, |layout, cx| {
            layout.toggle(ThreadPanelKind::Artifacts, cx)
        });
    }
    fn toggle_members(&mut self, _: &ToggleThreadMembers, _: &mut Window, cx: &mut Context<Self>) {
        self.layout
            .update(cx, |layout, cx| layout.toggle(ThreadPanelKind::Members, cx));
    }
}

impl Render for ThreadPanelControlsView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let layout = self.layout.read(cx);
        let artifacts = layout.is_visible(ThreadPanelKind::Artifacts);
        let members = layout.is_visible(ThreadPanelKind::Members);
        let artifact_icon = if artifacts {
            IconName::PanelRightClose
        } else {
            IconName::PanelRightOpen
        };
        h_flex()
            .items_center()
            .gap_1()
            .key_context("ThreadPanels")
            .track_focus(&self.focus)
            .on_action(cx.listener(Self::toggle_artifacts))
            .on_action(cx.listener(Self::toggle_members))
            .child(
                Button::new("bottom-bar-toggle-thread-members-sidebar")
                    .ghost()
                    .small()
                    .compact()
                    .tooltip(t!("settings.sidebar.members").to_string())
                    .child(
                        Icon::new(PioneerIconName::UserCheck)
                            .size_3p5()
                            .opacity(0.6)
                            .when(members, |icon| {
                                icon.opacity(1.0).text_color(cx.theme().blue)
                            }),
                    )
                    .on_click({
                        let focus = self.focus.clone();
                        move |_, window, cx| focus.dispatch_action(&ToggleThreadMembers, window, cx)
                    }),
            )
            .child(
                Button::new("bottom-bar-toggle-thread-artifacts-sidebar")
                    .ghost()
                    .small()
                    .compact()
                    .tooltip(t!("artifacts.title").to_string())
                    .child(
                        Icon::new(artifact_icon)
                            .size_3p5()
                            .opacity(0.6)
                            .when(artifacts, |icon| {
                                icon.opacity(1.0).text_color(cx.theme().blue)
                            }),
                    )
                    .on_click({
                        let focus = self.focus.clone();
                        move |_, window, cx| {
                            focus.dispatch_action(&ToggleThreadArtifacts, window, cx)
                        }
                    }),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ThreadPanelControlsView, ThreadPanelKind, ThreadPanelLayoutStore, ToggleThreadMembers,
    };
    use gpui_kit::{AppContext, TestAppContext, px};
    #[gpui_kit::test]
    fn pointer_key_and_context_action_toggle_the_same_panel_owner(cx: &mut TestAppContext) {
        use gpui_kit::component::Root;
        use gpui_kit::{KeyBinding, Modifiers, point};
        cx.update(gpui_kit::init);
        let layout = cx.new(|_| ThreadPanelLayoutStore::default());
        let (root, cx) = cx.add_window_view(|window, cx| {
            let controls = cx.new(|cx| ThreadPanelControlsView::new(layout.clone(), cx));
            Root::new(controls, window, cx)
        });
        let controls = root.read_with(cx, |root, _| {
            root.view()
                .clone()
                .downcast::<ThreadPanelControlsView>()
                .unwrap()
        });
        cx.run_until_parked();
        cx.simulate_click(point(px(10.), px(10.)), Modifiers::default());
        cx.run_until_parked();
        assert!(layout.read_with(cx, |layout, _| layout.is_visible(ThreadPanelKind::Members)));
        cx.update(|window, cx| {
            let focus = controls.read(cx).focus.clone();
            focus.focus(window, cx);
            cx.bind_keys([KeyBinding::new(
                "ctrl-alt-p",
                ToggleThreadMembers,
                Some("ThreadPanels"),
            )]);
        });
        cx.simulate_keystrokes("ctrl-alt-p");
        cx.run_until_parked();
        assert!(!layout.read_with(cx, |layout, _| layout.is_open()));
        cx.update(|window, cx| {
            controls
                .read(cx)
                .focus
                .clone()
                .dispatch_action(&ToggleThreadMembers, window, cx)
        });
        cx.run_until_parked();
        assert!(layout.read_with(cx, |layout, _| layout.is_visible(ThreadPanelKind::Members)));
    }

    #[gpui_kit::test]
    fn replacing_content_controls_preserves_layout_and_drop_releases_observation(
        cx: &mut TestAppContext,
    ) {
        let layout = cx.new(|_| ThreadPanelLayoutStore::default());
        let old = cx.new(|cx| ThreadPanelControlsView::new(layout.clone(), cx));
        let weak_old = old.downgrade();
        layout.update(cx, |layout, cx| {
            layout.open(ThreadPanelKind::Members, cx);
            layout.resize(px(412.), cx);
        });
        drop(old);
        cx.update(|_| {});
        cx.run_until_parked();
        assert!(weak_old.upgrade().is_none());
        let next = cx.new(|cx| ThreadPanelControlsView::new(layout.clone(), cx));
        assert_eq!(
            layout.read_with(cx, |layout, _| (
                layout.selected(),
                layout.is_open(),
                layout.width()
            )),
            (ThreadPanelKind::Members, true, px(412.))
        );
        let weak_layout = layout.downgrade();
        drop(next);
        drop(layout);
        cx.update(|_| {});
        cx.run_until_parked();
        assert!(weak_layout.upgrade().is_none());
    }

    #[gpui_kit::test]
    fn window_retains_layout_across_unmount_and_releases_it_on_close(cx: &mut TestAppContext) {
        use gpui_kit::component::Root;
        use gpui_kit::{Context, Entity, IntoElement, ParentElement, Render, Window, div};
        struct Host(Option<Entity<ThreadPanelControlsView>>);
        impl Render for Host {
            fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
                div().children(self.0.clone())
            }
        }
        cx.update(gpui_kit::init);
        let (root, cx) = cx.add_window_view(|window, cx| {
            let layout = ThreadPanelLayoutStore::for_window(window, cx);
            let controls = cx.new(|cx| ThreadPanelControlsView::new(layout, cx));
            let host = cx.new(|_| Host(Some(controls)));
            Root::new(host, window, cx)
        });
        let weak = cx.update(|window, cx| {
            let first = ThreadPanelLayoutStore::for_window(window, cx);
            first.update(cx, |layout, cx| {
                layout.open(ThreadPanelKind::Members, cx);
                layout.resize(px(412.), cx);
            });
            first.downgrade()
        });
        // The last content/control handle is discarded, as when routing away.
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<Host>().unwrap()
        });
        host.update(cx, |host, cx| {
            host.0 = None;
            cx.notify();
        });
        cx.run_until_parked();
        assert!(weak.upgrade().is_some());
        cx.update(|window, cx| {
            let next = ThreadPanelLayoutStore::for_window(window, cx);
            assert_eq!(next.downgrade(), weak);
            assert_eq!(next.read(cx).selected(), ThreadPanelKind::Members);
            assert!(next.read(cx).is_open());
            assert_eq!(next.read(cx).width(), px(412.));
            window.remove_window();
        });
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn panel_selection_open_state_and_width_have_window_lifetime() {
        let mut layout = ThreadPanelLayoutStore::default();
        assert_eq!(layout.width(), px(340.));
        assert!(layout.select(ThreadPanelKind::Members, true));
        layout.width = px(412.);
        // Content bindings can replace A with B without a ThreadId in layout.
        let previous = (layout.selected(), layout.is_open(), layout.width());
        assert!(!layout.select(ThreadPanelKind::Members, true));
        assert_eq!(
            (layout.selected(), layout.is_open(), layout.width()),
            previous
        );
        assert!(layout.select(ThreadPanelKind::Artifacts, true));
        assert!(!layout.is_visible(ThreadPanelKind::Members));
        assert!(layout.is_visible(ThreadPanelKind::Artifacts));
        assert!(layout.select(ThreadPanelKind::Artifacts, false));
        assert_eq!(layout.selected(), ThreadPanelKind::Artifacts);
        assert_eq!(layout.width(), px(412.));
    }
}
