use crate::{
    artifacts::ThreadArtifactsView,
    members::ThreadMembersView,
    panel_layout::{ThreadPanelKind, ThreadPanelLayoutStore, ThreadPanelOpened},
};
use gpui_kit::{prelude::*, *};

/// Owns content handles only. The window's layout entity is the sole owner of
/// selection, visibility and width across replacement of mounted threads.
pub(crate) struct ThreadSidePanelHostView {
    layout: Entity<ThreadPanelLayoutStore>,
    artifacts: Entity<ThreadArtifactsView>,
    members: Entity<ThreadMembersView>,
    _subscriptions: Vec<Subscription>,
}
impl ThreadSidePanelHostView {
    pub(crate) fn new(
        layout: Entity<ThreadPanelLayoutStore>,
        artifacts: Entity<ThreadArtifactsView>,
        members: Entity<ThreadMembersView>,
        cx: &mut Context<Self>,
    ) -> Self {
        let subscriptions = vec![
            cx.observe(&layout, |view, _, cx| {
                view.synchronize(cx);
                cx.notify();
            }),
            cx.subscribe(&layout, |view, _, event: &ThreadPanelOpened, cx| {
                if event.0 == ThreadPanelKind::Members {
                    view.members.read(cx).retry();
                }
            }),
        ];
        let mut view = Self {
            layout,
            artifacts,
            members,
            _subscriptions: subscriptions,
        };
        view.synchronize(cx);
        if view.layout.read(cx).is_visible(ThreadPanelKind::Members) {
            view.members.read(cx).observe();
        }
        view
    }
    pub(crate) fn set_route_visible(&self, visible: bool, cx: &mut Context<Self>) {
        self.members.read(cx).set_visible(visible);
        self.artifacts
            .update(cx, |view, cx| view.set_route_visible(visible, cx));
    }
    fn synchronize(&mut self, cx: &mut Context<Self>) {
        let visible = self.layout.read(cx).is_visible(ThreadPanelKind::Artifacts);
        self.artifacts
            .update(cx, |view, cx| view.set_visible(visible, cx));
    }
    pub(crate) fn open_artifact(&mut self, artifact_id: String, cx: &mut Context<Self>) {
        self.artifacts
            .update(cx, |view, cx| view.select_thread_artifact(artifact_id, cx));
        self.layout
            .update(cx, |layout, cx| layout.open(ThreadPanelKind::Artifacts, cx));
    }
}
impl Render for ThreadSidePanelHostView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        match self.layout.read(cx).selected() {
            ThreadPanelKind::Artifacts => self.artifacts.clone().into_any_element(),
            ThreadPanelKind::Members => self.members.clone().into_any_element(),
        }
    }
}
