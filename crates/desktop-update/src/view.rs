use crate::*;
use gpui_kit::component::{
    Icon, IconName, StyledExt, h_flex, spinner::Spinner, theme::ActiveTheme, v_flex,
};
use gpui_kit::{prelude::*, *};
use std::{path::PathBuf, sync::Arc};
actions!(
    desktop_update,
    [CheckForUpdate, DownloadUpdate, ApplyUpdate, CancelUpdate]
);
type ApplyError = Arc<dyn Fn(String, &mut Window, &mut App)>;
pub struct DesktopUpdateConfig {
    runtime_home: PathBuf,
    port: Arc<dyn DesktopUpdatePort>,
    ready: SharedString,
    downloading: SharedString,
    leaf: SharedString,
    apply_error: ApplyError,
}
impl DesktopUpdateConfig {
    pub fn new(
        runtime_home: PathBuf,
        port: Arc<dyn DesktopUpdatePort>,
        ready: SharedString,
        downloading: SharedString,
        leaf: SharedString,
        apply_error: impl Fn(String, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            runtime_home,
            port,
            ready,
            downloading,
            leaf,
            apply_error: Arc::new(apply_error),
        }
    }
}
pub struct DesktopUpdateView {
    controller: DesktopUpdateController,
    config: DesktopUpdateConfig,
    task: Option<Task<()>>,
    focus: FocusHandle,
}
impl DesktopUpdateView {
    pub fn new(config: DesktopUpdateConfig, cx: &mut Context<Self>) -> Self {
        Self {
            controller: DesktopUpdateController::new(config.runtime_home.clone()),
            config,
            task: None,
            focus: cx.focus_handle(),
        }
    }
    pub fn check(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(plan) = self.controller.check() {
            self.execute(plan, window, cx);
            cx.notify();
        }
    }
    fn apply(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(plan) = self.controller.apply() {
            self.execute(plan, window, cx);
        }
    }
    fn execute(&mut self, plan: DesktopUpdatePlan, window: &mut Window, cx: &mut Context<Self>) {
        let port = self.config.port.clone();
        self.task = Some(cx.spawn_in(window, async move |view, cx| {
            let completion = cx.background_spawn(async move { port.execute(plan) }).await;
            let _ = view.update_in(cx, |view, window, cx| {
                let transition = view.controller.complete(completion);
                if transition.changed {
                    cx.notify();
                    if let Some(error) = view.controller.error() {
                        if matches!(
                            view.controller.snapshot().as_ref(),
                            DesktopUpdateSnapshot::Ready { .. }
                        ) {
                            (view.config.apply_error)(error.to_owned(), window, cx);
                        }
                    }
                }
                if transition.relaunch {
                    cx.quit();
                }
                if let Some(next) = transition.next {
                    view.execute(next, window, cx);
                }
            });
        }));
    }
    pub fn close(&mut self) {
        self.controller.cancel();
        self.task.take();
    }
}
impl Drop for DesktopUpdateView {
    fn drop(&mut self) {
        self.close();
    }
}
impl Render for DesktopUpdateView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let snapshot = self.controller.snapshot();
        let panel = match snapshot.as_ref() {
            DesktopUpdateSnapshot::Ready { version, .. } => {
                let hover_bg = cx.theme().sidebar_accent;
                let version_label = if version.starts_with('v') {
                    version.clone()
                } else {
                    format!("v{version}")
                };
                h_flex()
                    .id("desktop-update-sidebar-ready")
                    .w_full()
                    .items_center()
                    .gap_4()
                    .px_3()
                    .py_2()
                    .rounded_xl()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().muted.opacity(0.35))
                    .cursor_pointer()
                    .hover(move |this| this.bg(hover_bg))
                    .child(Icon::empty().path(self.config.leaf.clone()).size_5())
                    .child(
                        v_flex()
                            .min_w_0()
                            .flex_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_semibold()
                                    .child(self.config.ready.clone()),
                            )
                            .child(div().text_xs().opacity(0.6).child(version_label)),
                    )
                    .child(Icon::new(IconName::ArrowRight).size_5().opacity(0.6))
                    .on_click(cx.listener(|view, _, window, cx| view.apply(window, cx)))
                    .into_any_element()
            }
            DesktopUpdateSnapshot::Downloading { .. } => h_flex()
                .id("desktop-update-sidebar-downloading")
                .w_full()
                .items_center()
                .gap_2()
                .px_3()
                .py_2()
                .rounded_xl()
                .border_1()
                .border_color(cx.theme().border)
                .bg(cx.theme().muted.opacity(0.35))
                .child(
                    div().size_5().flex().items_center().justify_center().child(
                        Spinner::new()
                            .icon(IconName::Loader)
                            .color(cx.theme().foreground.opacity(0.6)),
                    ),
                )
                .child(
                    div()
                        .min_w_0()
                        .flex_1()
                        .text_sm()
                        .opacity(0.6)
                        .child(self.config.downloading.clone()),
                )
                .into_any_element(),
            _ => return div().hidden().into_any_element(),
        };
        div()
            .track_focus(&self.focus)
            .flex_none()
            .px_2()
            .pb_2()
            .child(panel)
            .on_action(cx.listener(|view, _: &ApplyUpdate, window, cx| view.apply(window, cx)))
            .on_action(cx.listener(|view, _: &CheckForUpdate, window, cx| view.check(window, cx)))
            .on_action(cx.listener(|view, _: &DownloadUpdate, window, cx| view.check(window, cx)))
            .on_action(cx.listener(|view, _: &CancelUpdate, _, cx| {
                view.close();
                cx.notify();
            }))
            .into_any_element()
    }
}
