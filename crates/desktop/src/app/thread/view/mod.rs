mod approvals;
mod artifacts;
mod composer;
mod header;
pub(crate) mod timeline;

use super::super::root::PioneerDesktop;
use gpui::{prelude::*, *};
use gpui_component::{
    resizable::{h_resizable, resizable_panel},
    theme::ActiveTheme,
    *,
};

impl PioneerDesktop {
    pub(crate) fn render_thread(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        self.ensure_active_thread_artifacts_loaded(cx);
        let active_thread_id = self.current_active_thread_id().map(str::to_owned);

        let timeline_model = self.semantic_timeline_render_model(active_thread_id.as_deref());

        let desktop_entity = cx.entity().clone();
        let pending_requests = self.active_thread_pending_requests();
        let timeline = self.render_timeline(
            active_thread_id.as_deref(),
            timeline_model,
            pending_requests,
            window,
            cx,
        );
        let thread_body = v_flex()
            .size_full()
            .min_w_0()
            .min_h_0()
            .bg(cx.theme().background)
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .min_h_0()
                    .overflow_hidden()
                    .child(div().flex_1().min_h_0().child(timeline))
                    .child(self.render_composer(cx)),
            );

        let thread_split = h_resizable("thread-artifacts-layout")
            .on_resize({
                let desktop_entity = desktop_entity.clone();
                move |state, _, cx| {
                    let artifacts_width = state.read(cx).sizes().get(1).copied();
                    if let Some(artifacts_width) = artifacts_width {
                        let _ = desktop_entity.update(cx, |view, cx| {
                            view.thread_artifacts_sidebar_width = artifacts_width;
                            cx.notify();
                        });
                    }
                }
            })
            .child(
                resizable_panel()
                    .size_range(px(360.)..Pixels::MAX)
                    .child(thread_body),
            )
            .child(
                resizable_panel()
                    .visible(self.show_thread_artifacts_sidebar)
                    .size(self.thread_artifacts_sidebar_width)
                    .size_range(px(320.)..px(640.))
                    .child(self.render_thread_artifacts_panel(cx)),
            );

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .child(self.render_thread_header(cx))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_hidden()
                    .child(thread_split),
            )
            .into_any_element()
    }
}
