use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

/// Compact catalogue geometry, shared by native MCP lists and plugin components.
#[derive(gpui_kit::IntoElement)]
pub struct CatalogSummaryCard {
    title: String,
    capability_labels: Vec<String>,
    status_label: String,
    status_color: Hsla,
    pending: bool,
}
impl CatalogSummaryCard {
    pub const HEIGHT: f32 = 78.0;
    pub fn new(
        title: String,
        capability_labels: Vec<String>,
        status_label: String,
        status_color: Hsla,
    ) -> Self {
        Self {
            title,
            capability_labels,
            status_label,
            status_color,
            pending: false,
        }
    }
    pub fn pending(mut self, pending: bool) -> Self {
        self.pending = pending;
        self
    }
    pub fn status(mut self, label: String, color: Hsla) -> Self {
        self.status_label = label;
        self.status_color = color;
        self
    }
}
impl RenderOnce for CatalogSummaryCard {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        h_flex()
            .w_full()
            .h(px(Self::HEIGHT))
            .pt_3()
            .px_4()
            .pb_3()
            .rounded_lg()
            .bg(cx.theme().background)
            .border_1()
            .border_color(cx.theme().border)
            .gap_4()
            .items_start()
            .hover(|this| this.bg(cx.theme().secondary.opacity(0.45)))
            .child(
                v_flex()
                    .flex_1()
                    .h_full()
                    .gap_1()
                    .child(
                        h_flex()
                            .w_full()
                            .justify_between()
                            .items_center()
                            .gap_2()
                            .child(div().text_sm().font_semibold().child(self.title)),
                    )
                    .child(div().flex_1())
                    .child(
                        h_flex().justify_between().items_center().gap_2().child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .children(
                                    self.capability_labels
                                        .into_iter()
                                        .map(|label| div().text_xs().opacity(0.6).child(label)),
                                )
                                .when(self.pending, |this| {
                                    this.child(
                                        Icon::default()
                                            .path("icons/refresh-cw.svg")
                                            .size_3()
                                            .text_color(cx.theme().warning),
                                    )
                                }),
                        ),
                    ),
            )
            .child(crate::catalog_status_badge(self.status_label, self.status_color).mt_1p5())
    }
}
