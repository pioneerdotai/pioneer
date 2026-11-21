use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

/// Shared geometry for skill and plugin catalogue items. Callers supply copy and navigation.
#[derive(gpui_kit::IntoElement)]
pub struct CatalogCard {
    icon: Option<AnyElement>,
    icon_size: Rems,
    title: String,
    owner: Option<String>,
    description: String,
    metadata: String,
    status: Option<(String, Hsla)>,
    pending: bool,
}

impl CatalogCard {
    pub fn new(title: String) -> Self {
        Self {
            icon: None,
            icon_size: rems(2.),
            title,
            owner: None,
            description: String::new(),
            metadata: String::new(),
            status: None,
            pending: false,
        }
    }
    pub fn icon(mut self, icon: impl IntoElement + Styled) -> Self {
        self.icon = Some(icon.size_full().into_any_element());
        self
    }

    /// Sets the icon's side length; the shared catalogue mask scales with it.
    pub fn icon_size(mut self, size: Rems) -> Self {
        self.icon_size = size;
        self
    }

    pub fn owner(mut self, owner: String) -> Self {
        self.owner = Some(owner);
        self
    }
    pub fn description(mut self, description: String) -> Self {
        self.description = description;
        self
    }
    pub fn metadata(mut self, metadata: String) -> Self {
        self.metadata = metadata;
        self
    }
    pub fn status(mut self, label: String, color: Hsla) -> Self {
        self.status = Some((label, color));
        self
    }
    pub fn pending(mut self, pending: bool) -> Self {
        self.pending = pending;
        self
    }
}
impl RenderOnce for CatalogCard {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        h_flex()
            .id("catalog-card")
            .w_full()
            .min_w_0()
            .pt_3()
            .px_4()
            .pb_3()
            .rounded_lg()
            .overflow_hidden()
            .bg(cx.theme().background)
            .border_1()
            .border_color(cx.theme().border)
            .gap_4()
            .items_start()
            .hover(|this| this.bg(cx.theme().secondary.opacity(0.45)))
            .when_some(self.icon, |view, icon| {
                view.child(
                    div()
                        .mt_1()
                        .child(crate::catalog_icon::catalog_icon(icon, self.icon_size)),
                )
            })
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .gap_3()
                    .child(
                        v_flex()
                            .gap_1()
                            .child(
                                h_flex()
                                    .w_full()
                                    .min_w_0()
                                    .justify_between()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        h_flex()
                                            .min_w_0()
                                            .items_center()
                                            .gap_1()
                                            .when_some(self.owner.as_deref(), |this, owner| {
                                                this.child(
                                                    div()
                                                        .flex_none()
                                                        .text_sm()
                                                        .opacity(0.6)
                                                        .child(owner.to_owned()),
                                                )
                                                .child(
                                                    div()
                                                        .flex_none()
                                                        .text_sm()
                                                        .opacity(0.6)
                                                        .child("/"),
                                                )
                                            })
                                            .child(
                                                div()
                                                    .min_w_0()
                                                    .text_sm()
                                                    .font_semibold()
                                                    .overflow_hidden()
                                                    .whitespace_nowrap()
                                                    .text_ellipsis()
                                                    .child(self.title),
                                            ),
                                    ),
                            )
                            .child(
                                div()
                                    .w_full()
                                    .min_w_0()
                                    .text_xs()
                                    .line_height(relative(1.3))
                                    .opacity(0.6)
                                    .overflow_hidden()
                                    .whitespace_normal()
                                    .child(self.description),
                            ),
                    )
                    .child(
                        h_flex()
                            .w_full()
                            .min_w_0()
                            .justify_between()
                            .items_center()
                            .gap_2()
                            .child(
                                h_flex()
                                    .min_w_0()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        div()
                                            .min_w_0()
                                            .text_xs()
                                            .opacity(0.6)
                                            .overflow_hidden()
                                            .whitespace_nowrap()
                                            .text_ellipsis()
                                            .child(self.metadata),
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
            .when_some(self.status, |card, (label, color)| {
                card.child(
                    crate::catalog_status_badge(label, color)
                        .flex_none()
                        .mt_1p5(),
                )
            })
    }
}
