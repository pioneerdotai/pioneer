use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

/// Shared detail-page header. Callers retain domain state and action handlers.
#[derive(gpui_kit::IntoElement)]
pub struct CatalogDetailsHeader {
    icon: Option<AnyElement>,
    title: SharedString,
    owner: Option<SharedString>,
    description: Option<SharedString>,
    status: Option<(String, Hsla)>,
    actions: Option<AnyElement>,
}

impl CatalogDetailsHeader {
    pub fn new(title: impl Into<SharedString>) -> Self {
        Self {
            icon: None,
            title: title.into(),
            owner: None,
            description: None,
            status: None,
            actions: None,
        }
    }

    pub fn icon(mut self, icon: impl IntoElement) -> Self {
        self.icon = Some(icon.into_any_element());
        self
    }

    pub fn status(mut self, label: String, color: Hsla) -> Self {
        self.status = Some((label, color));
        self
    }

    pub fn owner(mut self, owner: impl Into<SharedString>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    pub fn description(mut self, description: impl Into<SharedString>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn actions(mut self, actions: impl IntoElement) -> Self {
        self.actions = Some(actions.into_any_element());
        self
    }
}

impl RenderOnce for CatalogDetailsHeader {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        v_flex()
            .pt_3()
            .px_6()
            .pb_5()
            .gap_4()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                h_flex()
                    .w_full()
                    .min_w_0()
                    .justify_between()
                    .items_start()
                    .gap_10()
                    .child(
                        h_flex()
                            .flex_1()
                            .min_w_0()
                            .items_start()
                            .gap_3()
                            .child(div().children(self.icon))
                            .child(
                                v_flex().flex_1().min_w_0().gap_3().child(
                                    v_flex()
                                        .w_full()
                                        .min_w_0()
                                        .child(
                                            h_flex()
                                                .w_full()
                                                .min_w_0()
                                                .items_baseline()
                                                .gap_1()
                                                .when_some(self.owner, |this, owner| {
                                                    this.child(
                                                        div()
                                                            .flex_none()
                                                            .text_base()
                                                            .font_medium()
                                                            .opacity(0.6)
                                                            .overflow_hidden()
                                                            .whitespace_nowrap()
                                                            .text_ellipsis()
                                                            .child(owner),
                                                    )
                                                    .child(
                                                        div()
                                                            .flex_none()
                                                            .text_base()
                                                            .opacity(0.6)
                                                            .child("/"),
                                                    )
                                                })
                                                .child(
                                                    div()
                                                        .min_w_0()
                                                        .text_base()
                                                        .font_semibold()
                                                        .overflow_hidden()
                                                        .whitespace_nowrap()
                                                        .text_ellipsis()
                                                        .child(self.title),
                                                ),
                                        )
                                        .when_some(self.description, |view, description| {
                                            view.child(
                                                div()
                                                    .w_full()
                                                    .min_w_0()
                                                    .text_sm()
                                                    .line_height(relative(1.35))
                                                    .opacity(0.6)
                                                    .overflow_hidden()
                                                    .whitespace_normal()
                                                    .line_clamp(3)
                                                    .child(description),
                                            )
                                        }),
                                ),
                            ),
                    )
                    .when_some(self.status, |row, (label, color)| {
                        row.child(
                            crate::catalog_status_badge(label, color)
                                .flex_none()
                                .mt_1p5(),
                        )
                    }),
            )
            .children(self.actions)
    }
}
