use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

/// Full-width settings presentation; callers own copy, controls and policies.
pub fn settings_section(title: impl Into<SharedString>, content: impl IntoElement) -> Div {
    v_flex()
        .w_full()
        .min_w_0()
        .gap_4()
        .child(div().text_xl().font_semibold().child(title.into()))
        .child(content)
}

pub fn settings_group(cx: &App) -> Div {
    v_flex()
        .w_full()
        .min_w_0()
        .gap_0()
        .px_4()
        .py_0()
        .rounded_lg()
        .border_1()
        .border_color(cx.theme().border)
        .bg(cx.theme().background)
}

pub fn settings_row(
    label: impl Into<SharedString>,
    description: impl Into<SharedString>,
    control: impl IntoElement,
) -> Div {
    h_flex()
        .w_full()
        .gap_6()
        .py_3()
        .justify_between()
        .items_center()
        .child(
            v_flex()
                .min_w_0()
                .flex_1()
                .child(div().text_sm().font_semibold().child(label.into()))
                .child(div().text_xs().opacity(0.6).child(description.into())),
        )
        .child(div().flex_none().child(control))
}

pub fn settings_divider(cx: &App) -> Div {
    div().w_full().border_t_1().border_color(cx.theme().border)
}
