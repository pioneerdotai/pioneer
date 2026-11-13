use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

/// Shared unfilled native sidebar icon slot. Domain commands stay with callers.
pub fn catalog_sidebar_menu_icon(icon: impl Into<Icon>) -> Div {
    div()
        .size_6()
        .rounded_full()
        .flex()
        .items_center()
        .justify_center()
        .child(Icon::new(icon).size_4().opacity(0.8))
}

/// Shared native sidebar icon slot. The caller supplies domain meaning.
pub fn catalog_sidebar_icon(icon: impl Into<Icon>, filled: bool, cx: &App) -> Div {
    catalog_sidebar_menu_icon(icon)
        .when(filled, |view| view.bg(cx.theme().foreground.opacity(0.075)))
}
pub fn catalog_sidebar_label(label: String) -> Div {
    div().line_height(relative(1.)).opacity(0.8).child(label)
}

pub fn catalog_heading(title: String, description: String, cx: &App) -> AnyElement {
    v_flex()
        .child(div().text_xl().font_semibold().child(title))
        .child(
            div()
                .text_sm()
                .text_color(cx.theme().muted_foreground)
                .child(description),
        )
        .into_any_element()
}

/// Catalogue status geometry; label and semantic color belong to the domain.
/// Position and flex participation belong to the containing row or header.
pub fn catalog_status_badge(label: String, color: Hsla) -> Div {
    div()
        .px_2()
        .py_0p5()
        .rounded_full()
        .border_1()
        .border_color(color)
        .text_xs()
        .text_color(color)
        .font_medium()
        .child(label)
}

/// Shared list heading and summary band, with the original Skills catalogue styles.
pub fn catalog_list_header(
    title: String,
    description: String,
    count: String,
    cx: &App,
) -> AnyElement {
    v_flex()
        .pt_3()
        .px_6()
        .pb_3()
        .gap_2()
        .border_b_1()
        .border_color(cx.theme().border)
        .child(catalog_heading(title, description, cx))
        .child(
            h_flex()
                .items_center()
                .gap_2()
                .child(div().text_xs().opacity(0.6).child(count)),
        )
        .into_any_element()
}

/// Shared MCP-style empty catalogue. The caller owns permissions and the install command.
pub fn catalog_empty_state(
    title: String,
    description: String,
    install: Option<gpui_kit::component::button::Button>,
    cx: &App,
) -> AnyElement {
    v_flex()
        .w_full()
        .h_full()
        .items_center()
        .justify_center()
        .gap_4()
        .p_8()
        .bg(cx.theme().background)
        .when_some(install, |view, install| {
            view.child(
                install.child(
                    div()
                        .size_10()
                        .rounded_full()
                        .bg(cx.theme().foreground)
                        .flex()
                        .items_center()
                        .justify_center()
                        .text_color(cx.theme().primary_foreground)
                        .child(Icon::new(IconName::Plus).size_6()),
                ),
            )
        })
        .child(
            v_flex()
                .items_center()
                .justify_center()
                .gap_1()
                .child(div().text_base().font_semibold().child(title))
                .child(div().text_sm().opacity(0.6).child(description)),
        )
        .into_any_element()
}
