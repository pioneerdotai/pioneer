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
    div()
        .line_height(relative(1.))
        .opacity(0.8)
        .child(label)
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
