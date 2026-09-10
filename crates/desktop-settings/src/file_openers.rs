use crate::buttons::small_outline_button;
use gpui_kit::component::{button::Button, *};
use gpui_kit::{prelude::*, *};
use pioneer_desktop_foundation::file_opener::FileOpenerId;
pub(crate) fn file_opener_icon(opener: FileOpenerId) -> AnyElement {
    if let Some(path) = opener.logo_path() {
        if matches!(opener, FileOpenerId::Cursor | FileOpenerId::Zed) {
            Icon::empty().path(path).size_3p5().into_any_element()
        } else {
            img(path).size_3p5().flex_none().into_any_element()
        }
    } else {
        Icon::new(IconName::Folder).size_3p5().into_any_element()
    }
}

pub(crate) fn file_opener_trigger(id: impl Into<ElementId>, opener: FileOpenerId) -> Button {
    small_outline_button(id).compact().child(
        h_flex()
            .w_full()
            .items_center()
            .justify_between()
            .gap_2()
            .child(file_opener_icon(opener))
            .child(div().text_sm().child(opener.label()))
            .child(Icon::new(IconName::ChevronsUpDown).size_3p5()),
    )
}

pub(crate) fn file_opener_menu_row(
    opener: FileOpenerId,
    label: SharedString,
    selected: bool,
    cx: &App,
) -> AnyElement {
    let hover_background = cx.theme().accent;
    let selected_background = cx.theme().popover.blend(hover_background.opacity(0.88));

    h_flex()
        .flex_1()
        .h(px(26.))
        .mx_neg_2()
        .px_2()
        .rounded(cx.theme().radius.min(px(8.)))
        .items_center()
        .gap_2()
        .text_sm()
        .when(selected, |row| {
            row.bg(selected_background)
                .text_color(cx.theme().accent_foreground)
                .hover(move |row| row.bg(hover_background))
        })
        .child(file_opener_icon(opener))
        .child(label)
        .into_any_element()
}
