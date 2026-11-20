use gpui_kit::{prelude::*, *};

const ICON_RADIUS_RATIO: f32 = 0.42;

/// One icon mask for catalogue lists and detail pages, scaled with the slot.
pub(crate) fn catalog_icon(icon: impl IntoElement, size: Rems) -> Div {
    div()
        .size(size)
        .flex_none()
        .rounded(size * ICON_RADIUS_RATIO)
        .overflow_hidden()
        .child(icon)
}
