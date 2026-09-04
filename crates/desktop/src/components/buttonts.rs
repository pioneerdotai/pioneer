use gpui_kit::component::{
    Sizable,
    button::{Button, *},
};
use gpui_kit::prelude::*;

pub fn default_primary_button(id: impl Into<gpui_kit::ElementId>) -> Button {
    Button::new(id).small().primary().h_8().px_4()
}

pub fn default_outline_button(id: impl Into<gpui_kit::ElementId>) -> Button {
    Button::new(id).small().outline().h_8().px_4()
}

pub fn small_outline_button(id: impl Into<gpui_kit::ElementId>) -> Button {
    Button::new(id).small().outline().h_7().px_3()
}
