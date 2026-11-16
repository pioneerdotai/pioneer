use gpui_kit::component::{Icon, IconNamed};
use gpui_kit::{IntoElement, SharedString, prelude::*};

#[derive(IntoElement, Clone)]
pub(crate) enum PioneerIconName {
    ArrowUpRight,
}

impl IconNamed for PioneerIconName {
    fn path(self) -> SharedString {
        match self {
            Self::ArrowUpRight => "icons/arrow-up-right.svg",
        }
        .into()
    }
}

impl RenderOnce for PioneerIconName {
    fn render(self, _: &mut gpui_kit::Window, _: &mut gpui_kit::App) -> impl IntoElement {
        Icon::new(self)
    }
}
