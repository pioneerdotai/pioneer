use gpui_kit::component::{Icon, IconNamed};
use gpui_kit::{IntoElement, SharedString, prelude::*};
#[derive(IntoElement, Clone)]
pub(crate) enum PioneerIconName {
    RefreshCw,
    Trash,
}
impl IconNamed for PioneerIconName {
    fn path(self) -> SharedString {
        match self {
            Self::RefreshCw => "icons/refresh-cw.svg",
            Self::Trash => "icons/trash.svg",
        }
        .into()
    }
}
impl RenderOnce for PioneerIconName {
    fn render(self, _: &mut gpui_kit::Window, _: &mut gpui_kit::App) -> impl IntoElement {
        Icon::new(self)
    }
}
