use gpui_kit::component::{Icon, IconNamed};
use gpui_kit::{IntoElement, SharedString, prelude::*};
#[derive(IntoElement, Clone)]
pub(crate) enum PioneerIconName {
    Terminal,
    Bolt,
    RefreshCw,
}
impl IconNamed for PioneerIconName {
    fn path(self) -> SharedString {
        match self {
            Self::Terminal => "icons/terminal.svg",
            Self::Bolt => "icons/bolt.svg",
            Self::RefreshCw => "icons/refresh-cw.svg",
        }
        .into()
    }
}
impl RenderOnce for PioneerIconName {
    fn render(self, _: &mut gpui_kit::Window, _: &mut gpui_kit::App) -> impl IntoElement {
        Icon::new(self)
    }
}
