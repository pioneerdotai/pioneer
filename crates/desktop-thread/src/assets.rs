use gpui_kit::component::{Icon, IconNamed};
use gpui_kit::{IntoElement, SharedString, prelude::*};
#[derive(IntoElement, Clone)]
pub(crate) enum PioneerIconName {
    AtSign,
    Bolt,
    Clock,
    Copy,
    Eye,
    EyeOff,
    FolderPlus,
    FolderTree,
    GalleryVerticalEnd,
    Globe,
    Infinity,
    Leaf,
    Lightbulb,
    Mcp,
    MessageCircle,
    Microphone,
    Paperclip,
    Pen,
    PowerOff,
    RefreshCw,
    Reply,
    RotateCcw,
    RotateCcwClock,
    ShieldAlert,
    ShieldCheck,
    ShieldX,
    Square,
    SquarePen,
    SunMoon,
    Terminal,
    Trash,
    UserCheck,
    Users,
    Zap,
}

impl IconNamed for PioneerIconName {
    fn path(self) -> SharedString {
        match self {
            Self::AtSign => "icons/at-sign.svg",
            Self::Bolt => "icons/bolt.svg",
            Self::Clock => "icons/clock.svg",
            Self::Copy => "icons/copy.svg",
            Self::Eye => "icons/eye.svg",
            Self::EyeOff => "icons/eye-off.svg",
            Self::FolderPlus => "icons/folder-plus.svg",
            Self::FolderTree => "icons/folder-tree.svg",
            Self::GalleryVerticalEnd => "icons/gallery-vertical-end.svg",
            Self::Globe => "icons/globe.svg",
            Self::Infinity => "icons/infinity.svg",
            Self::Leaf => "icons/leaf.svg",
            Self::Lightbulb => "icons/lightbulb.svg",
            Self::Mcp => "icons/mcp.svg",
            Self::MessageCircle => "icons/message-circle.svg",
            Self::Microphone => "icons/microphone.svg",
            Self::Paperclip => "icons/paperclip.svg",
            Self::Pen => "icons/pen.svg",
            Self::PowerOff => "icons/power-off.svg",
            Self::RefreshCw => "icons/refresh-cw.svg",
            Self::Reply => "icons/reply.svg",
            Self::RotateCcw => "icons/rotate-ccw.svg",
            Self::RotateCcwClock => "icons/rotate-ccw-clock.svg",
            Self::ShieldAlert => "icons/shield-alert.svg",
            Self::ShieldCheck => "icons/shield-check.svg",
            Self::ShieldX => "icons/shield-x.svg",
            Self::Square => "icons/square.svg",
            Self::SquarePen => "icons/square-pen.svg",
            Self::SunMoon => "icons/sun-moon.svg",
            Self::Terminal => "icons/terminal.svg",
            Self::Trash => "icons/trash.svg",
            Self::UserCheck => "icons/user-check.svg",
            Self::Users => "icons/users.svg",
            Self::Zap => "icons/zap.svg",
        }
        .into()
    }
}

impl RenderOnce for PioneerIconName {
    fn render(self, _: &mut gpui_kit::Window, _: &mut gpui_kit::App) -> impl IntoElement {
        Icon::new(self)
    }
}
