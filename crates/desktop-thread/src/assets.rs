use gpui_kit::component::{Icon, IconNamed};
use gpui_kit::{IntoElement, SharedString, prelude::*};
#[derive(IntoElement, Clone)]
pub(crate) enum PioneerIconName {
    AtSign,
    Clock,
    Copy,
    Eye,
    EyeOff,
    Globe,
    Infinity,
    Lightbulb,
    Mcp,
    MessageCircle,
    Microphone,
    Paperclip,
    Pen,
    Reply,
    RotateCcwClock,
    ShieldAlert,
    ShieldCheck,
    ShieldX,
    Square,
    SquarePen,
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
            Self::Clock => "icons/clock.svg",
            Self::Copy => "icons/copy.svg",
            Self::Eye => "icons/eye.svg",
            Self::EyeOff => "icons/eye-off.svg",
            Self::Globe => "icons/globe.svg",
            Self::Infinity => "icons/infinity.svg",
            Self::Lightbulb => "icons/lightbulb.svg",
            Self::Mcp => "icons/mcp.svg",
            Self::MessageCircle => "icons/message-circle.svg",
            Self::Microphone => "icons/microphone.svg",
            Self::Paperclip => "icons/paperclip.svg",
            Self::Pen => "icons/pen.svg",
            Self::Reply => "icons/reply.svg",
            Self::RotateCcwClock => "icons/rotate-ccw-clock.svg",
            Self::ShieldAlert => "icons/shield-alert.svg",
            Self::ShieldCheck => "icons/shield-check.svg",
            Self::ShieldX => "icons/shield-x.svg",
            Self::Square => "icons/square.svg",
            Self::SquarePen => "icons/square-pen.svg",
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
