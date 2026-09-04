use gpui_kit::component::{Icon, IconNamed};
use gpui_kit::{AssetSource, IntoElement, Result, SharedString, prelude::*};
use rust_embed::RustEmbed;
use std::borrow::Cow;

#[derive(RustEmbed)]
#[folder = "assets"]
#[include = "dino-dark.webp"]
#[include = "dino-light.webp"]
#[include = "icons/**/*.svg"]
#[include = "logos/**/*.svg"]
struct PioneerAssets;

/// Asset source that serves Pioneer's custom icons,
/// falling back to GPUI Kit's bundled assets for built-in icons.
pub struct PioneerAssetsSource;

impl AssetSource for PioneerAssetsSource {
    fn load(&self, path: &str) -> Result<Option<Cow<'static, [u8]>>> {
        if path.is_empty() {
            return Ok(None);
        }

        if let Some(file) = PioneerAssets::get(path) {
            return Ok(Some(file.data));
        }

        gpui_kit::assets::Assets.load(path)
    }

    fn list(&self, path: &str) -> Result<Vec<SharedString>> {
        let mut items: Vec<SharedString> = PioneerAssets::iter()
            .filter(|p| p.starts_with(path))
            .map(|p| p.into())
            .collect();

        if let Ok(mut base_items) = gpui_kit::assets::Assets.list(path) {
            items.append(&mut base_items);
        }

        Ok(items)
    }
}

#[derive(IntoElement, Clone)]
pub enum PioneerIconName {
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

#[cfg(test)]
mod tests {
    use super::PioneerAssetsSource;
    use crate::file_opener::FILE_OPENER_CANDIDATES;
    use gpui_kit::AssetSource as _;

    #[test]
    fn serves_running_turn_dino_assets() {
        let assets = PioneerAssetsSource;
        assert!(assets.load("dino-dark.webp").unwrap().is_some());
        assert!(assets.load("dino-light.webp").unwrap().is_some());
    }

    #[test]
    fn serves_timeline_link_icon() {
        assert!(
            PioneerAssetsSource
                .load("icons/globe.svg")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn serves_every_file_opener_logo() {
        let assets = PioneerAssetsSource;
        for opener in FILE_OPENER_CANDIDATES {
            let Some(path) = opener.logo_path() else {
                continue;
            };
            assert!(assets.load(path).unwrap().is_some(), "missing logo: {path}");
        }

        for path in [
            "logos/editors/finder.svg",
            "logos/editors/explorer.svg",
            "logos/editors/files.svg",
        ] {
            assert!(assets.load(path).unwrap().is_some(), "missing logo: {path}");
        }
    }
}
