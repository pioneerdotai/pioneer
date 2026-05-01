use gpui::{AssetSource, IntoElement, Result, SharedString, prelude::*};
use gpui_component::{Icon, IconNamed};
use rust_embed::RustEmbed;
use std::borrow::Cow;

#[derive(RustEmbed)]
#[folder = "assets"]
#[include = "icons/**/*.svg"]
#[include = "logos/**/*.svg"]
struct PioneerAssets;

/// Asset source that serves Pioneer's custom icons,
/// falling back to gpui-component-assets for built-in icons.
pub struct PioneerAssetsSource;

impl AssetSource for PioneerAssetsSource {
    fn load(&self, path: &str) -> Result<Option<Cow<'static, [u8]>>> {
        if path.is_empty() {
            return Ok(None);
        }

        if let Some(file) = PioneerAssets::get(path) {
            return Ok(Some(file.data));
        }

        gpui_component_assets::Assets.load(path)
    }

    fn list(&self, path: &str) -> Result<Vec<SharedString>> {
        let mut items: Vec<SharedString> = PioneerAssets::iter()
            .filter(|p| p.starts_with(path))
            .map(|p| p.into())
            .collect();

        if let Ok(mut base_items) = gpui_component_assets::Assets.list(path) {
            items.append(&mut base_items);
        }

        Ok(items)
    }
}

#[derive(IntoElement, Clone)]
pub enum PioneerIconName {
    Bolt,
    FolderPlus,
    FolderTree,
    Infinity,
    Lightbulb,
    Mcp,
    MessageCircle,
    RefreshCw,
    RotateCcw,
    SunMoon,
    Terminal,
    Trash,
    Zap,
}

impl IconNamed for PioneerIconName {
    fn path(self) -> SharedString {
        match self {
            Self::Bolt => "icons/bolt.svg",
            Self::FolderPlus => "icons/folder-plus.svg",
            Self::FolderTree => "icons/folder-tree.svg",
            Self::Infinity => "icons/infinity.svg",
            Self::Lightbulb => "icons/lightbulb.svg",
            Self::Mcp => "icons/mcp.svg",
            Self::MessageCircle => "icons/message-circle.svg",
            Self::RefreshCw => "icons/refresh-cw.svg",
            Self::RotateCcw => "icons/rotate-ccw.svg",
            Self::SunMoon => "icons/sun-moon.svg",
            Self::Terminal => "icons/terminal.svg",
            Self::Trash => "icons/trash.svg",
            Self::Zap => "icons/zap.svg",
        }
        .into()
    }
}

impl RenderOnce for PioneerIconName {
    fn render(self, _: &mut gpui::Window, _: &mut gpui::App) -> impl IntoElement {
        Icon::new(self)
    }
}
