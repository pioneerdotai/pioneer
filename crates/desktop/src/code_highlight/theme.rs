use std::sync::OnceLock;

use syntect::highlighting::{Color, Theme};
use two_face::theme::{EmbeddedLazyThemeSet, EmbeddedThemeName};

use super::CodeThemeId;

static THEME_SET: OnceLock<EmbeddedLazyThemeSet> = OnceLock::new();

pub(super) fn theme(theme_id: CodeThemeId) -> &'static Theme {
    let themes = THEME_SET.get_or_init(two_face::theme::extra);
    let name = match theme_id {
        CodeThemeId::Light => EmbeddedThemeName::CatppuccinLatte,
        CodeThemeId::Dark => EmbeddedThemeName::CatppuccinFrappe,
    };
    themes.get(name)
}

pub(super) const fn render_background(theme_id: CodeThemeId) -> Color {
    match theme_id {
        CodeThemeId::Light => Color {
            r: 0xf5,
            g: 0xf5,
            b: 0xf5,
            a: 0xff,
        },
        CodeThemeId::Dark => Color {
            r: 0x26,
            g: 0x26,
            b: 0x26,
            a: 0xff,
        },
    }
}
