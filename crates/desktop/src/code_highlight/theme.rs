use std::sync::OnceLock;

use syntect::highlighting::Theme;
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
