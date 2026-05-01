use crate::settings::{self, WindowThemePreference};
use std::path::PathBuf;

use gpui::{App, SharedString};
use gpui_component::{Theme, ThemeMode, ThemeRegistry};

const THEME_LIGHT: &str = "Pioneer Light";
const THEME_DARK: &str = "Pioneer Dark";

pub fn init(cx: &mut App) {
    if let Err(error) = settings::ensure_loaded(cx) {
        tracing::warn!(
            error = %format!("{error:#}"),
            "failed to load desktop settings; using system theme"
        );
    }

    let themes_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("themes");

    let light = SharedString::from(THEME_LIGHT);
    let dark = SharedString::from(THEME_DARK);

    if let Err(err) = ThemeRegistry::watch_dir(themes_dir, cx, move |cx| {
        let light_theme = ThemeRegistry::global(cx).themes().get(&light).cloned();
        let dark_theme = ThemeRegistry::global(cx).themes().get(&dark).cloned();

        if let Some(theme) = light_theme {
            Theme::global_mut(cx).apply_config(&theme);
        }
        if let Some(theme) = dark_theme {
            Theme::global_mut(cx).apply_config(&theme);
        }

        apply_theme_preference(cx);
    }) {
        tracing::error!(
            error = %err,
            message = %t!("errors.theme.watch_dir_failed")
        );
    }
}

fn apply_theme_preference(cx: &mut App) {
    match settings::window_theme(cx) {
        WindowThemePreference::System => Theme::sync_system_appearance(None, cx),
        WindowThemePreference::Light => Theme::change(ThemeMode::Light, None, cx),
        WindowThemePreference::Dark => Theme::change(ThemeMode::Dark, None, cx),
    }
}
