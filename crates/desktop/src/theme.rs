use crate::settings::{self, WindowThemePreference};
use anyhow::{Context as _, Result};
use pioneer_config::AppConfig;
use std::{fs, path::PathBuf};

use gpui::{App, SharedString};
use gpui_component::{Theme, ThemeMode, ThemeRegistry};

const THEME_LIGHT: &str = "Pioneer Light";
const THEME_DARK: &str = "Pioneer Dark";
const BUNDLED_THEME_FILE_NAME: &str = "pioneer.json";
const BUNDLED_THEME: &str = include_str!("../themes/pioneer.json");

pub fn init(cx: &mut App) {
    if let Err(error) = settings::ensure_loaded(cx) {
        tracing::warn!(
            error = %format!("{error:#}"),
            "failed to load desktop settings; using system theme"
        );
    }

    let themes_dir = themes_dir();

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

fn themes_dir() -> PathBuf {
    let source_themes_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("themes");

    if cfg!(debug_assertions) {
        return source_themes_dir;
    }

    match write_bundled_theme() {
        Ok(themes_dir) => themes_dir,
        Err(error) => {
            tracing::warn!(
                error = %format!("{error:#}"),
                "failed to prepare bundled desktop theme; falling back to source theme path"
            );
            source_themes_dir
        }
    }
}

fn write_bundled_theme() -> Result<PathBuf> {
    let config = AppConfig::load().context("failed to load app config for bundled theme")?;
    let runtime_home = config
        .ensure_runtime_home_dir()
        .context("failed to ensure runtime home dir for bundled theme")?;
    let themes_dir = runtime_home.join("themes");
    fs::create_dir_all(&themes_dir)
        .with_context(|| format!("failed to create themes dir `{}`", themes_dir.display()))?;

    let theme_path = themes_dir.join(BUNDLED_THEME_FILE_NAME);
    let needs_write = fs::read_to_string(&theme_path)
        .map(|existing| existing != BUNDLED_THEME)
        .unwrap_or(true);

    if needs_write {
        fs::write(&theme_path, BUNDLED_THEME)
            .with_context(|| format!("failed to write bundled theme `{}`", theme_path.display()))?;
    }

    Ok(themes_dir)
}

fn apply_theme_preference(cx: &mut App) {
    match settings::window_theme(cx) {
        WindowThemePreference::System => Theme::sync_system_appearance(None, cx),
        WindowThemePreference::Light => Theme::change(ThemeMode::Light, None, cx),
        WindowThemePreference::Dark => Theme::change(ThemeMode::Dark, None, cx),
    }
}
