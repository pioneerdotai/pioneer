use anyhow::{Context as _, Result};
use gpui::{App, Global};
use pioneer_config::AppConfig;
use serde::{Deserialize, Serialize};
use std::{env, fs, path::PathBuf};

const DESKTOP_SETTINGS_FILE_NAME: &str = "desktop-settings.toml";
const DESKTOP_SETTINGS_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DesktopSettingsFile {
    #[serde(default = "default_settings_version")]
    version: u32,
    #[serde(default)]
    general: GeneralSettings,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WindowOpenState {
    Windowed,
    Maximized,
    Fullscreen,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WindowThemePreference {
    #[default]
    System,
    Light,
    Dark,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AppLanguagePreference {
    #[default]
    System,
    English,
    Russian,
    Chinese,
    Hindi,
    Spanish,
    German,
    French,
    Japanese,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct GeneralSettings {
    #[serde(default)]
    language: AppLanguagePreference,
    #[serde(default)]
    theme: WindowThemePreference,
}

struct DesktopSettingsState {
    path: PathBuf,
    settings: DesktopSettingsFile,
}

impl Global for DesktopSettingsState {}

const fn default_settings_version() -> u32 {
    DESKTOP_SETTINGS_VERSION
}

pub(crate) fn ensure_loaded(cx: &mut App) -> Result<()> {
    if cx.has_global::<DesktopSettingsState>() {
        return Ok(());
    }

    cx.set_global(DesktopSettingsState::load()?);
    Ok(())
}

pub(crate) fn window_theme(cx: &App) -> WindowThemePreference {
    cx.try_global::<DesktopSettingsState>()
        .map(|state| state.settings.general.theme)
        .unwrap_or_default()
}

pub(crate) fn app_language(cx: &App) -> AppLanguagePreference {
    cx.try_global::<DesktopSettingsState>()
        .map(|state| state.settings.general.language)
        .unwrap_or_default()
}

pub(crate) fn set_app_language(cx: &mut App, language: AppLanguagePreference) -> Result<()> {
    ensure_loaded(cx)?;

    let (path, serialized) = {
        let state = cx.global_mut::<DesktopSettingsState>();
        state.settings.version = DESKTOP_SETTINGS_VERSION;
        state.settings.general.language = language;
        (state.path.clone(), serialize_settings(&state.settings)?)
    };

    write_settings_file(path.as_path(), serialized)?;
    Ok(())
}

pub(crate) fn set_window_theme(cx: &mut App, theme: WindowThemePreference) -> Result<()> {
    ensure_loaded(cx)?;

    let (path, serialized) = {
        let state = cx.global_mut::<DesktopSettingsState>();
        state.settings.version = DESKTOP_SETTINGS_VERSION;
        state.settings.general.theme = theme;
        (state.path.clone(), serialize_settings(&state.settings)?)
    };

    write_settings_file(path.as_path(), serialized)?;
    Ok(())
}

pub(crate) fn load_app_language_preference() -> Option<AppLanguagePreference> {
    let settings = load_settings_file().ok()?;
    Some(settings.general.language)
}

pub(crate) fn resolve_app_locale() -> String {
    load_app_language_preference()
        .unwrap_or_default()
        .resolve_locale()
}

impl AppLanguagePreference {
    pub(crate) fn resolve_locale(self) -> String {
        if let Some(locale) = self.explicit_locale() {
            return locale.to_owned();
        }

        detect_system_locale()
            .map(str::to_owned)
            .unwrap_or_else(|| "en".to_owned())
    }

    fn explicit_locale(self) -> Option<&'static str> {
        match self {
            Self::System => None,
            Self::English => Some("en"),
            Self::Russian => Some("ru"),
            Self::Chinese => Some("zh"),
            Self::Hindi => Some("hi"),
            Self::Spanish => Some("es"),
            Self::German => Some("de"),
            Self::French => Some("fr"),
            Self::Japanese => Some("jp"),
        }
    }
}

impl DesktopSettingsState {
    fn load() -> Result<Self> {
        let path = settings_path()?;

        let settings = if path.is_file() {
            load_settings_file_from_path(path.as_path())?
        } else {
            DesktopSettingsFile {
                version: DESKTOP_SETTINGS_VERSION,
                ..Default::default()
            }
        };

        Ok(Self { path, settings })
    }
}

fn settings_path() -> Result<PathBuf> {
    let config = AppConfig::load().context("failed to load app config for desktop settings")?;
    let runtime_home = config
        .ensure_runtime_home_dir()
        .context("failed to ensure runtime home dir for desktop settings")?;
    Ok(runtime_home.join(DESKTOP_SETTINGS_FILE_NAME))
}

fn load_settings_file() -> Result<DesktopSettingsFile> {
    let path = settings_path()?;
    if !path.is_file() {
        return Ok(DesktopSettingsFile {
            version: DESKTOP_SETTINGS_VERSION,
            ..Default::default()
        });
    }

    load_settings_file_from_path(path.as_path())
}

fn load_settings_file_from_path(path: &std::path::Path) -> Result<DesktopSettingsFile> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read desktop settings `{}`", path.display()))?;
    toml::from_str::<DesktopSettingsFile>(raw.as_str())
        .with_context(|| format!("failed to parse desktop settings `{}`", path.display()))
}

fn serialize_settings(settings: &DesktopSettingsFile) -> Result<String> {
    toml::to_string_pretty(settings).context("failed to serialize desktop settings")
}

fn write_settings_file(path: &std::path::Path, serialized: String) -> Result<()> {
    fs::write(path, serialized)
        .with_context(|| format!("failed to write desktop settings `{}`", path.display()))
}

fn detect_system_locale() -> Option<&'static str> {
    if let Some(system_locale) = sys_locale::get_locale()
        && let Some(locale) = normalize_locale(system_locale.as_str())
    {
        return Some(locale);
    }

    for env_var in ["LC_ALL", "LC_MESSAGES", "LANG"] {
        if let Ok(value) = env::var(env_var)
            && let Some(locale) = normalize_locale(value.as_str())
        {
            return Some(locale);
        }
    }

    None
}

fn normalize_locale(raw: &str) -> Option<&'static str> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let lowered = raw.to_ascii_lowercase();
    let lowered = lowered
        .split('.')
        .next()
        .unwrap_or(lowered.as_str())
        .split('@')
        .next()
        .unwrap_or(lowered.as_str());
    let language = lowered.split(['-', '_']).next().unwrap_or(lowered);

    match language {
        "en" => Some("en"),
        "ru" => Some("ru"),
        "zh" => Some("zh"),
        "hi" => Some("hi"),
        "es" => Some("es"),
        "de" => Some("de"),
        "fr" => Some("fr"),
        "ja" | "jp" => Some("jp"),
        _ => None,
    }
}
