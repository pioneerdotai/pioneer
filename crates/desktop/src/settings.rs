use anyhow::{Context as _, Result};
use gpui::{App, Global};
use pioneer_config::AppConfig;
use pioneer_gateway::{
    GatewayMemorySettings, load_or_create_gateway_settings, normalize_settings_file_name,
    save_gateway_settings,
};
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

#[derive(Debug, Clone, Deserialize, Default)]
struct LegacyDesktopSettingsFile {
    #[serde(default)]
    memory: Option<MemorySettings>,
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

pub(crate) type MemorySettings = GatewayMemorySettings;

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

pub(crate) fn memory_settings(_cx: &App) -> MemorySettings {
    load_memory_settings().unwrap_or_default()
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

pub(crate) fn set_memory_settings(_cx: &mut App, memory: MemorySettings) -> Result<()> {
    let config = AppConfig::load().context("failed to load app config for gateway settings")?;
    let path = gateway_settings_path(&config)?;
    let settings_file_name =
        normalize_settings_file_name(config.gateway.settings_file_name.as_str())?;
    let mut settings = load_or_create_gateway_settings(
        path.as_path(),
        config.gateway.settings_version,
        settings_file_name.as_str(),
    )?;
    settings.set_memory_settings(memory);
    save_gateway_settings(path.as_path(), &settings)?;
    Ok(())
}

pub(crate) fn load_app_language_preference() -> Option<AppLanguagePreference> {
    let settings = load_settings_file().ok()?;
    Some(settings.general.language)
}

pub(crate) fn load_memory_settings() -> Result<MemorySettings> {
    let config = AppConfig::load().context("failed to load app config for gateway settings")?;
    let path = gateway_settings_path(&config)?;
    let settings_file_name =
        normalize_settings_file_name(config.gateway.settings_file_name.as_str())?;
    let settings = load_or_create_gateway_settings(
        path.as_path(),
        config.gateway.settings_version,
        settings_file_name.as_str(),
    )?;
    Ok(settings.effective_memory_settings(&config.gateway.memory))
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
            let raw = fs::read_to_string(path.as_path())
                .with_context(|| format!("failed to read desktop settings `{}`", path.display()))?;
            let settings = parse_settings_file(path.as_path(), raw.as_str())?;
            migrate_legacy_desktop_memory_settings(path.as_path(), raw.as_str(), &settings)?;
            settings
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

fn gateway_settings_path(config: &AppConfig) -> Result<PathBuf> {
    let runtime_home = config
        .ensure_runtime_home_dir()
        .context("failed to ensure runtime home dir for gateway settings")?;
    let settings_file_name =
        normalize_settings_file_name(config.gateway.settings_file_name.as_str())?;
    Ok(runtime_home.join(settings_file_name))
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
    parse_settings_file(path, raw.as_str())
}

fn parse_settings_file(path: &std::path::Path, raw: &str) -> Result<DesktopSettingsFile> {
    toml::from_str::<DesktopSettingsFile>(raw)
        .with_context(|| format!("failed to parse desktop settings `{}`", path.display()))
}

fn serialize_settings(settings: &DesktopSettingsFile) -> Result<String> {
    toml::to_string_pretty(settings).context("failed to serialize desktop settings")
}

fn write_settings_file(path: &std::path::Path, serialized: String) -> Result<()> {
    fs::write(path, serialized)
        .with_context(|| format!("failed to write desktop settings `{}`", path.display()))
}

fn migrate_legacy_desktop_memory_settings(
    path: &std::path::Path,
    raw: &str,
    settings: &DesktopSettingsFile,
) -> Result<()> {
    let Some(memory) = toml::from_str::<LegacyDesktopSettingsFile>(raw)
        .ok()
        .and_then(|legacy| legacy.memory)
    else {
        return Ok(());
    };

    let config = AppConfig::load().context("failed to load app config for gateway settings")?;
    let gateway_path = gateway_settings_path(&config)?;
    let settings_file_name =
        normalize_settings_file_name(config.gateway.settings_file_name.as_str())?;
    let mut gateway_settings = load_or_create_gateway_settings(
        gateway_path.as_path(),
        config.gateway.settings_version,
        settings_file_name.as_str(),
    )?;
    if !gateway_settings.has_memory_settings() {
        gateway_settings.set_memory_settings(memory);
        save_gateway_settings(gateway_path.as_path(), &gateway_settings)?;
    }

    write_settings_file(path, serialize_settings(settings)?)?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::{DesktopSettingsFile, load_settings_file_from_path, serialize_settings};
    use std::fs;

    #[test]
    fn desktop_settings_do_not_own_memory_settings() {
        let settings = toml::from_str::<DesktopSettingsFile>(
            r#"
version = 1

[general]
theme = "dark"

[memory]
enabled = false
"#,
        )
        .expect("desktop settings should parse");

        let serialized = serialize_settings(&settings).expect("settings should serialize");
        assert!(!serialized.contains("[memory]"));
        assert!(serialized.contains("[general]"));
    }

    #[test]
    fn desktop_settings_load_from_path_ignores_legacy_memory_table() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("desktop-settings.toml");
        fs::write(
            path.as_path(),
            r#"
version = 1

[memory]
enabled = false
"#,
        )
        .expect("write settings file");

        let settings = load_settings_file_from_path(path.as_path()).expect("settings load");
        let serialized = serialize_settings(&settings).expect("settings should serialize");
        assert!(!serialized.contains("[memory]"));
    }
}
