use anyhow::{Context as _, Result};
use gpui::{App, Global};
use pioneer_config::{AppConfig, GatewayMemoryConfig};
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
    #[serde(default)]
    memory: MemorySettings,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MemorySettings {
    #[serde(default = "default_memory_enabled")]
    pub enabled: bool,
    #[serde(default = "default_memory_deterministic_recall_enabled")]
    pub deterministic_recall_enabled: bool,
    #[serde(default = "default_memory_active_recall_enabled")]
    pub active_recall_enabled: bool,
    #[serde(default = "default_memory_tools_enabled")]
    pub tools_enabled: bool,
    #[serde(default = "default_memory_proactive_writes_enabled")]
    pub proactive_writes_enabled: bool,
    #[serde(default = "default_memory_background_extraction_enabled")]
    pub background_extraction_enabled: bool,
    #[serde(default)]
    pub debug_trace_enabled: bool,
    #[serde(default)]
    pub strict_diagnostics_enabled: bool,
}

impl Default for MemorySettings {
    fn default() -> Self {
        Self {
            enabled: default_memory_enabled(),
            deterministic_recall_enabled: default_memory_deterministic_recall_enabled(),
            active_recall_enabled: default_memory_active_recall_enabled(),
            tools_enabled: default_memory_tools_enabled(),
            proactive_writes_enabled: default_memory_proactive_writes_enabled(),
            background_extraction_enabled: default_memory_background_extraction_enabled(),
            debug_trace_enabled: false,
            strict_diagnostics_enabled: false,
        }
    }
}

struct DesktopSettingsState {
    path: PathBuf,
    settings: DesktopSettingsFile,
}

impl Global for DesktopSettingsState {}

const fn default_settings_version() -> u32 {
    DESKTOP_SETTINGS_VERSION
}

const fn default_memory_enabled() -> bool {
    true
}

const fn default_memory_deterministic_recall_enabled() -> bool {
    true
}

const fn default_memory_active_recall_enabled() -> bool {
    true
}

const fn default_memory_tools_enabled() -> bool {
    true
}

const fn default_memory_proactive_writes_enabled() -> bool {
    true
}

const fn default_memory_background_extraction_enabled() -> bool {
    true
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

pub(crate) fn memory_settings(cx: &App) -> MemorySettings {
    cx.try_global::<DesktopSettingsState>()
        .map(|state| state.settings.memory)
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

pub(crate) fn set_memory_settings(cx: &mut App, memory: MemorySettings) -> Result<()> {
    ensure_loaded(cx)?;

    let (path, serialized) = {
        let state = cx.global_mut::<DesktopSettingsState>();
        state.settings.version = DESKTOP_SETTINGS_VERSION;
        state.settings.memory = memory;
        (state.path.clone(), serialize_settings(&state.settings)?)
    };

    write_settings_file(path.as_path(), serialized)?;
    Ok(())
}

pub(crate) fn load_app_language_preference() -> Option<AppLanguagePreference> {
    let settings = load_settings_file().ok()?;
    Some(settings.general.language)
}

pub(crate) fn load_memory_settings() -> Result<MemorySettings> {
    let settings = load_settings_file()?;
    Ok(settings.memory)
}

pub(crate) fn apply_memory_settings_to_app_config(
    mut config: AppConfig,
    memory: MemorySettings,
) -> AppConfig {
    config.gateway.memory = memory.apply_to_gateway_memory_config(config.gateway.memory);
    config
}

pub(crate) fn apply_loaded_memory_settings_to_app_config(config: AppConfig) -> AppConfig {
    match load_memory_settings() {
        Ok(memory) => apply_memory_settings_to_app_config(config, memory),
        Err(_) => config,
    }
}

pub(crate) fn resolve_app_locale() -> String {
    load_app_language_preference()
        .unwrap_or_default()
        .resolve_locale()
}

impl MemorySettings {
    pub(crate) fn apply_to_gateway_memory_config(
        self,
        mut config: GatewayMemoryConfig,
    ) -> GatewayMemoryConfig {
        config.enabled = self.enabled;
        config.deterministic_recall_enabled = self.deterministic_recall_enabled;
        config.active_recall_enabled = self.active_recall_enabled;
        config.tools_enabled = self.tools_enabled;
        config.proactive_writes_enabled = self.proactive_writes_enabled;
        config.background_extraction_enabled = self.background_extraction_enabled;
        config.debug_trace_enabled = self.debug_trace_enabled;
        config.strict_diagnostics_enabled = self.strict_diagnostics_enabled;
        config
    }
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

#[cfg(test)]
mod tests {
    use super::{
        DesktopSettingsFile, MemorySettings, apply_memory_settings_to_app_config,
        load_settings_file_from_path, serialize_settings,
    };
    use pioneer_config::GatewayMemoryConfig;
    use std::fs;

    #[test]
    fn memory_settings_default_to_product_defaults() {
        let memory = MemorySettings::default();

        assert!(memory.enabled);
        assert!(memory.deterministic_recall_enabled);
        assert!(memory.active_recall_enabled);
        assert!(memory.tools_enabled);
        assert!(memory.proactive_writes_enabled);
        assert!(memory.background_extraction_enabled);
        assert!(!memory.debug_trace_enabled);
        assert!(!memory.strict_diagnostics_enabled);
    }

    #[test]
    fn memory_settings_missing_fields_load_with_defaults() {
        let settings = toml::from_str::<DesktopSettingsFile>(
            r#"
version = 1

[general]
theme = "dark"
"#,
        )
        .expect("desktop settings should parse");

        assert_eq!(settings.memory, MemorySettings::default());
    }

    #[test]
    fn memory_settings_roundtrip_stable_toml_fields() {
        let settings = DesktopSettingsFile {
            memory: MemorySettings {
                enabled: false,
                deterministic_recall_enabled: false,
                active_recall_enabled: false,
                tools_enabled: false,
                proactive_writes_enabled: false,
                background_extraction_enabled: false,
                debug_trace_enabled: true,
                strict_diagnostics_enabled: true,
            },
            ..DesktopSettingsFile::default()
        };

        let serialized = serialize_settings(&settings).expect("settings should serialize");
        assert!(serialized.contains("[memory]"));
        assert!(serialized.contains("enabled = false"));
        assert!(serialized.contains("deterministic_recall_enabled = false"));
        assert!(serialized.contains("active_recall_enabled = false"));
        assert!(serialized.contains("tools_enabled = false"));
        assert!(serialized.contains("proactive_writes_enabled = false"));
        assert!(serialized.contains("background_extraction_enabled = false"));
        assert!(serialized.contains("debug_trace_enabled = true"));
        assert!(serialized.contains("strict_diagnostics_enabled = true"));

        let parsed =
            toml::from_str::<DesktopSettingsFile>(serialized.as_str()).expect("settings parse");
        assert_eq!(parsed.memory, settings.memory);
    }

    #[test]
    fn memory_settings_load_from_path_uses_backward_safe_defaults() {
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
        assert!(!settings.memory.enabled);
        assert!(settings.memory.deterministic_recall_enabled);
        assert!(settings.memory.active_recall_enabled);
        assert!(settings.memory.tools_enabled);
        assert!(settings.memory.proactive_writes_enabled);
        assert!(settings.memory.background_extraction_enabled);
    }

    #[test]
    fn memory_settings_apply_to_gateway_memory_config_preserves_storage_and_scope_fields() {
        let gateway = GatewayMemoryConfig {
            capsules_dir: "memory/custom".to_owned(),
            allow_global_user_by_default: false,
            allow_global_agent_by_default: true,
            ..GatewayMemoryConfig::default()
        };
        let mapped = MemorySettings {
            enabled: false,
            deterministic_recall_enabled: false,
            active_recall_enabled: false,
            tools_enabled: false,
            proactive_writes_enabled: false,
            background_extraction_enabled: false,
            debug_trace_enabled: true,
            strict_diagnostics_enabled: true,
        }
        .apply_to_gateway_memory_config(gateway);

        assert_eq!(mapped.capsules_dir, "memory/custom");
        assert!(!mapped.allow_global_user_by_default);
        assert!(mapped.allow_global_agent_by_default);
        assert!(!mapped.enabled);
        assert!(!mapped.deterministic_recall_enabled);
        assert!(!mapped.active_recall_enabled);
        assert!(!mapped.tools_enabled);
        assert!(!mapped.proactive_writes_enabled);
        assert!(!mapped.background_extraction_enabled);
        assert!(mapped.debug_trace_enabled);
        assert!(mapped.strict_diagnostics_enabled);
    }

    #[test]
    fn memory_settings_apply_to_app_config_maps_gateway_memory_only() {
        let mut config = pioneer_config::AppConfig::load().expect("default config loads");
        config.gateway.memory.capsules_dir = "memory/custom".to_owned();
        config.gateway.service_name = "service-name-before-memory-settings".to_owned();

        let mapped = apply_memory_settings_to_app_config(
            config,
            MemorySettings {
                enabled: false,
                deterministic_recall_enabled: false,
                active_recall_enabled: false,
                tools_enabled: false,
                proactive_writes_enabled: false,
                background_extraction_enabled: false,
                debug_trace_enabled: true,
                strict_diagnostics_enabled: true,
            },
        );

        assert_eq!(
            mapped.gateway.service_name,
            "service-name-before-memory-settings"
        );
        assert_eq!(mapped.gateway.memory.capsules_dir, "memory/custom");
        assert!(!mapped.gateway.memory.enabled);
        assert!(!mapped.gateway.memory.deterministic_recall_enabled);
        assert!(!mapped.gateway.memory.active_recall_enabled);
        assert!(!mapped.gateway.memory.tools_enabled);
        assert!(!mapped.gateway.memory.proactive_writes_enabled);
        assert!(!mapped.gateway.memory.background_extraction_enabled);
        assert!(mapped.gateway.memory.debug_trace_enabled);
        assert!(mapped.gateway.memory.strict_diagnostics_enabled);
    }
}
