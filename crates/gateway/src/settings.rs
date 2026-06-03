use anyhow::{Context, Result, bail};
use pioneer_config::{
    AppConfig, GatewayConfig, GatewayMemoryConfig, GatewayMemoryModelSelectionConfig,
    GatewayMemoryModelSelectionSource as ConfigGatewayMemoryModelSelectionSource,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Component, Path};

use crate::helpers::normalize_non_empty;

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewaySettings {
    version: u32,
    #[serde(default, skip_serializing_if = "GatewayGeneralSettings::is_default")]
    general: GatewayGeneralSettings,
    secrets: GatewaySecretsSettings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory: Option<GatewayMemorySettingsOverride>,
    #[serde(skip)]
    migrated: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewaySettingsWire {
    version: u32,
    #[serde(default)]
    general: GatewayGeneralSettings,
    secrets: GatewaySecretsSettings,
    #[serde(default)]
    memory: Option<GatewayMemorySettingsOverride>,
}

impl<'de> Deserialize<'de> for GatewaySettings {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = GatewaySettingsWire::deserialize(deserializer)?;
        let mut settings = Self {
            version: wire.version,
            general: wire.general,
            secrets: wire.secrets,
            memory: wire.memory,
            migrated: false,
        };
        settings.migrate_legacy_active_recall_model();
        Ok(settings)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayGeneralSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    keepawake: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    preflight_model: Option<GatewayMemoryModelSelectionConfig>,
}

impl GatewayGeneralSettings {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }

    fn effective(&self, config: &GatewayConfig) -> pioneer_protocol::GatewayGeneralSettings {
        pioneer_protocol::GatewayGeneralSettings {
            keepawake: self.keepawake.unwrap_or(config.keepawake),
            preflight_model: model_selection_to_protocol(
                self.preflight_model
                    .as_ref()
                    .unwrap_or(&config.preflight_model),
            ),
        }
    }

    fn apply_protocol_update(
        &mut self,
        update: pioneer_protocol::GatewayGeneralSettingsUpdate,
    ) -> GatewayGeneralSettingsChangeSet {
        let mut changes = GatewayGeneralSettingsChangeSet::default();
        if let Some(keepawake) = update.keepawake {
            self.keepawake = Some(keepawake);
            changes.keepawake = Some(keepawake);
        }
        if let Some(preflight_model) = update.preflight_model {
            let preflight_model = model_selection_from_protocol(preflight_model);
            self.preflight_model = Some(preflight_model.clone());
            changes.preflight_model = Some(preflight_model);
        }
        changes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewaySecretsSettings {
    backend: GatewaySecretsBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GatewaySecretsBackend {
    Keystore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayMemorySettings {
    pub enabled: bool,
    pub deterministic_recall_enabled: bool,
    pub active_recall_enabled: bool,
    pub tools_enabled: bool,
    pub proactive_writes_enabled: bool,
    pub background_extraction_enabled: bool,
    #[serde(default)]
    pub proactive_writes_model: GatewayMemoryModelSelectionConfig,
    pub debug_trace_enabled: bool,
    pub strict_diagnostics_enabled: bool,
}

impl Default for GatewayMemorySettings {
    fn default() -> Self {
        Self::from_gateway_memory_config(&GatewayMemoryConfig::default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayMemorySettingsOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deterministic_recall_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    active_recall_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tools_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proactive_writes_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    background_extraction_enabled: Option<bool>,
    #[serde(default, rename = "active_recall_model", skip_serializing)]
    legacy_active_recall_model: Option<GatewayMemoryModelSelectionConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proactive_writes_model: Option<GatewayMemoryModelSelectionConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    debug_trace_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    strict_diagnostics_enabled: Option<bool>,
}

impl Default for GatewaySecretsSettings {
    fn default() -> Self {
        Self {
            backend: GatewaySecretsBackend::Keystore,
        }
    }
}

impl GatewaySettings {
    pub fn effective_general_settings(
        &self,
        config: &GatewayConfig,
    ) -> pioneer_protocol::GatewayGeneralSettings {
        self.general.effective(config)
    }

    pub fn secrets_backend(&self) -> GatewaySecretsBackend {
        self.secrets.backend
    }

    pub fn has_memory_settings(&self) -> bool {
        self.memory.is_some()
    }

    pub fn effective_memory_settings(&self, config: &GatewayMemoryConfig) -> GatewayMemorySettings {
        let settings = GatewayMemorySettings::from_gateway_memory_config(config);
        if let Some(memory) = &self.memory {
            memory.apply_to_memory_settings(settings)
        } else {
            settings
        }
    }

    pub fn set_memory_settings(&mut self, memory: GatewayMemorySettings) {
        self.memory = Some(GatewayMemorySettingsOverride::from_memory_settings(memory));
    }

    pub fn snapshot(&self, config: &GatewayConfig) -> pioneer_protocol::GatewaySettingsSnapshot {
        let general = self.effective_general_settings(config);
        pioneer_protocol::GatewaySettingsSnapshot {
            general,
            memory: self.effective_memory_settings(&config.memory).to_protocol(),
        }
    }

    pub fn apply_protocol_update(
        &mut self,
        update: pioneer_protocol::GatewaySettingsUpdate,
    ) -> GatewaySettingsChangeSet {
        let mut changes = GatewaySettingsChangeSet::default();
        if let Some(general) = update.general {
            changes.general = self.general.apply_protocol_update(general);
        }
        if let Some(memory) = update.memory {
            let memory = GatewayMemorySettings::from_protocol(memory);
            self.set_memory_settings(memory);
            changes.memory = true;
        }
        changes
    }

    pub fn apply_to_gateway_memory_config(
        &self,
        config: GatewayMemoryConfig,
    ) -> GatewayMemoryConfig {
        if let Some(memory) = &self.memory {
            memory.apply_to_gateway_memory_config(config)
        } else {
            config
        }
    }

    pub fn apply_to_gateway_config(&self, mut config: GatewayConfig) -> GatewayConfig {
        let general = self.effective_general_settings(&config);
        config.keepawake = general.keepawake;
        config.preflight_model = model_selection_from_protocol(general.preflight_model);
        config.memory = self.apply_to_gateway_memory_config(config.memory);
        config
    }

    pub fn apply_to_app_config(&self, mut config: AppConfig) -> AppConfig {
        config.gateway = self.apply_to_gateway_config(config.gateway);
        config
    }

    fn migrate_legacy_active_recall_model(&mut self) -> bool {
        let mut migrated = false;
        if let Some(memory) = &mut self.memory {
            if self.general.preflight_model.is_none() {
                self.general.preflight_model = memory.legacy_active_recall_model.clone();
            }
            migrated = memory.legacy_active_recall_model.is_some();
            memory.legacy_active_recall_model = None;
        }
        self.migrated |= migrated;
        migrated
    }
}

impl GatewayMemorySettings {
    pub fn from_gateway_memory_config(config: &GatewayMemoryConfig) -> Self {
        Self {
            enabled: config.enabled,
            deterministic_recall_enabled: config.deterministic_recall_enabled,
            active_recall_enabled: config.active_recall_enabled,
            tools_enabled: config.tools_enabled,
            proactive_writes_enabled: config.proactive_writes_enabled,
            background_extraction_enabled: config.background_extraction_enabled,
            proactive_writes_model: config.proactive_writes_model.clone(),
            debug_trace_enabled: config.debug_trace_enabled,
            strict_diagnostics_enabled: config.strict_diagnostics_enabled,
        }
    }

    pub fn from_protocol(settings: pioneer_protocol::GatewayMemorySettings) -> Self {
        Self {
            enabled: settings.enabled,
            deterministic_recall_enabled: settings.deterministic_recall_enabled,
            active_recall_enabled: settings.active_recall_enabled,
            tools_enabled: settings.tools_enabled,
            proactive_writes_enabled: settings.proactive_writes_enabled,
            background_extraction_enabled: settings.background_extraction_enabled,
            proactive_writes_model: model_selection_from_protocol(settings.proactive_writes_model),
            debug_trace_enabled: settings.debug_trace_enabled,
            strict_diagnostics_enabled: settings.strict_diagnostics_enabled,
        }
    }

    pub fn to_protocol(&self) -> pioneer_protocol::GatewayMemorySettings {
        pioneer_protocol::GatewayMemorySettings {
            enabled: self.enabled,
            deterministic_recall_enabled: self.deterministic_recall_enabled,
            active_recall_enabled: self.active_recall_enabled,
            tools_enabled: self.tools_enabled,
            proactive_writes_enabled: self.proactive_writes_enabled,
            background_extraction_enabled: self.background_extraction_enabled,
            proactive_writes_model: model_selection_to_protocol(&self.proactive_writes_model),
            debug_trace_enabled: self.debug_trace_enabled,
            strict_diagnostics_enabled: self.strict_diagnostics_enabled,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewaySettingsChangeSet {
    pub general: GatewayGeneralSettingsChangeSet,
    pub memory: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayGeneralSettingsChangeSet {
    pub keepawake: Option<bool>,
    pub preflight_model: Option<GatewayMemoryModelSelectionConfig>,
}

impl GatewayMemorySettingsOverride {
    fn from_memory_settings(settings: GatewayMemorySettings) -> Self {
        Self {
            enabled: Some(settings.enabled),
            deterministic_recall_enabled: Some(settings.deterministic_recall_enabled),
            active_recall_enabled: Some(settings.active_recall_enabled),
            tools_enabled: Some(settings.tools_enabled),
            proactive_writes_enabled: Some(settings.proactive_writes_enabled),
            background_extraction_enabled: Some(settings.background_extraction_enabled),
            legacy_active_recall_model: None,
            proactive_writes_model: Some(settings.proactive_writes_model),
            debug_trace_enabled: Some(settings.debug_trace_enabled),
            strict_diagnostics_enabled: Some(settings.strict_diagnostics_enabled),
        }
    }

    fn apply_to_memory_settings(
        &self,
        mut settings: GatewayMemorySettings,
    ) -> GatewayMemorySettings {
        if let Some(enabled) = self.enabled {
            settings.enabled = enabled;
        }
        if let Some(deterministic_recall_enabled) = self.deterministic_recall_enabled {
            settings.deterministic_recall_enabled = deterministic_recall_enabled;
        }
        if let Some(active_recall_enabled) = self.active_recall_enabled {
            settings.active_recall_enabled = active_recall_enabled;
        }
        if let Some(tools_enabled) = self.tools_enabled {
            settings.tools_enabled = tools_enabled;
        }
        if let Some(proactive_writes_enabled) = self.proactive_writes_enabled {
            settings.proactive_writes_enabled = proactive_writes_enabled;
        }
        if let Some(background_extraction_enabled) = self.background_extraction_enabled {
            settings.background_extraction_enabled = background_extraction_enabled;
        }
        if let Some(proactive_writes_model) = &self.proactive_writes_model {
            settings.proactive_writes_model = proactive_writes_model.clone();
        }
        if let Some(debug_trace_enabled) = self.debug_trace_enabled {
            settings.debug_trace_enabled = debug_trace_enabled;
        }
        if let Some(strict_diagnostics_enabled) = self.strict_diagnostics_enabled {
            settings.strict_diagnostics_enabled = strict_diagnostics_enabled;
        }
        settings
    }

    fn apply_to_gateway_memory_config(
        &self,
        mut config: GatewayMemoryConfig,
    ) -> GatewayMemoryConfig {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(deterministic_recall_enabled) = self.deterministic_recall_enabled {
            config.deterministic_recall_enabled = deterministic_recall_enabled;
        }
        if let Some(active_recall_enabled) = self.active_recall_enabled {
            config.active_recall_enabled = active_recall_enabled;
        }
        if let Some(tools_enabled) = self.tools_enabled {
            config.tools_enabled = tools_enabled;
        }
        if let Some(proactive_writes_enabled) = self.proactive_writes_enabled {
            config.proactive_writes_enabled = proactive_writes_enabled;
        }
        if let Some(background_extraction_enabled) = self.background_extraction_enabled {
            config.background_extraction_enabled = background_extraction_enabled;
        }
        if let Some(proactive_writes_model) = &self.proactive_writes_model {
            config.proactive_writes_model = proactive_writes_model.clone();
        }
        if let Some(debug_trace_enabled) = self.debug_trace_enabled {
            config.debug_trace_enabled = debug_trace_enabled;
        }
        if let Some(strict_diagnostics_enabled) = self.strict_diagnostics_enabled {
            config.strict_diagnostics_enabled = strict_diagnostics_enabled;
        }
        config
    }
}

fn model_selection_from_protocol(
    selection: pioneer_protocol::GatewayMemoryModelSelection,
) -> GatewayMemoryModelSelectionConfig {
    let source = match selection.source {
        pioneer_protocol::GatewayMemoryModelSelectionSource::Thread => {
            ConfigGatewayMemoryModelSelectionSource::Thread
        }
        pioneer_protocol::GatewayMemoryModelSelectionSource::Custom => {
            ConfigGatewayMemoryModelSelectionSource::Custom
        }
    };
    GatewayMemoryModelSelectionConfig {
        source,
        model_provider: selection.model_provider,
        model: selection.model,
    }
}

fn model_selection_to_protocol(
    selection: &GatewayMemoryModelSelectionConfig,
) -> pioneer_protocol::GatewayMemoryModelSelection {
    let source = match &selection.source {
        ConfigGatewayMemoryModelSelectionSource::Thread => {
            pioneer_protocol::GatewayMemoryModelSelectionSource::Thread
        }
        ConfigGatewayMemoryModelSelectionSource::Custom => {
            pioneer_protocol::GatewayMemoryModelSelectionSource::Custom
        }
    };
    pioneer_protocol::GatewayMemoryModelSelection {
        source,
        model_provider: selection.model_provider.clone(),
        model: selection.model.clone(),
    }
}

pub fn normalize_settings_file_name(value: &str) -> Result<String> {
    let trimmed = normalize_non_empty(value, "settings_file_name must not be empty")?;
    let path = Path::new(trimmed.as_str());

    if path.is_absolute() {
        bail!("settings_file_name must be relative");
    }

    if path.components().any(is_disallowed_component) {
        bail!("settings_file_name must not contain parent or root components");
    }

    Ok(trimmed)
}

pub fn load_or_create_gateway_settings(
    path: &Path,
    expected_version: u32,
    settings_file_name: &str,
) -> Result<GatewaySettings> {
    if path.exists() {
        return load_gateway_settings(path, expected_version, settings_file_name);
    }

    let settings = GatewaySettings {
        version: expected_version,
        general: GatewayGeneralSettings::default(),
        secrets: GatewaySecretsSettings::default(),
        memory: None,
        migrated: false,
    };

    save_gateway_settings(path, &settings)?;
    Ok(settings)
}

fn load_gateway_settings(
    path: &Path,
    expected_version: u32,
    _settings_file_name: &str,
) -> Result<GatewaySettings> {
    let path_display = path.display().to_string();
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read gateway settings `{path_display}`"))?;

    let settings = toml::from_str::<GatewaySettings>(&content)
        .with_context(|| format!("failed to parse gateway settings `{path_display}`"))?;

    if settings.version != expected_version {
        bail!(
            "unsupported gateway settings version `{}` in `{}`; expected `{}`",
            settings.version,
            path.display(),
            expected_version
        );
    }

    if settings.secrets_backend() != GatewaySecretsBackend::Keystore {
        bail!(
            "unsupported gateway secrets backend in `{}`",
            path.display()
        );
    }

    if settings.migrated {
        save_gateway_settings(path, &settings).with_context(|| {
            format!("failed to save migrated gateway settings `{path_display}`")
        })?;
    }

    Ok(settings)
}

pub fn save_gateway_settings(path: &Path, settings: &GatewaySettings) -> Result<()> {
    let mut settings = settings.clone();
    settings.migrate_legacy_active_recall_model();
    let content =
        toml::to_string_pretty(&settings).context("failed to serialize gateway settings")?;
    write_settings_file(path, content.as_str())
}

fn write_settings_file(path: &Path, content: &str) -> Result<()> {
    let path_display = path.display().to_string();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "gateway-settings.toml".into());
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let temp_path = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        now_nanos
    ));

    fs::write(&temp_path, content)
        .with_context(|| format!("failed to write `{}`", temp_path.display()))?;
    set_private_permissions(&temp_path)?;
    if let Err(error) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error).with_context(|| format!("failed to replace `{path_display}`"));
    }
    set_private_permissions(path)?;
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)
        .with_context(|| format!("failed to read metadata for `{}`", path.display()))?
        .permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("failed to set permissions for `{}`", path.display()))
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn is_disallowed_component(component: Component<'_>) -> bool {
    matches!(
        component,
        Component::ParentDir | Component::RootDir | Component::Prefix(_)
    )
}

#[cfg(test)]
mod tests {
    use super::{GatewayMemorySettings, load_or_create_gateway_settings, save_gateway_settings};
    use pioneer_config::{
        GatewayArtifactsConfig, GatewayAuthConfig, GatewayComputerUseToolsConfig, GatewayConfig,
        GatewayDatabaseConfig, GatewayExecutionWindowsConfig, GatewayMemoryConfig,
        GatewayMemoryModelSelectionConfig, GatewayProviderConfig, GatewaySkillsConfig,
        GatewayThreadConfig, GatewayToolLoopBudgetConfig, GatewayToolRetryBudgetConfig,
        GatewayToolsConfig, GatewayWebToolsConfig,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn creates_sanitized_gateway_settings_without_jwt_or_provider_secrets() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");

        let _settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should be created");
        let content = fs::read_to_string(&path).expect("read settings");

        assert!(!content.contains("[general]"));
        assert!(!content.contains("keepawake"));
        assert!(content.contains("[secrets]"));
        assert!(content.contains("backend = \"keystore\""));
        assert!(!content.contains("jwt_secret"));
        assert!(!content.contains("[providers]"));
        assert!(!content.contains("[providers.keys]"));
        assert!(!content.contains("[mcp]"));
        assert!(!content.contains("[mcp.secrets]"));
        assert!(!content.contains("[memory]"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn gateway_settings_memory_overrides_gateway_config_without_owning_storage_fields() {
        let settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"

[memory]
enabled = false
debug_trace_enabled = true
active_recall_model = { source = "custom", model_provider = "legacy-provider", model = "legacy-model" }
proactive_writes_model = { source = "custom", model_provider = "extractor-provider", model = "extractor-model" }
"#,
        )
        .expect("gateway settings should parse");

        let base = GatewayMemoryConfig {
            capsules_dir: "memory/custom".to_owned(),
            allow_global_user_by_default: false,
            allow_global_agent_by_default: true,
            deterministic_recall_enabled: false,
            active_recall_enabled: false,
            tools_enabled: false,
            proactive_writes_enabled: false,
            background_extraction_enabled: false,
            strict_diagnostics_enabled: true,
            ..GatewayMemoryConfig::default()
        };

        assert_eq!(
            settings
                .effective_general_settings(&gateway_config_with_keepawake(false))
                .preflight_model,
            pioneer_protocol::GatewayMemoryModelSelection::custom(
                "legacy-provider",
                "legacy-model"
            )
        );

        let mapped = settings.apply_to_gateway_memory_config(base);

        assert_eq!(mapped.capsules_dir, "memory/custom");
        assert!(!mapped.allow_global_user_by_default);
        assert!(mapped.allow_global_agent_by_default);
        assert!(!mapped.enabled);
        assert!(!mapped.deterministic_recall_enabled);
        assert!(!mapped.active_recall_enabled);
        assert!(!mapped.tools_enabled);
        assert!(!mapped.proactive_writes_enabled);
        assert!(!mapped.background_extraction_enabled);
        assert!(mapped.active_recall_model.is_thread_model());
        assert_eq!(
            mapped.proactive_writes_model,
            GatewayMemoryModelSelectionConfig::custom("extractor-provider", "extractor-model")
        );
        assert!(mapped.debug_trace_enabled);
        assert!(mapped.strict_diagnostics_enabled);
    }

    #[test]
    fn gateway_settings_general_overrides_gateway_config() {
        let settings_without_override = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"
"#,
        )
        .expect("gateway settings should parse");
        let mut gateway_config = gateway_config_with_keepawake(false);
        assert!(
            !settings_without_override
                .effective_general_settings(&gateway_config)
                .keepawake
        );
        gateway_config.keepawake = true;
        assert!(
            settings_without_override
                .effective_general_settings(&gateway_config)
                .keepawake
        );
        gateway_config.preflight_model =
            GatewayMemoryModelSelectionConfig::custom("config-provider", "config-model");
        assert_eq!(
            settings_without_override
                .effective_general_settings(&gateway_config)
                .preflight_model,
            pioneer_protocol::GatewayMemoryModelSelection::custom(
                "config-provider",
                "config-model"
            )
        );

        let settings_with_enabled_override = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[general]
keepawake = true
preflight_model = { source = "custom", model_provider = "settings-provider", model = "settings-model" }

[secrets]
backend = "keystore"
"#,
        )
        .expect("gateway settings should parse");
        assert!(
            settings_with_enabled_override
                .effective_general_settings(&gateway_config_with_keepawake(false))
                .keepawake
        );
        assert_eq!(
            settings_with_enabled_override
                .effective_general_settings(&gateway_config)
                .preflight_model,
            pioneer_protocol::GatewayMemoryModelSelection::custom(
                "settings-provider",
                "settings-model"
            )
        );

        let settings_with_disabled_override = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[general]
keepawake = false

[secrets]
backend = "keystore"
"#,
        )
        .expect("gateway settings should parse");
        assert!(
            !settings_with_disabled_override
                .effective_general_settings(&gateway_config_with_keepawake(true))
                .keepawake
        );

        let applied_config = settings_with_disabled_override
            .apply_to_gateway_config(gateway_config_with_keepawake(true));
        assert!(!applied_config.keepawake);
    }

    #[test]
    fn saves_gateway_general_settings_in_gateway_settings_file() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        let mut settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should be created");

        settings.apply_protocol_update(pioneer_protocol::GatewaySettingsUpdate {
            general: Some(pioneer_protocol::GatewayGeneralSettingsUpdate {
                keepawake: Some(true),
                preflight_model: Some(pioneer_protocol::GatewayMemoryModelSelection::custom(
                    "planner-provider",
                    "planner-model",
                )),
            }),
            memory: None,
        });
        save_gateway_settings(&path, &settings).expect("settings should save");

        let content = fs::read_to_string(&path).expect("read settings");
        assert!(content.contains("[general]"));
        assert!(content.contains("keepawake = true"));
        assert!(content.contains("preflight_model"));
        assert!(content.contains("model_provider = \"planner-provider\""));
        assert!(content.contains("model = \"planner-model\""));
        assert!(!content.contains("[memory]"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn saves_gateway_memory_settings_in_gateway_settings_file() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        let mut settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should be created");

        settings.set_memory_settings(GatewayMemorySettings {
            enabled: false,
            deterministic_recall_enabled: false,
            active_recall_enabled: false,
            tools_enabled: false,
            proactive_writes_enabled: false,
            background_extraction_enabled: false,
            proactive_writes_model: GatewayMemoryModelSelectionConfig::thread(),
            debug_trace_enabled: true,
            strict_diagnostics_enabled: true,
        });
        save_gateway_settings(&path, &settings).expect("settings should save");

        let content = fs::read_to_string(&path).expect("read settings");
        assert!(content.contains("[memory]"));
        assert!(content.contains("enabled = false"));
        assert!(content.contains("deterministic_recall_enabled = false"));
        assert!(content.contains("active_recall_enabled = false"));
        assert!(content.contains("tools_enabled = false"));
        assert!(content.contains("proactive_writes_enabled = false"));
        assert!(content.contains("background_extraction_enabled = false"));
        assert!(!content.contains("active_recall_model"));
        assert!(!content.contains("planner-provider"));
        assert!(!content.contains("planner-model"));
        assert!(content.contains("proactive_writes_model = \"thread\""));
        assert!(content.contains("debug_trace_enabled = true"));
        assert!(content.contains("strict_diagnostics_enabled = true"));
        assert!(!content.contains("capsules_dir"));
        assert!(!content.contains("allow_global_user_by_default"));
        assert!(!content.contains("allow_global_agent_by_default"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn gateway_settings_migration_copies_legacy_active_recall_model_to_general() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 1

[secrets]
backend = "keystore"

[memory]
proactive_writes_model = { source = "custom", model_provider = "extractor-provider", model = "extractor-model" }

[memory.active_recall_model]
source = "custom"
model_provider = "legacy-provider"
model = "legacy-model"
"#,
        )
        .expect("write legacy settings");

        let settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("legacy settings should load");
        assert_eq!(
            settings
                .effective_general_settings(&gateway_config_with_keepawake(false))
                .preflight_model,
            pioneer_protocol::GatewayMemoryModelSelection::custom(
                "legacy-provider",
                "legacy-model"
            )
        );

        let content = fs::read_to_string(&path).expect("read migrated settings");
        assert!(content.contains("preflight_model"));
        assert!(content.contains("legacy-provider"));
        assert!(content.contains("legacy-model"));
        assert!(!content.contains("active_recall_model"));
        assert!(content.contains("proactive_writes_model"));
        assert!(content.contains("extractor-provider"));
        assert!(content.contains("extractor-model"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn gateway_settings_migration_preserves_explicit_preflight_model() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 1

[general]
preflight_model = { source = "custom", model_provider = "new-provider", model = "new-model" }

[secrets]
backend = "keystore"

[memory.active_recall_model]
source = "custom"
model_provider = "legacy-provider"
model = "legacy-model"
"#,
        )
        .expect("write mixed settings");

        let settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("mixed settings should load");
        assert_eq!(
            settings
                .effective_general_settings(&gateway_config_with_keepawake(false))
                .preflight_model,
            pioneer_protocol::GatewayMemoryModelSelection::custom("new-provider", "new-model")
        );

        let content = fs::read_to_string(&path).expect("read migrated settings");
        assert!(content.contains("new-provider"));
        assert!(content.contains("new-model"));
        assert!(!content.contains("active_recall_model"));
        assert!(!content.contains("legacy-provider"));
        assert!(!content.contains("legacy-model"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_unsupported_version() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
            version = 2

            [secrets]
            backend = "keystore"
            "#,
        )
        .expect("write unsupported-version settings");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("unsupported settings version should be rejected");
        assert!(
            format!("{error:#}").contains("unsupported gateway settings version"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_removed_jwt_secret_field() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
            version = 1
            jwt_secret = "abcd"

            [secrets]
            backend = "keystore"
            "#,
        )
        .expect("write settings with removed jwt field");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("removed jwt_secret field should be rejected");
        assert!(
            format!("{error:#}").contains("unknown field `jwt_secret`"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_top_level_keepawake_field() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
            version = 1
            keepawake = true

            [secrets]
            backend = "keystore"
            "#,
        )
        .expect("write settings with top-level keepawake field");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("top-level keepawake field should be rejected");
        assert!(
            format!("{error:#}").contains("unknown field `keepawake`"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_removed_providers_field() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
            version = 1

            [secrets]
            backend = "keystore"

            [providers.keys]
            openrouter = "sk-test"
            "#,
        )
        .expect("write settings with removed providers field");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("removed providers field should be rejected");
        assert!(
            format!("{error:#}").contains("unknown field `providers`"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_removed_mcp_field() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
            version = 1

            [secrets]
            backend = "keystore"

            [mcp]
            "#,
        )
        .expect("write settings with removed mcp field");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("removed mcp field should be rejected");
        assert!(
            format!("{error:#}").contains("unknown field `mcp`"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_removed_mcp_secrets_field() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
            version = 1

            [secrets]
            backend = "keystore"

            [mcp.secrets]
            token = "secret"
            "#,
        )
        .expect("write settings with removed mcp secrets field");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("removed mcp secrets field should be rejected");
        assert!(
            format!("{error:#}").contains("unknown field `mcp`"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_unsupported_secret_backend() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
            version = 1

            [secrets]
            backend = "db-keystore"
            "#,
        )
        .expect("write settings with unsupported backend");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("unsupported backend should be rejected");
        assert!(
            format!("{error:#}").contains("unknown variant `db-keystore`"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    fn unique_temp_dir() -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pioneer-settings-tests-{nanos}-{id}"))
    }

    fn gateway_config_with_keepawake(keepawake: bool) -> GatewayConfig {
        GatewayConfig {
            settings_version: 1,
            settings_file_name: "gateway-settings.toml".to_owned(),
            service_name: "com.pioneer.gateway".to_owned(),
            legacy_service_names: Vec::new(),
            listen_addr: "0.0.0.0:17878".to_owned(),
            outbound_queue_capacity: 128,
            keepawake,
            preflight_model: Default::default(),
            thread: GatewayThreadConfig {
                default_model: "gpt-5.4".to_owned(),
                default_model_provider: "openai".to_owned(),
                summary_model: None,
                summary_model_provider: None,
                title_model: None,
                title_model_provider: None,
                max_context_tokens: 128_000,
                response_reserve_tokens: 16_000,
            },
            tools: GatewayToolsConfig {
                web: GatewayWebToolsConfig::default(),
                computer_use: GatewayComputerUseToolsConfig::default(),
                budget: GatewayToolLoopBudgetConfig::default(),
                execution_windows: Some(GatewayExecutionWindowsConfig::default()),
                retry: GatewayToolRetryBudgetConfig::default(),
            },
            tasks: Default::default(),
            skills: GatewaySkillsConfig::default(),
            provider: GatewayProviderConfig::default(),
            database: GatewayDatabaseConfig {
                file_name: "gateway.db".to_owned(),
                max_connections: 10,
                connect_timeout_ms: 5_000,
                acquire_timeout_ms: 5_000,
                idle_timeout_ms: 30_000,
                sqlx_logging: false,
                run_migrations_on_startup: true,
            },
            memory: GatewayMemoryConfig::default(),
            hooks: Default::default(),
            artifacts: GatewayArtifactsConfig::default(),
            auth: GatewayAuthConfig {
                jwt_issuer: "pioneer".to_owned(),
                jwt_audience: "pioneer-clients".to_owned(),
                superuser_subject: "superuser".to_owned(),
                superuser_role: "superuser".to_owned(),
                secret_size_bytes: 64,
                token_ttl_seconds: 31_536_000,
                token_refresh_leeway_seconds: 86_400,
            },
        }
    }
}
