use anyhow::{Context, Result, bail};
use pioneer_config::{
    AppConfig, GatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeInstancesConfig,
    GatewayCliAgentRuntimeKindConfig, GatewayConfig, GatewayMemoryConfig,
    GatewayMemoryModelSelectionConfig,
    GatewayMemoryModelSelectionSource as ConfigGatewayMemoryModelSelectionSource,
    GatewayRemoteAccessConfig, GatewayThreadEpisodicConfig,
    GatewayThreadEpisodicVectorProviderConfig, GatewayThreadEpisodicVectorSearchConfig,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thread_episodic: Option<GatewayThreadEpisodicSettingsOverride>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    workspaces: BTreeMap<String, GatewayWorkspaceSettingsOverride>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cli_runtimes: Option<GatewayCliRuntimeSettingsOverride>,
    #[serde(
        default,
        skip_serializing_if = "GatewayRemoteAccessSettingsOverride::is_default"
    )]
    remote_access: GatewayRemoteAccessSettingsOverride,
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
    #[serde(default)]
    thread_episodic: Option<GatewayThreadEpisodicSettingsOverride>,
    #[serde(default)]
    workspaces: BTreeMap<String, GatewayWorkspaceSettingsOverride>,
    #[serde(default)]
    cli_runtimes: Option<GatewayCliRuntimeSettingsOverride>,
    #[serde(default)]
    remote_access: GatewayRemoteAccessSettingsOverride,
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
            thread_episodic: wire.thread_episodic,
            workspaces: wire.workspaces,
            cli_runtimes: wire.cli_runtimes,
            remote_access: wire.remote_access,
            migrated: false,
        };
        settings.migrate_legacy_active_recall_model();
        settings.migrate_default_codex_cli_display_name();
        settings.migrate_default_claude_cli_runtime_instance();
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayThreadEpisodicSettings {
    pub enabled: bool,
    pub indexing_enabled: bool,
    pub recall_enabled: bool,
    pub default_prompt_chars: u32,
    pub max_prompt_chars: u32,
    pub max_hit_chars: u32,
    pub default_max_candidates: u32,
    pub max_candidate_work: u32,
    pub max_segments: u32,
    pub min_relevancy: f32,
    pub min_results: u32,
    pub snippet_chars: u32,
    pub index_batch_limit: u32,
    pub retry_base_delay_secs: i64,
    pub retry_max_delay_secs: i64,
    pub max_attempts: i64,
    pub near_capacity_percent: f64,
    pub vector_search: GatewayThreadEpisodicVectorSearchConfig,
}

impl Default for GatewayThreadEpisodicSettings {
    fn default() -> Self {
        Self::from_gateway_thread_episodic_config(&GatewayThreadEpisodicConfig::default())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayThreadEpisodicSettingsOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    indexing_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recall_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_prompt_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_prompt_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_hit_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_max_candidates: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_candidate_work: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_segments: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    min_relevancy: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    min_results: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snippet_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    index_batch_limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_base_delay_secs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_max_delay_secs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_attempts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    near_capacity_percent: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vector_search: Option<GatewayThreadEpisodicVectorSearchConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayWorkspaceSettingsOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thread_episodic: Option<GatewayWorkspaceThreadEpisodicSettingsOverride>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayWorkspaceThreadEpisodicSettingsOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vector_search: Option<GatewayThreadEpisodicVectorSearchConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayCliRuntimeSettingsOverride {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    instances: Vec<GatewayCliRuntimeInstanceSettingsOverride>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayCliRuntimeInstanceSettingsOverride {
    id: String,
    kind: GatewayCliAgentRuntimeKindConfig,
    display_name: String,
    enabled: bool,
    binary_path: String,
    home_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shadow_home_path: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayRemoteAccessSettingsOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    service_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transport: Option<GatewayRemoteAccessTransportConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    secret_ref: Option<String>,
}

impl GatewayRemoteAccessSettingsOverride {
    const DEFAULT_SECRET_REF: &'static str = "remote_access";

    fn is_default(&self) -> bool {
        self == &Self::default()
    }

    fn secret_ref(&self) -> &str {
        self.secret_ref
            .as_deref()
            .unwrap_or(Self::DEFAULT_SECRET_REF)
    }

    fn service_name(&self, config: &GatewayRemoteAccessConfig) -> String {
        self.service_name
            .clone()
            .unwrap_or_else(|| config.service_name.clone())
    }

    fn apply_protocol_update(
        &mut self,
        update: pioneer_protocol::GatewayRemoteAccessSettingsUpdate,
    ) -> Result<GatewayRemoteAccessChangeSet> {
        let mut changes = GatewayRemoteAccessChangeSet {
            changed: true,
            secret_ref: self.secret_ref().to_owned(),
            ..GatewayRemoteAccessChangeSet::default()
        };

        if let Some(enabled) = update.enabled {
            self.enabled = Some(enabled);
            changes.enabled = Some(enabled);
        }

        if let Some(server) = update.server {
            self.server = normalize_remote_access_optional_text(
                "remote access server",
                server.as_str(),
                512,
            )?;
            changes.server_changed = true;
        }

        if let Some(key) = update.key {
            let key =
                normalize_remote_access_required_text("remote access key", key.as_str(), 4096)?;
            self.secret_ref = Some(changes.secret_ref.clone());
            changes.key = Some(key);
            changes.clear_key = false;
        } else if update.clear_key.unwrap_or(false) {
            changes.clear_key = true;
        }

        Ok(changes)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum GatewayRemoteAccessTransportConfig {
    #[default]
    Tcp,
    Tls,
    Noise,
    Websocket,
}

impl GatewayRemoteAccessTransportConfig {
    fn to_protocol(self) -> pioneer_protocol::GatewayRemoteAccessTransport {
        match self {
            Self::Tcp => pioneer_protocol::GatewayRemoteAccessTransport::Tcp,
            Self::Tls => pioneer_protocol::GatewayRemoteAccessTransport::Tls,
            Self::Noise => pioneer_protocol::GatewayRemoteAccessTransport::Noise,
            Self::Websocket => pioneer_protocol::GatewayRemoteAccessTransport::Websocket,
        }
    }
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

    pub fn effective_thread_episodic_settings(
        &self,
        config: &GatewayThreadEpisodicConfig,
    ) -> GatewayThreadEpisodicSettings {
        let settings = GatewayThreadEpisodicSettings::from_gateway_thread_episodic_config(config);
        if let Some(thread_episodic) = &self.thread_episodic {
            thread_episodic.apply_to_thread_episodic_settings(settings)
        } else {
            settings
        }
    }

    pub fn effective_thread_episodic_settings_for_workspace(
        &self,
        config: &GatewayThreadEpisodicConfig,
        workspace_id: Option<&str>,
    ) -> GatewayThreadEpisodicSettings {
        let mut settings = self.effective_thread_episodic_settings(config);
        if let Some(vector_search) = self.workspace_vector_search_override(workspace_id) {
            settings.vector_search = vector_search.clone();
        }
        settings
    }

    pub fn workspace_thread_episodic_vector_search_configs(
        &self,
    ) -> BTreeMap<String, GatewayThreadEpisodicVectorSearchConfig> {
        self.workspaces
            .iter()
            .filter_map(|(workspace_id, workspace)| {
                workspace
                    .thread_episodic
                    .as_ref()
                    .and_then(|thread_episodic| thread_episodic.vector_search.as_ref())
                    .map(|vector_search| (workspace_id.clone(), vector_search.clone()))
            })
            .collect()
    }

    pub fn effective_cli_runtime_settings(
        &self,
        config: &GatewayConfig,
    ) -> pioneer_protocol::GatewayCliRuntimeSettings {
        cli_runtime_settings_from_gateway_config(&self.apply_to_gateway_config(config.clone()))
    }

    pub fn remote_access_secret_ref(&self) -> Option<&str> {
        if self.remote_access.enabled.unwrap_or(false)
            || self.remote_access.server.is_some()
            || self.remote_access.secret_ref.is_some()
        {
            Some(
                self.remote_access
                    .secret_ref
                    .as_deref()
                    .unwrap_or(GatewayRemoteAccessSettingsOverride::DEFAULT_SECRET_REF),
            )
        } else {
            None
        }
    }

    pub fn effective_remote_access_settings(
        &self,
        config: &GatewayRemoteAccessConfig,
        has_key: bool,
        status: pioneer_protocol::GatewayRemoteAccessStatusSnapshot,
    ) -> pioneer_protocol::GatewayRemoteAccessSettings {
        pioneer_protocol::GatewayRemoteAccessSettings {
            enabled: self.remote_access.enabled.unwrap_or(false),
            server: Some(config.relay_addr.clone()),
            service_name: Some(self.remote_access.service_name(config)),
            transport: self
                .remote_access
                .transport
                .unwrap_or_default()
                .to_protocol(),
            has_key,
            status,
        }
    }

    pub fn set_memory_settings(&mut self, memory: GatewayMemorySettings) {
        self.memory = Some(GatewayMemorySettingsOverride::from_memory_settings(memory));
    }

    pub fn set_thread_episodic_settings(&mut self, thread_episodic: GatewayThreadEpisodicSettings) {
        self.thread_episodic = Some(
            GatewayThreadEpisodicSettingsOverride::from_thread_episodic_settings(thread_episodic),
        );
    }

    fn set_cli_runtime_settings(
        &mut self,
        cli_runtimes: pioneer_protocol::GatewayCliRuntimeSettings,
    ) -> Result<()> {
        self.cli_runtimes = Some(GatewayCliRuntimeSettingsOverride::from_protocol(
            cli_runtimes,
        )?);
        Ok(())
    }

    pub fn snapshot(&self, config: &GatewayConfig) -> pioneer_protocol::GatewaySettingsSnapshot {
        self.snapshot_with_remote_access_status(
            config,
            false,
            pioneer_protocol::GatewayRemoteAccessStatusSnapshot::default(),
        )
    }

    pub fn snapshot_with_remote_access_status(
        &self,
        config: &GatewayConfig,
        has_remote_access_key: bool,
        remote_access_status: pioneer_protocol::GatewayRemoteAccessStatusSnapshot,
    ) -> pioneer_protocol::GatewaySettingsSnapshot {
        self.snapshot_with_remote_access_status_for_workspace(
            config,
            None,
            has_remote_access_key,
            remote_access_status,
        )
    }

    pub fn snapshot_with_remote_access_status_for_workspace(
        &self,
        config: &GatewayConfig,
        workspace_id: Option<&str>,
        has_remote_access_key: bool,
        remote_access_status: pioneer_protocol::GatewayRemoteAccessStatusSnapshot,
    ) -> pioneer_protocol::GatewaySettingsSnapshot {
        let general = self.effective_general_settings(config);
        pioneer_protocol::GatewaySettingsSnapshot {
            general,
            memory: self.effective_memory_settings(&config.memory).to_protocol(),
            thread_episodic: self
                .effective_thread_episodic_settings_for_workspace(
                    &config.thread_episodic,
                    workspace_id,
                )
                .to_protocol(),
            cli_runtimes: self.effective_cli_runtime_settings(config),
            remote_access: self.effective_remote_access_settings(
                &config.remote_access,
                has_remote_access_key,
                remote_access_status,
            ),
        }
    }

    pub fn apply_protocol_update(
        &mut self,
        update: pioneer_protocol::GatewaySettingsUpdate,
    ) -> Result<GatewaySettingsChangeSet> {
        self.apply_protocol_update_for_workspace(update, None)
    }

    pub fn apply_protocol_update_for_workspace(
        &mut self,
        update: pioneer_protocol::GatewaySettingsUpdate,
        workspace_id: Option<&str>,
    ) -> Result<GatewaySettingsChangeSet> {
        let mut changes = GatewaySettingsChangeSet::default();
        if let Some(general) = update.general {
            changes.general = self.general.apply_protocol_update(general);
        }
        if let Some(memory) = update.memory {
            let memory = GatewayMemorySettings::from_protocol(memory);
            self.set_memory_settings(memory);
            changes.memory = true;
        }
        if let Some(thread_episodic) = update.thread_episodic {
            let normalized_workspace_id = normalize_workspace_settings_key(workspace_id);
            if thread_episodic.vector_search.is_some() && normalized_workspace_id.is_none() {
                bail!(
                    "workspace context is required to update thread episodic vector search settings"
                );
            }
            let previous_vector_identity = self
                .thread_episodic_vector_projection_identity_hash_for_workspace(
                    normalized_workspace_id.as_deref(),
                );
            self.apply_thread_episodic_settings_update(
                thread_episodic,
                normalized_workspace_id.as_deref(),
            );
            changes.thread_episodic = true;
            if normalized_workspace_id.is_some() {
                changes.thread_episodic_vector_projection_workspace_id =
                    normalized_workspace_id.clone();
            }
            changes.thread_episodic_vector_projection_changed = previous_vector_identity
                != self.thread_episodic_vector_projection_identity_hash_for_workspace(
                    normalized_workspace_id.as_deref(),
                );
        }
        if let Some(cli_runtimes) = update.cli_runtimes {
            self.set_cli_runtime_settings(cli_runtimes)?;
            changes.cli_runtimes = true;
        }
        if let Some(remote_access) = update.remote_access {
            changes.remote_access = self.remote_access.apply_protocol_update(remote_access)?;
        }
        Ok(changes)
    }

    fn apply_thread_episodic_settings_update(
        &mut self,
        mut update: pioneer_protocol::GatewayThreadEpisodicSettingsUpdate,
        workspace_id: Option<&str>,
    ) {
        let vector_update = update.vector_search.take();
        if thread_episodic_settings_update_has_global_fields(&update) {
            self.thread_episodic
                .get_or_insert_with(GatewayThreadEpisodicSettingsOverride::default)
                .apply_protocol_update(update);
        }
        let Some(vector_update) = vector_update else {
            return;
        };

        match workspace_id {
            Some(workspace_id) => {
                let mut vector_search = self
                    .effective_thread_episodic_vector_search_config_for_workspace(Some(
                        workspace_id,
                    ));
                apply_vector_search_protocol_update(&mut vector_search, vector_update);
                self.workspaces
                    .entry(workspace_id.to_owned())
                    .or_default()
                    .thread_episodic
                    .get_or_insert_with(GatewayWorkspaceThreadEpisodicSettingsOverride::default)
                    .vector_search = Some(vector_search);
            }
            None => {
                let mut vector_search =
                    self.effective_thread_episodic_vector_search_config_for_workspace(None);
                apply_vector_search_protocol_update(&mut vector_search, vector_update);
                self.thread_episodic
                    .get_or_insert_with(GatewayThreadEpisodicSettingsOverride::default)
                    .vector_search = Some(vector_search);
            }
        }
    }

    fn thread_episodic_vector_projection_identity_hash_for_workspace(
        &self,
        workspace_id: Option<&str>,
    ) -> String {
        let vector_search =
            self.effective_thread_episodic_vector_search_config_for_workspace(workspace_id);
        thread_episodic_vector_projection_identity_hash(&vector_search)
    }

    fn effective_thread_episodic_vector_search_config_for_workspace(
        &self,
        workspace_id: Option<&str>,
    ) -> GatewayThreadEpisodicVectorSearchConfig {
        self.workspace_vector_search_override(workspace_id)
            .cloned()
            .or_else(|| {
                self.thread_episodic
                    .as_ref()
                    .and_then(|thread_episodic| thread_episodic.vector_search.clone())
            })
            .unwrap_or_default()
    }

    fn workspace_vector_search_override(
        &self,
        workspace_id: Option<&str>,
    ) -> Option<&GatewayThreadEpisodicVectorSearchConfig> {
        let workspace_id = workspace_id?.trim();
        if workspace_id.is_empty() {
            return None;
        }
        self.workspaces
            .get(workspace_id)
            .and_then(|workspace| workspace.thread_episodic.as_ref())
            .and_then(|thread_episodic| thread_episodic.vector_search.as_ref())
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

    pub fn apply_to_gateway_thread_episodic_config(
        &self,
        config: GatewayThreadEpisodicConfig,
    ) -> GatewayThreadEpisodicConfig {
        if let Some(thread_episodic) = &self.thread_episodic {
            thread_episodic.apply_to_gateway_thread_episodic_config(config)
        } else {
            config
        }
    }

    pub fn apply_to_gateway_config(&self, mut config: GatewayConfig) -> GatewayConfig {
        let general = self.effective_general_settings(&config);
        config.keepawake = general.keepawake;
        config.preflight_model = model_selection_from_protocol(general.preflight_model);
        config.memory = self.apply_to_gateway_memory_config(config.memory);
        config.thread_episodic =
            self.apply_to_gateway_thread_episodic_config(config.thread_episodic);
        if let Some(cli_runtimes) = &self.cli_runtimes {
            config.cli_agent_runtime.enabled = false;
            config.cli_agent_runtimes =
                cli_runtimes.to_gateway_cli_agent_runtime_instances_config();
        } else if config.cli_agent_runtimes.is_empty() && !config.cli_agent_runtime.enabled {
            config.cli_agent_runtime.enabled = true;
        }
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

    fn migrate_default_codex_cli_display_name(&mut self) -> bool {
        let Some(cli_runtimes) = &mut self.cli_runtimes else {
            return false;
        };
        let mut migrated = false;
        for instance in &mut cli_runtimes.instances {
            if instance.id == "codex"
                && instance.kind == GatewayCliAgentRuntimeKindConfig::Codex
                && instance.display_name == "Codex"
            {
                instance.display_name = "Codex CLI".to_owned();
                migrated = true;
            }
        }
        self.migrated |= migrated;
        migrated
    }

    fn migrate_default_claude_cli_runtime_instance(&mut self) -> bool {
        let Some(cli_runtimes) = &mut self.cli_runtimes else {
            return false;
        };
        if !cli_runtime_settings_look_like_legacy_default_codex_only(cli_runtimes) {
            return false;
        }

        cli_runtimes
            .instances
            .push(GatewayCliRuntimeInstanceSettingsOverride {
                id: "claude".to_owned(),
                kind: GatewayCliAgentRuntimeKindConfig::Claude,
                display_name: "Claude CLI".to_owned(),
                enabled: true,
                binary_path: "claude".to_owned(),
                home_path: "~/.claude".to_owned(),
                shadow_home_path: None,
            });
        self.migrated = true;
        true
    }
}

fn cli_runtime_settings_look_like_legacy_default_codex_only(
    cli_runtimes: &GatewayCliRuntimeSettingsOverride,
) -> bool {
    let [instance] = cli_runtimes.instances.as_slice() else {
        return false;
    };

    instance.id == "codex"
        && instance.kind == GatewayCliAgentRuntimeKindConfig::Codex
        && instance.enabled
        && instance.binary_path == "codex"
        && instance.home_path == "~/.codex"
        && instance.shadow_home_path.is_none()
        && (instance.display_name == "Codex" || instance.display_name == "Codex CLI")
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

impl GatewayThreadEpisodicSettings {
    pub fn from_gateway_thread_episodic_config(config: &GatewayThreadEpisodicConfig) -> Self {
        Self {
            enabled: config.enabled,
            indexing_enabled: config.indexing_enabled,
            recall_enabled: config.recall_enabled,
            default_prompt_chars: config.default_prompt_chars,
            max_prompt_chars: config.max_prompt_chars,
            max_hit_chars: config.max_hit_chars.min(u32::MAX as usize) as u32,
            default_max_candidates: config.default_max_candidates,
            max_candidate_work: config.max_candidate_work,
            max_segments: config.max_segments.min(u32::MAX as u64) as u32,
            min_relevancy: config.min_relevancy,
            min_results: config.min_results,
            snippet_chars: config.snippet_chars,
            index_batch_limit: config.index_batch_limit.min(u32::MAX as u64) as u32,
            retry_base_delay_secs: config.retry_base_delay_secs,
            retry_max_delay_secs: config.retry_max_delay_secs,
            max_attempts: config.max_attempts,
            near_capacity_percent: config.near_capacity_percent,
            vector_search: config.vector_search.clone(),
        }
    }

    pub fn from_protocol(settings: pioneer_protocol::GatewayThreadEpisodicSettings) -> Self {
        Self {
            enabled: settings.enabled,
            indexing_enabled: settings.indexing_enabled,
            recall_enabled: settings.recall_enabled,
            default_prompt_chars: settings.default_prompt_chars,
            max_prompt_chars: settings.max_prompt_chars,
            max_hit_chars: settings.max_hit_chars,
            default_max_candidates: settings.default_max_candidates,
            max_candidate_work: settings.max_candidate_work,
            max_segments: settings.max_segments,
            min_relevancy: settings.min_relevancy,
            min_results: settings.min_results,
            snippet_chars: settings.snippet_chars,
            index_batch_limit: settings.index_batch_limit,
            retry_base_delay_secs: settings.retry_base_delay_secs,
            retry_max_delay_secs: settings.retry_max_delay_secs,
            max_attempts: settings.max_attempts,
            near_capacity_percent: settings.near_capacity_percent,
            vector_search: vector_search_config_from_protocol(settings.vector_search),
        }
    }

    pub fn to_protocol(&self) -> pioneer_protocol::GatewayThreadEpisodicSettings {
        pioneer_protocol::GatewayThreadEpisodicSettings {
            enabled: self.enabled,
            indexing_enabled: self.indexing_enabled,
            recall_enabled: self.recall_enabled,
            default_prompt_chars: self.default_prompt_chars,
            max_prompt_chars: self.max_prompt_chars,
            max_hit_chars: self.max_hit_chars,
            default_max_candidates: self.default_max_candidates,
            max_candidate_work: self.max_candidate_work,
            max_segments: self.max_segments,
            min_relevancy: self.min_relevancy,
            min_results: self.min_results,
            snippet_chars: self.snippet_chars,
            index_batch_limit: self.index_batch_limit,
            retry_base_delay_secs: self.retry_base_delay_secs,
            retry_max_delay_secs: self.retry_max_delay_secs,
            max_attempts: self.max_attempts,
            near_capacity_percent: self.near_capacity_percent,
            vector_search: vector_search_config_to_protocol(&self.vector_search),
        }
    }
}

fn vector_search_config_to_protocol(
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings {
    let provider = config.provider.map(vector_provider_to_protocol);
    let local_model = if matches!(
        config.provider,
        Some(GatewayThreadEpisodicVectorProviderConfig::Local)
    ) {
        config.local_model.clone().or_else(|| config.model.clone())
    } else {
        config.local_model.clone()
    };
    let mut settings = pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings {
        enabled: config.enabled,
        provider,
        model: config.model.clone(),
        local_model,
        embedding_dimension: resolved_vector_embedding_dimension(config),
        embedding_normalized: config.embedding_normalized,
        provider_key: pioneer_protocol::GatewayThreadEpisodicVectorProviderKeyStatus {
            required: config.enabled && config.provider.is_some_and(vector_provider_requires_key),
            present: false,
        },
        refill_status: if config.enabled {
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Unknown
        } else {
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Disabled
        },
        local_model_status: if config.enabled
            && matches!(
                config.provider,
                Some(GatewayThreadEpisodicVectorProviderConfig::Local)
            ) {
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Unknown
        } else {
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::NotSelected
        },
    };
    apply_thread_episodic_vector_search_status(&mut settings);
    settings
}

fn vector_search_config_from_protocol(
    settings: pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings,
) -> GatewayThreadEpisodicVectorSearchConfig {
    GatewayThreadEpisodicVectorSearchConfig {
        enabled: settings.enabled,
        provider: settings.provider.map(vector_provider_from_protocol),
        model: settings.model,
        local_model: settings.local_model,
        embedding_normalized: settings.embedding_normalized,
    }
}

pub fn thread_episodic_vector_projection_identity_hash(
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> String {
    let provider = if config.enabled {
        config
            .provider
            .map(vector_provider_identity_name)
            .unwrap_or("missing")
    } else {
        "disabled"
    };
    let model = if !config.enabled {
        ""
    } else if matches!(
        config.provider,
        Some(GatewayThreadEpisodicVectorProviderConfig::Local)
    ) {
        selected_local_vector_model(config).unwrap_or("")
    } else {
        config.model.as_deref().map(str::trim).unwrap_or("")
    };
    thread_episodic_vector_projection_identity_hash_for_parts(
        config.enabled,
        provider,
        model,
        resolved_vector_embedding_dimension(config),
        config.enabled && config.embedding_normalized,
    )
}

pub(crate) fn thread_episodic_vector_projection_identity_hash_for_parts(
    enabled: bool,
    provider: &str,
    model: &str,
    dimension: Option<u32>,
    normalized: bool,
) -> String {
    let dimension = if enabled {
        dimension
            .map(|dimension| dimension.to_string())
            .unwrap_or_else(|| "unknown".to_owned())
    } else {
        "none".to_owned()
    };
    let normalized = if enabled && normalized {
        "true"
    } else {
        "false"
    };
    let identity = format!(
        "thread_episodic_vector_projection_v1\nenabled={}\nprovider={provider}\nmodel={model}\ndimension={dimension}\nnormalized={normalized}\n",
        enabled
    );
    let digest = Sha256::digest(identity.as_bytes());
    hex_lower(digest.as_slice())
}

pub fn apply_thread_episodic_vector_search_status(
    vector_search: &mut pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings,
) {
    let config = vector_search_config_from_protocol(vector_search.clone());
    vector_search.provider_key.required =
        vector_search.enabled && config.provider.is_some_and(vector_provider_requires_key);
    if !vector_search.provider_key.required {
        vector_search.provider_key.present = false;
    }

    if !vector_search.enabled {
        vector_search.refill_status =
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Disabled;
        vector_search.local_model_status =
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::NotSelected;
        return;
    }

    let dimension = resolved_vector_embedding_dimension(&config);
    vector_search.embedding_dimension = dimension;

    if dimension.is_none() && !vector_provider_allows_runtime_dimension_probe(&config) {
        vector_search.refill_status =
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required;
        if matches!(
            config.provider,
            Some(GatewayThreadEpisodicVectorProviderConfig::Local)
        ) {
            vector_search.local_model_status =
                pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Missing;
        }
        return;
    }

    if config.provider.is_some_and(vector_provider_requires_key)
        && !vector_search.provider_key.present
    {
        vector_search.refill_status =
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required;
        vector_search.local_model_status =
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::NotSelected;
        return;
    }

    if matches!(
        config.provider,
        Some(GatewayThreadEpisodicVectorProviderConfig::Local)
    ) {
        vector_search.local_model_status =
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Missing;
        vector_search.refill_status =
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required;
        return;
    }

    vector_search.local_model_status =
        pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::NotSelected;
    vector_search.refill_status =
        pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required;
}

pub fn mark_thread_episodic_vector_refill_running_if_ready(
    vector_search: &mut pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings,
) {
    if !vector_search.enabled
        || vector_search.refill_status
            != pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required
    {
        return;
    }
    if vector_search.provider_key.required && !vector_search.provider_key.present {
        return;
    }

    match vector_search.provider {
        Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenAi)
        | Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenRouter) => {
            if vector_search
                .model
                .as_deref()
                .map(str::trim)
                .filter(|model| !model.is_empty())
                .is_none()
            {
                return;
            }
        }
        Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local) => {
            if vector_search
                .model
                .as_deref()
                .or(vector_search.local_model.as_deref())
                .map(str::trim)
                .filter(|model| !model.is_empty())
                .is_none()
            {
                return;
            }
            if vector_search.local_model_status
                != pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Installed
            {
                return;
            }
        }
        None => return,
    }

    vector_search.refill_status =
        pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Running;
}

fn apply_vector_search_protocol_update(
    config: &mut GatewayThreadEpisodicVectorSearchConfig,
    update: pioneer_protocol::GatewayThreadEpisodicVectorSearchSettingsUpdate,
) {
    if let Some(enabled) = update.enabled {
        config.enabled = enabled;
    }
    if let Some(provider) = update.provider {
        config.provider = provider.map(vector_provider_from_protocol);
    }
    if let Some(model) = update.model {
        config.model = model;
    }
    if let Some(local_model) = update.local_model {
        config.local_model = local_model;
    }
    if let Some(embedding_normalized) = update.embedding_normalized {
        config.embedding_normalized = embedding_normalized;
    }
}

fn normalize_workspace_settings_key(workspace_id: Option<&str>) -> Option<String> {
    workspace_id
        .map(str::trim)
        .filter(|workspace_id| !workspace_id.is_empty())
        .map(ToOwned::to_owned)
}

fn thread_episodic_settings_update_has_global_fields(
    update: &pioneer_protocol::GatewayThreadEpisodicSettingsUpdate,
) -> bool {
    update.enabled.is_some()
        || update.indexing_enabled.is_some()
        || update.recall_enabled.is_some()
        || update.default_prompt_chars.is_some()
        || update.max_prompt_chars.is_some()
        || update.max_hit_chars.is_some()
        || update.default_max_candidates.is_some()
        || update.max_candidate_work.is_some()
        || update.max_segments.is_some()
        || update.min_relevancy.is_some()
        || update.min_results.is_some()
        || update.snippet_chars.is_some()
        || update.index_batch_limit.is_some()
        || update.retry_base_delay_secs.is_some()
        || update.retry_max_delay_secs.is_some()
        || update.max_attempts.is_some()
        || update.near_capacity_percent.is_some()
}

fn vector_provider_to_protocol(
    provider: GatewayThreadEpisodicVectorProviderConfig,
) -> pioneer_protocol::GatewayThreadEpisodicVectorProvider {
    match provider {
        GatewayThreadEpisodicVectorProviderConfig::OpenAi => {
            pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenAi
        }
        GatewayThreadEpisodicVectorProviderConfig::OpenRouter => {
            pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenRouter
        }
        GatewayThreadEpisodicVectorProviderConfig::Local => {
            pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local
        }
    }
}

fn vector_provider_from_protocol(
    provider: pioneer_protocol::GatewayThreadEpisodicVectorProvider,
) -> GatewayThreadEpisodicVectorProviderConfig {
    match provider {
        pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenAi => {
            GatewayThreadEpisodicVectorProviderConfig::OpenAi
        }
        pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenRouter => {
            GatewayThreadEpisodicVectorProviderConfig::OpenRouter
        }
        pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local => {
            GatewayThreadEpisodicVectorProviderConfig::Local
        }
    }
}

fn vector_provider_requires_key(provider: GatewayThreadEpisodicVectorProviderConfig) -> bool {
    matches!(
        provider,
        GatewayThreadEpisodicVectorProviderConfig::OpenAi
            | GatewayThreadEpisodicVectorProviderConfig::OpenRouter
    )
}

pub(crate) fn vector_provider_identity_name(
    provider: GatewayThreadEpisodicVectorProviderConfig,
) -> &'static str {
    match provider {
        GatewayThreadEpisodicVectorProviderConfig::OpenAi => "openai",
        GatewayThreadEpisodicVectorProviderConfig::OpenRouter => "openrouter",
        GatewayThreadEpisodicVectorProviderConfig::Local => "local",
    }
}

pub(crate) fn resolved_vector_embedding_dimension(
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> Option<u32> {
    match config.provider {
        Some(GatewayThreadEpisodicVectorProviderConfig::OpenAi) => {
            match config.model.as_deref().unwrap_or("").trim() {
                "text-embedding-3-small" | "text-embedding-ada-002" => Some(1536),
                "text-embedding-3-large" => Some(3072),
                _ => None,
            }
        }
        Some(GatewayThreadEpisodicVectorProviderConfig::OpenRouter) => {
            match config.model.as_deref().unwrap_or("").trim() {
                "openai/text-embedding-3-small" => Some(1536),
                "openai/text-embedding-3-large" => Some(3072),
                _ => None,
            }
        }
        Some(GatewayThreadEpisodicVectorProviderConfig::Local) => {
            match selected_local_vector_model(config).unwrap_or("") {
                "bge-small-en-v1.5" => Some(384),
                "bge-base-en-v1.5" | "nomic-embed-text-v1.5" => Some(768),
                "gte-large" => Some(1024),
                _ => None,
            }
        }
        None => None,
    }
}

fn vector_provider_allows_runtime_dimension_probe(
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> bool {
    matches!(
        config.provider,
        Some(GatewayThreadEpisodicVectorProviderConfig::OpenRouter)
    ) && config
        .model
        .as_deref()
        .map(str::trim)
        .is_some_and(|model| !model.is_empty())
}

fn selected_local_vector_model(config: &GatewayThreadEpisodicVectorSearchConfig) -> Option<&str> {
    config
        .model
        .as_deref()
        .or(config.local_model.as_deref())
        .map(str::trim)
        .filter(|model| !model.is_empty())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewaySettingsChangeSet {
    pub general: GatewayGeneralSettingsChangeSet,
    pub memory: bool,
    pub thread_episodic: bool,
    pub thread_episodic_vector_projection_changed: bool,
    pub thread_episodic_vector_projection_workspace_id: Option<String>,
    pub cli_runtimes: bool,
    pub remote_access: GatewayRemoteAccessChangeSet,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayGeneralSettingsChangeSet {
    pub keepawake: Option<bool>,
    pub preflight_model: Option<GatewayMemoryModelSelectionConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayRemoteAccessChangeSet {
    pub changed: bool,
    pub enabled: Option<bool>,
    pub server_changed: bool,
    pub secret_ref: String,
    pub key: Option<String>,
    pub clear_key: bool,
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

impl GatewayThreadEpisodicSettingsOverride {
    fn from_thread_episodic_settings(settings: GatewayThreadEpisodicSettings) -> Self {
        Self {
            enabled: Some(settings.enabled),
            indexing_enabled: Some(settings.indexing_enabled),
            recall_enabled: Some(settings.recall_enabled),
            default_prompt_chars: Some(settings.default_prompt_chars),
            max_prompt_chars: Some(settings.max_prompt_chars),
            max_hit_chars: Some(settings.max_hit_chars),
            default_max_candidates: Some(settings.default_max_candidates),
            max_candidate_work: Some(settings.max_candidate_work),
            max_segments: Some(settings.max_segments),
            min_relevancy: Some(settings.min_relevancy),
            min_results: Some(settings.min_results),
            snippet_chars: Some(settings.snippet_chars),
            index_batch_limit: Some(settings.index_batch_limit),
            retry_base_delay_secs: Some(settings.retry_base_delay_secs),
            retry_max_delay_secs: Some(settings.retry_max_delay_secs),
            max_attempts: Some(settings.max_attempts),
            near_capacity_percent: Some(settings.near_capacity_percent),
            vector_search: Some(settings.vector_search),
        }
    }

    fn apply_to_thread_episodic_settings(
        &self,
        mut settings: GatewayThreadEpisodicSettings,
    ) -> GatewayThreadEpisodicSettings {
        if let Some(enabled) = self.enabled {
            settings.enabled = enabled;
        }
        if let Some(indexing_enabled) = self.indexing_enabled {
            settings.indexing_enabled = indexing_enabled;
        }
        if let Some(recall_enabled) = self.recall_enabled {
            settings.recall_enabled = recall_enabled;
        }
        if let Some(default_prompt_chars) = self.default_prompt_chars {
            settings.default_prompt_chars = default_prompt_chars;
        }
        if let Some(max_prompt_chars) = self.max_prompt_chars {
            settings.max_prompt_chars = max_prompt_chars;
        }
        if let Some(max_hit_chars) = self.max_hit_chars {
            settings.max_hit_chars = max_hit_chars;
        }
        if let Some(default_max_candidates) = self.default_max_candidates {
            settings.default_max_candidates = default_max_candidates;
        }
        if let Some(max_candidate_work) = self.max_candidate_work {
            settings.max_candidate_work = max_candidate_work;
        }
        if let Some(max_segments) = self.max_segments {
            settings.max_segments = max_segments;
        }
        if let Some(min_relevancy) = self.min_relevancy {
            settings.min_relevancy = min_relevancy;
        }
        if let Some(min_results) = self.min_results {
            settings.min_results = min_results;
        }
        if let Some(snippet_chars) = self.snippet_chars {
            settings.snippet_chars = snippet_chars;
        }
        if let Some(index_batch_limit) = self.index_batch_limit {
            settings.index_batch_limit = index_batch_limit;
        }
        if let Some(retry_base_delay_secs) = self.retry_base_delay_secs {
            settings.retry_base_delay_secs = retry_base_delay_secs;
        }
        if let Some(retry_max_delay_secs) = self.retry_max_delay_secs {
            settings.retry_max_delay_secs = retry_max_delay_secs;
        }
        if let Some(max_attempts) = self.max_attempts {
            settings.max_attempts = max_attempts;
        }
        if let Some(near_capacity_percent) = self.near_capacity_percent {
            settings.near_capacity_percent = near_capacity_percent;
        }
        if let Some(vector_search) = &self.vector_search {
            settings.vector_search = vector_search.clone();
        }
        settings
    }

    fn apply_protocol_update(
        &mut self,
        update: pioneer_protocol::GatewayThreadEpisodicSettingsUpdate,
    ) {
        if let Some(enabled) = update.enabled {
            self.enabled = Some(enabled);
        }
        if let Some(indexing_enabled) = update.indexing_enabled {
            self.indexing_enabled = Some(indexing_enabled);
        }
        if let Some(recall_enabled) = update.recall_enabled {
            self.recall_enabled = Some(recall_enabled);
        }
        if let Some(default_prompt_chars) = update.default_prompt_chars {
            self.default_prompt_chars = Some(default_prompt_chars);
        }
        if let Some(max_prompt_chars) = update.max_prompt_chars {
            self.max_prompt_chars = Some(max_prompt_chars);
        }
        if let Some(max_hit_chars) = update.max_hit_chars {
            self.max_hit_chars = Some(max_hit_chars);
        }
        if let Some(default_max_candidates) = update.default_max_candidates {
            self.default_max_candidates = Some(default_max_candidates);
        }
        if let Some(max_candidate_work) = update.max_candidate_work {
            self.max_candidate_work = Some(max_candidate_work);
        }
        if let Some(max_segments) = update.max_segments {
            self.max_segments = Some(max_segments);
        }
        if let Some(min_relevancy) = update.min_relevancy {
            self.min_relevancy = Some(min_relevancy);
        }
        if let Some(min_results) = update.min_results {
            self.min_results = Some(min_results);
        }
        if let Some(snippet_chars) = update.snippet_chars {
            self.snippet_chars = Some(snippet_chars);
        }
        if let Some(index_batch_limit) = update.index_batch_limit {
            self.index_batch_limit = Some(index_batch_limit);
        }
        if let Some(retry_base_delay_secs) = update.retry_base_delay_secs {
            self.retry_base_delay_secs = Some(retry_base_delay_secs);
        }
        if let Some(retry_max_delay_secs) = update.retry_max_delay_secs {
            self.retry_max_delay_secs = Some(retry_max_delay_secs);
        }
        if let Some(max_attempts) = update.max_attempts {
            self.max_attempts = Some(max_attempts);
        }
        if let Some(near_capacity_percent) = update.near_capacity_percent {
            self.near_capacity_percent = Some(near_capacity_percent);
        }
        if let Some(vector_update) = update.vector_search {
            let mut vector_search = self.vector_search.clone().unwrap_or_default();
            apply_vector_search_protocol_update(&mut vector_search, vector_update);
            self.vector_search = Some(vector_search);
        }
    }

    fn apply_to_gateway_thread_episodic_config(
        &self,
        mut config: GatewayThreadEpisodicConfig,
    ) -> GatewayThreadEpisodicConfig {
        let settings = self.apply_to_thread_episodic_settings(
            GatewayThreadEpisodicSettings::from_gateway_thread_episodic_config(&config),
        );
        config.enabled = settings.enabled;
        config.indexing_enabled = settings.indexing_enabled;
        config.recall_enabled = settings.recall_enabled;
        config.default_prompt_chars = settings.default_prompt_chars;
        config.max_prompt_chars = settings.max_prompt_chars;
        config.max_hit_chars = settings.max_hit_chars as usize;
        config.default_max_candidates = settings.default_max_candidates;
        config.max_candidate_work = settings.max_candidate_work;
        config.max_segments = settings.max_segments as u64;
        config.min_relevancy = settings.min_relevancy;
        config.min_results = settings.min_results;
        config.snippet_chars = settings.snippet_chars;
        config.index_batch_limit = settings.index_batch_limit as u64;
        config.retry_base_delay_secs = settings.retry_base_delay_secs;
        config.retry_max_delay_secs = settings.retry_max_delay_secs;
        config.max_attempts = settings.max_attempts;
        config.near_capacity_percent = settings.near_capacity_percent;
        config.vector_search = settings.vector_search;
        config
    }
}

impl GatewayCliRuntimeSettingsOverride {
    fn default_supported() -> Result<Self> {
        Self::from_protocol(pioneer_protocol::GatewayCliRuntimeSettings::default())
    }

    fn from_protocol(settings: pioneer_protocol::GatewayCliRuntimeSettings) -> Result<Self> {
        let mut normalized_instances = Vec::with_capacity(settings.instances.len());
        let mut ids = HashSet::new();
        let mut display_names = HashSet::new();

        for instance in settings.instances {
            let id = normalize_cli_runtime_instance_id(instance.id.as_str())?;
            if !ids.insert(id.clone()) {
                bail!("duplicate CLI runtime instance id `{id}`");
            }

            let display_name =
                normalize_cli_runtime_display_name(instance.display_name.as_str(), id.as_str())?;
            let display_name_key = display_name.to_ascii_lowercase();
            if !display_names.insert(display_name_key) {
                bail!("duplicate CLI runtime display name `{display_name}`");
            }

            let binary_path =
                normalize_cli_runtime_required_path("binary_path", instance.binary_path.as_str())?;
            let home_path =
                normalize_cli_runtime_required_path("home_path", instance.home_path.as_str())?;
            let shadow_home_path = normalize_cli_runtime_optional_path(
                "shadow_home_path",
                instance.shadow_home_path.as_deref(),
            )?;
            if shadow_home_path.as_deref() == Some(home_path.as_str()) {
                bail!("shadow_home_path must differ from home_path for CLI runtime `{id}`");
            }

            normalized_instances.push(GatewayCliRuntimeInstanceSettingsOverride {
                id,
                kind: cli_runtime_kind_from_protocol(instance.kind)?,
                display_name,
                enabled: instance.enabled,
                binary_path,
                home_path,
                shadow_home_path,
            });
        }

        Ok(Self {
            instances: normalized_instances,
        })
    }

    fn to_gateway_cli_agent_runtime_instances_config(
        &self,
    ) -> GatewayCliAgentRuntimeInstancesConfig {
        let instances = self
            .instances
            .iter()
            .map(|instance| {
                (
                    instance.id.clone(),
                    GatewayCliAgentRuntimeInstanceConfig {
                        id: instance.id.clone(),
                        kind: instance.kind,
                        display_name: Some(instance.display_name.clone()),
                        enabled: Some(instance.enabled),
                        binary_path: Some(instance.binary_path.clone()),
                        home_path: Some(instance.home_path.clone()),
                        shadow_home_path: instance.shadow_home_path.clone(),
                        custom_models: Vec::new(),
                        app_server_args: Vec::new(),
                        startup_probe_timeout_ms: None,
                        request_timeout_ms: None,
                        idle_session_ttl_secs: None,
                        event_channel_capacity: None,
                        stderr_ring_lines: None,
                        debug_native_events: None,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        GatewayCliAgentRuntimeInstancesConfig { instances }
    }
}

fn cli_runtime_settings_from_gateway_config(
    config: &GatewayConfig,
) -> pioneer_protocol::GatewayCliRuntimeSettings {
    pioneer_protocol::GatewayCliRuntimeSettings {
        instances: config
            .effective_cli_agent_runtime_instances()
            .into_iter()
            .map(
                |instance| pioneer_protocol::GatewayCliRuntimeInstanceSettings {
                    id: instance.id,
                    kind: cli_runtime_kind_to_protocol(instance.kind),
                    display_name: instance.display_name,
                    enabled: instance.enabled,
                    binary_path: instance.binary_path,
                    home_path: instance.home_path,
                    shadow_home_path: instance.shadow_home_path,
                },
            )
            .collect(),
    }
}

fn cli_runtime_kind_to_protocol(
    kind: GatewayCliAgentRuntimeKindConfig,
) -> pioneer_protocol::CLIAgentRuntimeKind {
    match kind {
        GatewayCliAgentRuntimeKindConfig::Codex => pioneer_protocol::CLIAgentRuntimeKind::Codex,
        GatewayCliAgentRuntimeKindConfig::Claude => pioneer_protocol::CLIAgentRuntimeKind::Claude,
    }
}

fn cli_runtime_kind_from_protocol(
    kind: pioneer_protocol::CLIAgentRuntimeKind,
) -> Result<GatewayCliAgentRuntimeKindConfig> {
    match kind {
        pioneer_protocol::CLIAgentRuntimeKind::Codex => Ok(GatewayCliAgentRuntimeKindConfig::Codex),
        pioneer_protocol::CLIAgentRuntimeKind::Claude => {
            Ok(GatewayCliAgentRuntimeKindConfig::Claude)
        }
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

fn normalize_cli_runtime_instance_id(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("CLI runtime instance id must not be empty");
    }

    let mut normalized = String::new();
    let mut previous_separator = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            previous_separator = false;
        } else if ch == '_' || ch == '-' || ch == '.' || ch.is_ascii_whitespace() {
            if !normalized.is_empty() && !previous_separator {
                normalized.push('_');
                previous_separator = true;
            }
        } else {
            bail!("CLI runtime instance id `{raw}` contains unsupported character `{ch}`");
        }
    }

    let normalized = normalized.trim_matches('_').to_owned();
    if normalized.is_empty() {
        bail!("CLI runtime instance id `{raw}` must contain an ASCII letter or digit");
    }
    if normalized.chars().count() > 64 {
        bail!("CLI runtime instance id `{raw}` must be at most 64 characters");
    }
    Ok(normalized)
}

fn normalize_cli_runtime_display_name(raw: &str, id: &str) -> Result<String> {
    let trimmed = raw.trim();
    let display_name = if trimmed.is_empty() {
        cli_runtime_display_name_from_id(id)
    } else {
        trimmed.to_owned()
    };
    if display_name.chars().count() > 80 {
        bail!("CLI runtime display name `{display_name}` must be at most 80 characters");
    }
    if display_name.chars().any(is_disallowed_settings_text_char) {
        bail!("CLI runtime display name `{display_name}` contains unsupported control characters");
    }
    Ok(display_name)
}

fn normalize_cli_runtime_required_path(field: &str, raw: &str) -> Result<String> {
    let Some(value) = normalize_cli_runtime_optional_path(field, Some(raw))? else {
        bail!("CLI runtime `{field}` must not be empty");
    };
    Ok(value)
}

fn normalize_cli_runtime_optional_path(field: &str, raw: Option<&str>) -> Result<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > 512 {
        bail!("CLI runtime `{field}` must be at most 512 characters");
    }
    if trimmed.chars().any(is_disallowed_settings_text_char) {
        bail!("CLI runtime `{field}` contains unsupported control characters");
    }
    Ok(Some(trimmed.to_owned()))
}

fn normalize_remote_access_required_text(
    field: &str,
    raw: &str,
    max_chars: usize,
) -> Result<String> {
    let Some(value) = normalize_remote_access_optional_text(field, raw, max_chars)? else {
        bail!("{field} must not be empty");
    };
    Ok(value)
}

fn normalize_remote_access_optional_text(
    field: &str,
    raw: &str,
    max_chars: usize,
) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > max_chars {
        bail!("{field} must be at most {max_chars} characters");
    }
    if trimmed.chars().any(is_disallowed_settings_text_char) {
        bail!("{field} contains unsupported control characters");
    }
    Ok(Some(trimmed.to_owned()))
}

fn is_disallowed_settings_text_char(ch: char) -> bool {
    ch == '\0' || ch == '\n' || ch == '\r' || ch.is_control()
}

fn cli_runtime_display_name_from_id(id: &str) -> String {
    id.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            let mut word = String::new();
            word.push(first.to_ascii_uppercase());
            word.push_str(chars.as_str());
            word
        })
        .collect::<Vec<_>>()
        .join(" ")
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
        thread_episodic: None,
        workspaces: BTreeMap::new(),
        cli_runtimes: Some(GatewayCliRuntimeSettingsOverride::default_supported()?),
        remote_access: GatewayRemoteAccessSettingsOverride::default(),
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
    settings.migrate_default_codex_cli_display_name();
    settings.migrate_default_claude_cli_runtime_instance();
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
        GatewayArtifactsConfig, GatewayAuthConfig, GatewayCliAgentRuntimeConfig,
        GatewayCliAgentRuntimeInstancesConfig, GatewayComputerUseToolsConfig, GatewayConfig,
        GatewayDatabaseConfig, GatewayExecutionWindowsConfig, GatewayMemoryConfig,
        GatewayMemoryModelSelectionConfig, GatewayProviderConfig, GatewaySkillsConfig,
        GatewayThreadConfig, GatewayThreadEpisodicConfig,
        GatewayThreadEpisodicVectorProviderConfig, GatewayThreadEpisodicVectorSearchConfig,
        GatewayToolLoopBudgetConfig, GatewayToolRetryBudgetConfig, GatewayToolsConfig,
        GatewayWebToolsConfig,
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
        assert!(!content.contains("[thread_episodic]"));
        assert!(content.contains("[[cli_runtimes.instances]]"));
        assert!(content.contains("id = \"codex\""));
        assert!(content.contains("display_name = \"Codex CLI\""));
        assert!(content.contains("enabled = true"));

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

        settings
            .apply_protocol_update(pioneer_protocol::GatewaySettingsUpdate {
                general: Some(pioneer_protocol::GatewayGeneralSettingsUpdate {
                    keepawake: Some(true),
                    preflight_model: Some(pioneer_protocol::GatewayMemoryModelSelection::custom(
                        "planner-provider",
                        "planner-model",
                    )),
                }),
                memory: None,
                thread_episodic: None,
                cli_runtimes: None,
                remote_access: None,
            })
            .expect("settings update should apply");
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
    fn gateway_settings_thread_episodic_overrides_gateway_config_without_storage_fields() {
        let settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"

[thread_episodic]
enabled = true
indexing_enabled = false
recall_enabled = false
default_prompt_chars = 1200
max_prompt_chars = 4800
max_hit_chars = 600
default_max_candidates = 12
max_candidate_work = 24
max_segments = 4
min_relevancy = 0.45
min_results = 2
snippet_chars = 240
index_batch_limit = 4
retry_base_delay_secs = 12
retry_max_delay_secs = 120
max_attempts = 3
near_capacity_percent = 75.0

[thread_episodic.vector_search]
enabled = true
provider = "openrouter"
model = "openai/text-embedding-3-small"
local_model = "bge-base-en-v1.5"
embedding_dimension = 1536
embedding_normalized = true
"#,
        )
        .expect("gateway settings should parse");

        let base = GatewayThreadEpisodicConfig {
            enabled: false,
            indexing_enabled: true,
            recall_enabled: true,
            default_prompt_chars: 2400,
            max_prompt_chars: 12_000,
            max_hit_chars: 1_200,
            default_max_candidates: 32,
            max_candidate_work: 128,
            max_segments: 16,
            min_relevancy: 0.25,
            min_results: 1,
            snippet_chars: 360,
            index_batch_limit: 16,
            retry_base_delay_secs: 30,
            retry_max_delay_secs: 900,
            max_attempts: 5,
            near_capacity_percent: 85.0,
            vector_search: pioneer_config::GatewayThreadEpisodicVectorSearchConfig::default(),
        };

        let mapped = settings.apply_to_gateway_thread_episodic_config(base);

        assert!(mapped.enabled);
        assert!(!mapped.indexing_enabled);
        assert!(!mapped.recall_enabled);
        assert_eq!(mapped.default_prompt_chars, 1200);
        assert_eq!(mapped.max_prompt_chars, 4800);
        assert_eq!(mapped.max_hit_chars, 600);
        assert_eq!(mapped.default_max_candidates, 12);
        assert_eq!(mapped.max_candidate_work, 24);
        assert_eq!(mapped.max_segments, 4);
        assert_eq!(mapped.min_relevancy, 0.45);
        assert_eq!(mapped.min_results, 2);
        assert_eq!(mapped.snippet_chars, 240);
        assert_eq!(mapped.index_batch_limit, 4);
        assert_eq!(mapped.retry_base_delay_secs, 12);
        assert_eq!(mapped.retry_max_delay_secs, 120);
        assert_eq!(mapped.max_attempts, 3);
        assert_eq!(mapped.near_capacity_percent, 75.0);
        assert!(mapped.vector_search.enabled);
        assert_eq!(
            mapped.vector_search.provider,
            Some(pioneer_config::GatewayThreadEpisodicVectorProviderConfig::OpenRouter)
        );
        assert_eq!(
            mapped.vector_search.model.as_deref(),
            Some("openai/text-embedding-3-small")
        );
        assert_eq!(
            mapped.vector_search.local_model.as_deref(),
            Some("bge-base-en-v1.5")
        );
        assert!(mapped.vector_search.embedding_normalized);
    }

    #[test]
    fn vector_projection_hash_tracks_active_identity_but_not_disabled_model_fields() {
        let disabled = GatewayThreadEpisodicVectorSearchConfig {
            enabled: false,
            provider: Some(GatewayThreadEpisodicVectorProviderConfig::OpenAi),
            model: Some("text-embedding-3-small".to_owned()),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            embedding_normalized: true,
        };
        let disabled_changed_model = GatewayThreadEpisodicVectorSearchConfig {
            model: Some("text-embedding-3-large".to_owned()),
            provider: Some(GatewayThreadEpisodicVectorProviderConfig::OpenRouter),
            ..disabled.clone()
        };
        assert_eq!(
            super::thread_episodic_vector_projection_identity_hash(&disabled),
            super::thread_episodic_vector_projection_identity_hash(&disabled_changed_model)
        );

        let enabled_openai = GatewayThreadEpisodicVectorSearchConfig {
            enabled: true,
            ..disabled
        };
        let enabled_openrouter = GatewayThreadEpisodicVectorSearchConfig {
            provider: Some(GatewayThreadEpisodicVectorProviderConfig::OpenRouter),
            model: Some("openai/text-embedding-3-small".to_owned()),
            ..enabled_openai.clone()
        };
        assert_ne!(
            super::thread_episodic_vector_projection_identity_hash(&enabled_openai),
            super::thread_episodic_vector_projection_identity_hash(&enabled_openrouter)
        );
    }

    #[test]
    fn vector_search_status_marks_missing_provider_key_and_local_model_not_ready() {
        let mut disabled = pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings {
            enabled: false,
            provider: Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenAi),
            provider_key: pioneer_protocol::GatewayThreadEpisodicVectorProviderKeyStatus {
                required: true,
                present: true,
            },
            local_model_status:
                pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Missing,
            refill_status: pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required,
            ..Default::default()
        };
        super::apply_thread_episodic_vector_search_status(&mut disabled);
        assert!(!disabled.provider_key.required);
        assert!(!disabled.provider_key.present);
        assert_eq!(
            disabled.local_model_status,
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::NotSelected
        );
        assert_eq!(
            disabled.refill_status,
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Disabled
        );

        let mut openai = pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenAi),
            model: Some("text-embedding-3-small".to_owned()),
            ..Default::default()
        };
        super::apply_thread_episodic_vector_search_status(&mut openai);
        assert!(openai.provider_key.required);
        assert!(!openai.provider_key.present);
        assert_eq!(openai.embedding_dimension, Some(1536));
        assert_eq!(
            openai.refill_status,
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required
        );

        openai.provider_key.present = true;
        super::apply_thread_episodic_vector_search_status(&mut openai);
        assert_eq!(
            openai.refill_status,
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required
        );

        let mut local = pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            ..Default::default()
        };
        super::apply_thread_episodic_vector_search_status(&mut local);
        assert!(!local.provider_key.required);
        assert_eq!(local.embedding_dimension, Some(384));
        assert_eq!(
            local.local_model_status,
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Missing
        );
        assert_eq!(
            local.refill_status,
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required
        );
    }

    #[test]
    fn vector_refill_running_status_requires_startable_refill() {
        let mut remote_ready = pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenRouter),
            model: Some("openai/text-embedding-3-small".to_owned()),
            provider_key: pioneer_protocol::GatewayThreadEpisodicVectorProviderKeyStatus {
                required: true,
                present: true,
            },
            refill_status: pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required,
            ..Default::default()
        };
        super::mark_thread_episodic_vector_refill_running_if_ready(&mut remote_ready);
        assert_eq!(
            remote_ready.refill_status,
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Running
        );

        let mut remote_missing_key = pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings {
            provider_key: pioneer_protocol::GatewayThreadEpisodicVectorProviderKeyStatus {
                required: true,
                present: false,
            },
            ..remote_ready.clone()
        };
        remote_missing_key.refill_status =
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required;
        super::mark_thread_episodic_vector_refill_running_if_ready(&mut remote_missing_key);
        assert_eq!(
            remote_missing_key.refill_status,
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required
        );

        let mut local_missing = pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local),
            model: Some("bge-small-en-v1.5".to_owned()),
            local_model_status:
                pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Missing,
            refill_status: pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required,
            ..Default::default()
        };
        super::mark_thread_episodic_vector_refill_running_if_ready(&mut local_missing);
        assert_eq!(
            local_missing.refill_status,
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required
        );
    }

    #[test]
    fn vector_disable_settings_change_set_marks_model_change_and_disable_transition() {
        let mut settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"

[thread_episodic.vector_search]
enabled = true
provider = "openai"
model = "text-embedding-3-small"
local_model = "bge-small-en-v1.5"
embedding_normalized = true
"#,
        )
        .expect("gateway settings should parse");

        let model_change = settings
            .apply_protocol_update_for_workspace(
                pioneer_protocol::GatewaySettingsUpdate {
                    thread_episodic: Some(pioneer_protocol::GatewayThreadEpisodicSettingsUpdate {
                        vector_search: Some(
                            pioneer_protocol::GatewayThreadEpisodicVectorSearchSettingsUpdate {
                                model: Some(Some("text-embedding-3-large".to_owned())),
                                ..Default::default()
                            },
                        ),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Some("workspace_a"),
            )
            .expect("model update should apply");
        assert!(model_change.thread_episodic);
        assert!(model_change.thread_episodic_vector_projection_changed);

        let disable = settings
            .apply_protocol_update_for_workspace(
                pioneer_protocol::GatewaySettingsUpdate {
                    thread_episodic: Some(pioneer_protocol::GatewayThreadEpisodicSettingsUpdate {
                        vector_search: Some(
                            pioneer_protocol::GatewayThreadEpisodicVectorSearchSettingsUpdate {
                                enabled: Some(false),
                                ..Default::default()
                            },
                        ),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Some("workspace_a"),
            )
            .expect("disable update should apply");
        assert!(disable.thread_episodic_vector_projection_changed);

        let already_disabled = settings
            .apply_protocol_update_for_workspace(
                pioneer_protocol::GatewaySettingsUpdate {
                    thread_episodic: Some(pioneer_protocol::GatewayThreadEpisodicSettingsUpdate {
                        vector_search: Some(
                            pioneer_protocol::GatewayThreadEpisodicVectorSearchSettingsUpdate {
                                enabled: Some(false),
                                provider: Some(Some(
                                    pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenRouter,
                                )),
                                model: Some(Some("different-disabled-model".to_owned())),
                                local_model: Some(Some("different-disabled-local".to_owned())),
                                embedding_normalized: Some(false),
                            },
                        ),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Some("workspace_a"),
            )
            .expect("already-disabled update should apply");
        assert!(!already_disabled.thread_episodic_vector_projection_changed);
    }

    #[test]
    fn saves_gateway_thread_episodic_settings_in_gateway_settings_file() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        let mut settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should be created");

        settings.set_thread_episodic_settings(super::GatewayThreadEpisodicSettings {
            enabled: true,
            indexing_enabled: false,
            recall_enabled: false,
            default_prompt_chars: 1200,
            max_prompt_chars: 4800,
            max_hit_chars: 600,
            default_max_candidates: 12,
            max_candidate_work: 24,
            max_segments: 4,
            min_relevancy: 0.45,
            min_results: 2,
            snippet_chars: 240,
            index_batch_limit: 4,
            retry_base_delay_secs: 12,
            retry_max_delay_secs: 120,
            max_attempts: 3,
            near_capacity_percent: 75.0,
            vector_search: pioneer_config::GatewayThreadEpisodicVectorSearchConfig::default(),
        });
        save_gateway_settings(&path, &settings).expect("settings should save");

        let content = fs::read_to_string(&path).expect("read settings");
        assert!(content.contains("[thread_episodic]"));
        assert!(content.contains("indexing_enabled = false"));
        assert!(content.contains("recall_enabled = false"));
        assert!(content.contains("default_prompt_chars = 1200"));
        assert!(content.contains("min_relevancy = 0.45"));
        assert!(!content.contains("capsules_dir"));
        assert!(!content.contains("thread_episodic/"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn partial_gateway_thread_episodic_update_saves_enabled_without_hidden_fields() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        let mut settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should be created");

        let changes = settings
            .apply_protocol_update(pioneer_protocol::GatewaySettingsUpdate {
                thread_episodic: Some(
                    pioneer_protocol::GatewayThreadEpisodicSettingsUpdate::enabled(false),
                ),
                ..pioneer_protocol::GatewaySettingsUpdate::default()
            })
            .expect("settings update should apply");
        assert!(changes.thread_episodic);
        save_gateway_settings(&path, &settings).expect("settings should save");

        let content = fs::read_to_string(&path).expect("read settings");
        assert!(content.contains("[thread_episodic]"));
        assert!(content.contains("enabled = false"));
        assert!(!content.contains("indexing_enabled"));
        assert!(!content.contains("recall_enabled"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn gateway_thread_episodic_snapshot_exposes_vector_status_without_key_values() {
        let settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"
"#,
        )
        .expect("gateway settings should parse");
        let mut config = gateway_config_with_keepawake(false);
        config.thread_episodic.vector_search.enabled = true;
        config.thread_episodic.vector_search.provider =
            Some(pioneer_config::GatewayThreadEpisodicVectorProviderConfig::OpenRouter);
        config.thread_episodic.vector_search.model =
            Some("openai/text-embedding-3-small".to_owned());

        let snapshot = settings.snapshot(&config);

        assert!(snapshot.thread_episodic.vector_search.enabled);
        assert_eq!(
            snapshot.thread_episodic.vector_search.provider,
            Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenRouter)
        );
        assert_eq!(
            snapshot.thread_episodic.vector_search.model.as_deref(),
            Some("openai/text-embedding-3-small")
        );
        assert_eq!(
            snapshot.thread_episodic.vector_search.embedding_dimension,
            Some(1536)
        );
        assert!(snapshot.thread_episodic.vector_search.provider_key.required);
        assert!(!snapshot.thread_episodic.vector_search.provider_key.present);
        assert_eq!(
            snapshot.thread_episodic.vector_search.refill_status,
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required
        );

        let serialized = serde_json::to_string(&snapshot).expect("snapshot serializes");
        assert!(serialized.contains("provider_key"));
        assert!(!serialized.contains("api_key"));
        assert!(!serialized.contains("sk-"));
    }

    #[test]
    fn gateway_thread_episodic_protocol_update_applies_vector_settings_without_secrets() {
        let mut settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"
"#,
        )
        .expect("gateway settings should parse");

        let changes = settings
            .apply_protocol_update_for_workspace(
                pioneer_protocol::GatewaySettingsUpdate {
                    thread_episodic: Some(pioneer_protocol::GatewayThreadEpisodicSettingsUpdate {
                        vector_search: Some(
                            pioneer_protocol::GatewayThreadEpisodicVectorSearchSettingsUpdate {
                                enabled: Some(true),
                                provider: Some(Some(
                                    pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local,
                                )),
                                model: Some(Some("bge-base-en-v1.5".to_owned())),
                                local_model: Some(Some("bge-base-en-v1.5".to_owned())),
                                embedding_normalized: Some(true),
                            },
                        ),
                        ..pioneer_protocol::GatewayThreadEpisodicSettingsUpdate::default()
                    }),
                    ..pioneer_protocol::GatewaySettingsUpdate::default()
                },
                Some("workspace_a"),
            )
            .expect("vector settings update should apply");

        assert!(changes.thread_episodic);
        let config = gateway_config_with_keepawake(false);
        let snapshot = settings.snapshot_with_remote_access_status_for_workspace(
            &config,
            Some("workspace_a"),
            false,
            pioneer_protocol::GatewayRemoteAccessStatusSnapshot::default(),
        );
        let mapped = pioneer_config::GatewayThreadEpisodicVectorSearchConfig::from(
            crate::settings::GatewayThreadEpisodicSettings::from_protocol(snapshot.thread_episodic)
                .vector_search,
        );
        assert!(mapped.enabled);
        assert_eq!(
            mapped.provider,
            Some(pioneer_config::GatewayThreadEpisodicVectorProviderConfig::Local)
        );
        assert_eq!(mapped.model.as_deref(), Some("bge-base-en-v1.5"));
        assert_eq!(mapped.local_model.as_deref(), Some("bge-base-en-v1.5"));

        let serialized = toml::to_string(&settings).expect("settings should serialize");
        assert!(serialized.contains("[workspaces.workspace_a.thread_episodic.vector_search]"));
        assert!(serialized.contains("provider = \"local\""));
        assert!(!serialized.contains("api_key"));
        assert!(!serialized.contains("sk-"));
    }

    #[test]
    fn gateway_thread_episodic_vector_update_requires_workspace_scope() {
        let mut settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"
"#,
        )
        .expect("gateway settings should parse");

        let error = settings
            .apply_protocol_update(pioneer_protocol::GatewaySettingsUpdate {
                thread_episodic: Some(pioneer_protocol::GatewayThreadEpisodicSettingsUpdate {
                    vector_search: Some(
                        pioneer_protocol::GatewayThreadEpisodicVectorSearchSettingsUpdate {
                            enabled: Some(true),
                            ..Default::default()
                        },
                    ),
                    ..pioneer_protocol::GatewayThreadEpisodicSettingsUpdate::default()
                }),
                ..pioneer_protocol::GatewaySettingsUpdate::default()
            })
            .expect_err("vector settings update without workspace should fail");

        assert!(format!("{error:#}").contains(
            "workspace context is required to update thread episodic vector search settings"
        ));
    }

    #[test]
    fn gateway_thread_episodic_workspace_vector_update_is_workspace_scoped() {
        let mut settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"
"#,
        )
        .expect("gateway settings should parse");

        let update = pioneer_protocol::GatewaySettingsUpdate {
            thread_episodic: Some(pioneer_protocol::GatewayThreadEpisodicSettingsUpdate {
                vector_search: Some(
                    pioneer_protocol::GatewayThreadEpisodicVectorSearchSettingsUpdate {
                        enabled: Some(true),
                        provider: Some(Some(
                            pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenRouter,
                        )),
                        model: Some(Some("openai/text-embedding-3-small".to_owned())),
                        local_model: Some(None),
                        embedding_normalized: Some(true),
                    },
                ),
                ..pioneer_protocol::GatewayThreadEpisodicSettingsUpdate::default()
            }),
            ..pioneer_protocol::GatewaySettingsUpdate::default()
        };
        let changes = settings
            .apply_protocol_update_for_workspace(update, Some("workspace_a"))
            .expect("workspace vector settings update should apply");

        assert!(changes.thread_episodic);
        assert!(changes.thread_episodic_vector_projection_changed);
        assert_eq!(
            changes
                .thread_episodic_vector_projection_workspace_id
                .as_deref(),
            Some("workspace_a")
        );

        let global = settings.apply_to_gateway_thread_episodic_config(
            pioneer_config::GatewayThreadEpisodicConfig::default(),
        );
        assert!(!global.vector_search.enabled);

        let config = gateway_config_with_keepawake(false);
        let workspace_a = settings.snapshot_with_remote_access_status_for_workspace(
            &config,
            Some("workspace_a"),
            false,
            pioneer_protocol::GatewayRemoteAccessStatusSnapshot::default(),
        );
        let workspace_b = settings.snapshot_with_remote_access_status_for_workspace(
            &config,
            Some("workspace_b"),
            false,
            pioneer_protocol::GatewayRemoteAccessStatusSnapshot::default(),
        );

        assert!(workspace_a.thread_episodic.vector_search.enabled);
        assert_eq!(
            workspace_a.thread_episodic.vector_search.provider,
            Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::OpenRouter)
        );
        assert_eq!(
            workspace_a.thread_episodic.vector_search.model.as_deref(),
            Some("openai/text-embedding-3-small")
        );
        assert!(!workspace_b.thread_episodic.vector_search.enabled);
        let workspace_configs = settings.workspace_thread_episodic_vector_search_configs();
        assert_eq!(workspace_configs.len(), 1);
        assert!(workspace_configs.contains_key("workspace_a"));
    }

    #[test]
    fn gateway_remote_access_update_keeps_key_out_of_settings_file() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        let mut settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should be created");

        let changes = settings
            .apply_protocol_update(pioneer_protocol::GatewaySettingsUpdate {
                remote_access: Some(pioneer_protocol::GatewayRemoteAccessSettingsUpdate {
                    enabled: Some(true),
                    server: None,
                    key: Some(" tunnel-token ".to_owned()),
                    clear_key: None,
                }),
                ..pioneer_protocol::GatewaySettingsUpdate::default()
            })
            .expect("remote access settings should apply");

        assert!(changes.remote_access.changed);
        assert_eq!(changes.remote_access.secret_ref, "remote_access");
        assert_eq!(changes.remote_access.key.as_deref(), Some("tunnel-token"));

        let snapshot = settings.snapshot_with_remote_access_status(
            &gateway_config_with_keepawake(false),
            true,
            pioneer_protocol::GatewayRemoteAccessStatusSnapshot::default(),
        );
        assert!(snapshot.remote_access.enabled);
        assert_eq!(
            snapshot.remote_access.server.as_deref(),
            Some("relay-eu-west-1.getpioneer.dev:2333")
        );
        assert!(snapshot.remote_access.has_key);

        save_gateway_settings(&path, &settings).expect("settings should save");
        let content = fs::read_to_string(&path).expect("read settings");
        assert!(content.contains("[remote_access]"));
        assert!(content.contains("enabled = true"));
        assert!(!content.contains("server = "));
        assert!(!content.contains("tunnel-token"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn gateway_settings_cli_runtimes_override_gateway_config_and_save_cleanly() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        let mut settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should be created");

        let changes = settings
            .apply_protocol_update(pioneer_protocol::GatewaySettingsUpdate {
                cli_runtimes: Some(pioneer_protocol::GatewayCliRuntimeSettings {
                    instances: vec![
                        pioneer_protocol::GatewayCliRuntimeInstanceSettings {
                            id: "Codex Personal".to_owned(),
                            kind: pioneer_protocol::CLIAgentRuntimeKind::Codex,
                            display_name: "Codex Personal".to_owned(),
                            enabled: true,
                            binary_path: "codex".to_owned(),
                            home_path: "~/.codex".to_owned(),
                            shadow_home_path: None,
                        },
                        pioneer_protocol::GatewayCliRuntimeInstanceSettings {
                            id: "codex_work".to_owned(),
                            kind: pioneer_protocol::CLIAgentRuntimeKind::Codex,
                            display_name: "Codex Work".to_owned(),
                            enabled: false,
                            binary_path: "/opt/homebrew/bin/codex".to_owned(),
                            home_path: "~/.codex-work".to_owned(),
                            shadow_home_path: Some("~/.pioneer/codex-work".to_owned()),
                        },
                    ],
                }),
                ..pioneer_protocol::GatewaySettingsUpdate::default()
            })
            .expect("CLI runtime settings should apply");
        assert!(changes.cli_runtimes);

        let applied = settings.apply_to_gateway_config(gateway_config_with_keepawake(false));
        assert!(!applied.cli_agent_runtime.enabled);
        let instances = applied.effective_cli_agent_runtime_instances();
        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].id, "codex_personal");
        assert_eq!(instances[0].display_name, "Codex Personal");
        assert_eq!(instances[1].id, "codex_work");
        assert!(!instances[1].enabled);

        let snapshot = settings.snapshot(&gateway_config_with_keepawake(false));
        assert_eq!(snapshot.cli_runtimes.instances.len(), 2);
        assert_eq!(snapshot.cli_runtimes.instances[0].id, "codex_personal");

        save_gateway_settings(&path, &settings).expect("settings should save");
        let content = fs::read_to_string(&path).expect("read settings");
        assert!(content.contains("[[cli_runtimes.instances]]"));
        assert!(content.contains("id = \"codex_personal\""));
        assert!(content.contains("display_name = \"Codex Work\""));
        assert!(!content.contains("startup_probe_timeout_ms"));
        assert!(!content.contains("request_timeout_ms"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn gateway_settings_cli_runtimes_empty_override_disables_global_fallback() {
        let mut settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"

[cli_runtimes]
"#,
        )
        .expect("gateway settings should parse");
        let mut config = gateway_config_with_keepawake(false);
        config.cli_agent_runtime.enabled = true;
        assert_eq!(config.effective_cli_agent_runtime_instances().len(), 2);

        let snapshot = settings.snapshot(&config);
        assert!(snapshot.cli_runtimes.instances.is_empty());
        let applied = settings.apply_to_gateway_config(config);
        assert!(applied.effective_cli_agent_runtime_instances().is_empty());

        let changes = settings
            .apply_protocol_update(pioneer_protocol::GatewaySettingsUpdate {
                cli_runtimes: Some(pioneer_protocol::GatewayCliRuntimeSettings {
                    instances: Vec::new(),
                }),
                ..pioneer_protocol::GatewaySettingsUpdate::default()
            })
            .expect("empty CLI runtime settings should apply");
        assert!(changes.cli_runtimes);
    }

    #[test]
    fn gateway_settings_without_cli_runtimes_exposes_default_cli_runtimes() {
        let settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"
"#,
        )
        .expect("gateway settings should parse");

        let config = gateway_config_with_keepawake(false);
        assert!(config.effective_cli_agent_runtime_instances().is_empty());

        let snapshot = settings.snapshot(&config);
        assert_eq!(snapshot.cli_runtimes.instances.len(), 2);
        assert_eq!(snapshot.cli_runtimes.instances[0].id, "codex");
        assert!(snapshot.cli_runtimes.instances[0].enabled);
        assert_eq!(snapshot.cli_runtimes.instances[1].id, "claude");
        assert!(snapshot.cli_runtimes.instances[1].enabled);

        let applied = settings.apply_to_gateway_config(config);
        let instances = applied.effective_cli_agent_runtime_instances();
        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].id, "codex");
        assert!(instances[0].enabled);
        assert_eq!(instances[1].id, "claude");
        assert!(instances[1].enabled);
    }

    #[test]
    fn gateway_settings_migrates_default_codex_display_name() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 1

[secrets]
backend = "keystore"

[[cli_runtimes.instances]]
id = "codex"
kind = "codex"
display_name = "Codex"
enabled = true
binary_path = "codex"
home_path = "~/.codex"

[[cli_runtimes.instances]]
id = "codex_work"
kind = "codex"
display_name = "Codex Work"
enabled = true
binary_path = "codex"
home_path = "~/.codex-work"
"#,
        )
        .expect("write settings");

        let settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should load and migrate");
        let snapshot = settings.snapshot(&gateway_config_with_keepawake(false));

        let codex = snapshot
            .cli_runtimes
            .instances
            .iter()
            .find(|instance| instance.id == "codex")
            .expect("default Codex CLI runtime should exist");
        assert_eq!(codex.display_name, "Codex CLI");
        let codex_work = snapshot
            .cli_runtimes
            .instances
            .iter()
            .find(|instance| instance.id == "codex_work")
            .expect("custom Codex CLI runtime should exist");
        assert_eq!(codex_work.display_name, "Codex Work");

        let content = fs::read_to_string(&path).expect("read migrated settings");
        assert!(content.contains("display_name = \"Codex CLI\""));
        assert!(content.contains("display_name = \"Codex Work\""));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn gateway_settings_migrates_default_claude_cli_runtime_instance() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 1

[secrets]
backend = "keystore"

[[cli_runtimes.instances]]
id = "codex"
kind = "codex"
display_name = "Codex CLI"
enabled = true
binary_path = "codex"
home_path = "~/.codex"
"#,
        )
        .expect("write settings");

        let settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should load and migrate");
        let snapshot = settings.snapshot(&gateway_config_with_keepawake(false));

        assert_eq!(snapshot.cli_runtimes.instances.len(), 2);
        assert!(
            snapshot
                .cli_runtimes
                .instances
                .iter()
                .any(|instance| instance.id == "codex")
        );
        let claude = snapshot
            .cli_runtimes
            .instances
            .iter()
            .find(|instance| instance.id == "claude")
            .expect("default Claude CLI runtime should be migrated");
        assert_eq!(claude.display_name, "Claude CLI");
        assert!(claude.enabled);

        let content = fs::read_to_string(&path).expect("read migrated settings");
        assert!(content.contains("id = \"codex\""));
        assert!(content.contains("id = \"claude\""));
        assert!(content.contains("display_name = \"Claude CLI\""));
        assert!(content.contains("binary_path = \"claude\""));
        assert!(content.contains("home_path = \"~/.claude\""));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn gateway_settings_does_not_add_default_claude_to_custom_cli_runtime_list() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 1

[secrets]
backend = "keystore"

[[cli_runtimes.instances]]
id = "codex_work"
kind = "codex"
display_name = "Codex Work"
enabled = true
binary_path = "codex"
home_path = "~/.codex"
"#,
        )
        .expect("write settings");

        let settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should load without default Claude migration");
        let snapshot = settings.snapshot(&gateway_config_with_keepawake(false));

        assert_eq!(snapshot.cli_runtimes.instances.len(), 1);
        assert_eq!(snapshot.cli_runtimes.instances[0].id, "codex_work");
        assert!(
            !snapshot
                .cli_runtimes
                .instances
                .iter()
                .any(|instance| instance.id == "claude")
        );

        let content = fs::read_to_string(&path).expect("read settings");
        assert!(!content.contains("id = \"claude\""));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn gateway_settings_cli_runtimes_reject_duplicate_names_and_invalid_paths() {
        let mut settings = toml::from_str::<super::GatewaySettings>(
            r#"
version = 1

[secrets]
backend = "keystore"
"#,
        )
        .expect("gateway settings should parse");

        let duplicate = settings
            .apply_protocol_update(pioneer_protocol::GatewaySettingsUpdate {
                cli_runtimes: Some(pioneer_protocol::GatewayCliRuntimeSettings {
                    instances: vec![
                        pioneer_protocol::GatewayCliRuntimeInstanceSettings {
                            id: "codex_one".to_owned(),
                            kind: pioneer_protocol::CLIAgentRuntimeKind::Codex,
                            display_name: "Codex CLI".to_owned(),
                            enabled: true,
                            binary_path: "codex".to_owned(),
                            home_path: "~/.codex".to_owned(),
                            shadow_home_path: None,
                        },
                        pioneer_protocol::GatewayCliRuntimeInstanceSettings {
                            id: "codex_two".to_owned(),
                            kind: pioneer_protocol::CLIAgentRuntimeKind::Codex,
                            display_name: "codex cli".to_owned(),
                            enabled: true,
                            binary_path: "codex".to_owned(),
                            home_path: "~/.codex-two".to_owned(),
                            shadow_home_path: None,
                        },
                    ],
                }),
                ..pioneer_protocol::GatewaySettingsUpdate::default()
            })
            .expect_err("duplicate display names should be rejected");
        assert!(format!("{duplicate:#}").contains("duplicate CLI runtime display name"));

        let invalid_path = settings
            .apply_protocol_update(pioneer_protocol::GatewaySettingsUpdate {
                cli_runtimes: Some(pioneer_protocol::GatewayCliRuntimeSettings {
                    instances: vec![pioneer_protocol::GatewayCliRuntimeInstanceSettings {
                        id: "codex_bad".to_owned(),
                        kind: pioneer_protocol::CLIAgentRuntimeKind::Codex,
                        display_name: "Codex Bad".to_owned(),
                        enabled: true,
                        binary_path: "codex\nbad".to_owned(),
                        home_path: "~/.codex".to_owned(),
                        shadow_home_path: None,
                    }],
                }),
                ..pioneer_protocol::GatewaySettingsUpdate::default()
            })
            .expect_err("invalid path should be rejected");
        assert!(format!("{invalid_path:#}").contains("binary_path"));
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
            cli_agent_runtime: GatewayCliAgentRuntimeConfig::default(),
            cli_agent_runtimes: GatewayCliAgentRuntimeInstancesConfig::default(),
            remote_access: Default::default(),
            voice: Default::default(),
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
            thread_episodic: GatewayThreadEpisodicConfig::default(),
            hooks: Default::default(),
            artifacts: GatewayArtifactsConfig::default(),
            resilience: Default::default(),
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
