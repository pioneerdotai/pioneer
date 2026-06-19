use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::turn::CLIAgentRuntimeKind;

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsGetParams {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsGetResponse {
    pub settings: GatewaySettingsSnapshot,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub general: Option<GatewayGeneralSettingsUpdate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<GatewayMemorySettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_episodic: Option<GatewayThreadEpisodicSettingsUpdate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_runtimes: Option<GatewayCliRuntimeSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_access: Option<GatewayRemoteAccessSettingsUpdate>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayGeneralSettingsUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepawake: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preflight_model: Option<GatewayMemoryModelSelection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsUpdateParams {
    pub update: GatewaySettingsUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsUpdateResponse {
    pub settings: GatewaySettingsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GatewaySettingsSnapshot {
    #[serde(default)]
    pub general: GatewayGeneralSettings,
    pub memory: GatewayMemorySettings,
    #[serde(default)]
    pub thread_episodic: GatewayThreadEpisodicSettings,
    #[serde(default)]
    pub cli_runtimes: GatewayCliRuntimeSettings,
    #[serde(default)]
    pub remote_access: GatewayRemoteAccessSettings,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayGeneralSettings {
    #[serde(default)]
    pub keepawake: bool,
    #[serde(default)]
    pub preflight_model: GatewayMemoryModelSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayMemorySettings {
    pub enabled: bool,
    pub deterministic_recall_enabled: bool,
    pub active_recall_enabled: bool,
    pub tools_enabled: bool,
    pub proactive_writes_enabled: bool,
    pub background_extraction_enabled: bool,
    #[serde(default)]
    pub proactive_writes_model: GatewayMemoryModelSelection,
    pub debug_trace_enabled: bool,
    pub strict_diagnostics_enabled: bool,
}

impl Default for GatewayMemorySettings {
    fn default() -> Self {
        Self {
            enabled: true,
            deterministic_recall_enabled: true,
            active_recall_enabled: true,
            tools_enabled: true,
            proactive_writes_enabled: true,
            background_extraction_enabled: true,
            proactive_writes_model: GatewayMemoryModelSelection::thread(),
            debug_trace_enabled: false,
            strict_diagnostics_enabled: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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
    pub chunk_target_min_chars: u32,
    pub chunk_target_max_chars: u32,
    pub chunk_max_chars: u32,
    pub max_chunks_per_item: u32,
    pub index_batch_limit: u32,
    pub retry_base_delay_secs: i64,
    pub retry_max_delay_secs: i64,
    pub max_attempts: i64,
    pub near_capacity_percent: f64,
}

impl Default for GatewayThreadEpisodicSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            indexing_enabled: true,
            recall_enabled: true,
            default_prompt_chars: 2_400,
            max_prompt_chars: 12_000,
            max_hit_chars: 1_200,
            default_max_candidates: 32,
            max_candidate_work: 128,
            max_segments: 16,
            min_relevancy: 0.25,
            min_results: 1,
            snippet_chars: 360,
            chunk_target_min_chars: 700,
            chunk_target_max_chars: 1_200,
            chunk_max_chars: 1_600,
            max_chunks_per_item: 64,
            index_batch_limit: 16,
            retry_base_delay_secs: 30,
            retry_max_delay_secs: 900,
            max_attempts: 5,
            near_capacity_percent: 90.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayCliRuntimeSettings {
    #[serde(default)]
    pub instances: Vec<GatewayCliRuntimeInstanceSettings>,
}

impl Default for GatewayCliRuntimeSettings {
    fn default() -> Self {
        Self {
            instances: vec![GatewayCliRuntimeInstanceSettings::default_codex()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayCliRuntimeInstanceSettings {
    pub id: String,
    pub kind: CLIAgentRuntimeKind,
    pub display_name: String,
    pub enabled: bool,
    pub binary_path: String,
    pub home_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_home_path: Option<String>,
}

impl GatewayCliRuntimeInstanceSettings {
    pub fn default_codex() -> Self {
        Self {
            id: "codex".to_owned(),
            kind: CLIAgentRuntimeKind::Codex,
            display_name: "Codex CLI".to_owned(),
            enabled: true,
            binary_path: "codex".to_owned(),
            home_path: "~/.codex".to_owned(),
            shadow_home_path: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayRemoteAccessSettings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(default)]
    pub transport: GatewayRemoteAccessTransport,
    #[serde(default)]
    pub has_key: bool,
    #[serde(default)]
    pub status: GatewayRemoteAccessStatusSnapshot,
}

impl Default for GatewayRemoteAccessSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            server: None,
            public_address: None,
            service_name: None,
            transport: GatewayRemoteAccessTransport::default(),
            has_key: false,
            status: GatewayRemoteAccessStatusSnapshot::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayRemoteAccessTransport {
    Tcp,
    Tls,
    Noise,
    Websocket,
}

impl Default for GatewayRemoteAccessTransport {
    fn default() -> Self {
        Self::Tcp
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayRemoteAccessStatusSnapshot {
    #[serde(default)]
    pub state: GatewayRemoteAccessState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<GatewayRemoteAccessErrorKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayRemoteAccessStatusChangedNotification {
    pub status: GatewayRemoteAccessStatusSnapshot,
}

impl Default for GatewayRemoteAccessStatusSnapshot {
    fn default() -> Self {
        Self {
            state: GatewayRemoteAccessState::Disabled,
            error_kind: None,
            message: None,
            updated_at_unix: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayRemoteAccessState {
    Disabled,
    Starting,
    Connected,
    Reconnecting,
    Failed,
    Stopped,
}

impl Default for GatewayRemoteAccessState {
    fn default() -> Self {
        Self::Disabled
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayRemoteAccessErrorKind {
    InvalidSettings,
    MissingKey,
    MissingBinary,
    LocalGatewayUnavailable,
    RelayResolveFailed,
    RelayConnectFailed,
    TunnelAuthFailed,
    ProcessExited,
    UnsupportedTransport,
    RestartLimitReached,
    Io,
    Unknown,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayRemoteAccessSettingsUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clear_key: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayThreadEpisodicSettingsUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexing_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recall_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_prompt_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_prompt_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_hit_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_max_candidates: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_candidate_work: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_segments: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_relevancy: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_results: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_target_min_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_target_max_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_max_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chunks_per_item: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_batch_limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_base_delay_secs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_delay_secs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub near_capacity_percent: Option<f64>,
}

impl GatewayThreadEpisodicSettingsUpdate {
    pub fn enabled(enabled: bool) -> Self {
        Self {
            enabled: Some(enabled),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayMemoryModelSelectionSource {
    Thread,
    Custom,
}

impl Default for GatewayMemoryModelSelectionSource {
    fn default() -> Self {
        Self::Thread
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayMemoryModelSelection {
    #[serde(default)]
    pub source: GatewayMemoryModelSelectionSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

impl Default for GatewayMemoryModelSelection {
    fn default() -> Self {
        Self::thread()
    }
}

impl GatewayMemoryModelSelection {
    pub fn thread() -> Self {
        Self {
            source: GatewayMemoryModelSelectionSource::Thread,
            model_provider: None,
            model: None,
        }
    }

    pub fn custom(model_provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            source: GatewayMemoryModelSelectionSource::Custom,
            model_provider: Some(model_provider.into()),
            model: Some(model.into()),
        }
    }

    pub fn is_thread_model(&self) -> bool {
        self.source == GatewayMemoryModelSelectionSource::Thread
    }

    pub fn model_provider_override(&self) -> Option<String> {
        if self.is_thread_model() {
            return None;
        }
        let model_provider =
            normalized_optional_model_selection_text(self.model_provider.as_deref(), 80);
        let model = normalized_optional_model_selection_text(self.model.as_deref(), 160);
        model_provider.filter(|_| model.is_some())
    }

    pub fn model_override(&self) -> Option<String> {
        if self.is_thread_model() {
            return None;
        }
        let model_provider =
            normalized_optional_model_selection_text(self.model_provider.as_deref(), 80);
        let model = normalized_optional_model_selection_text(self.model.as_deref(), 160);
        model.filter(|_| model_provider.is_some())
    }
}

fn normalized_optional_model_selection_text(value: Option<&str>, max_len: usize) -> Option<String> {
    let normalized = value?.trim();
    if normalized.is_empty() {
        return None;
    }

    Some(normalized.chars().take(max_len).collect())
}

#[cfg(test)]
mod tests {
    use super::{
        GatewayCliRuntimeInstanceSettings, GatewayCliRuntimeSettings, GatewayGeneralSettings,
        GatewayGeneralSettingsUpdate, GatewayMemoryModelSelection, GatewaySettingsSnapshot,
        GatewaySettingsUpdate, GatewayThreadEpisodicSettings, GatewayThreadEpisodicSettingsUpdate,
    };
    use crate::turn::CLIAgentRuntimeKind;

    #[test]
    fn settings_general_defaults_to_thread_preflight_model() {
        let settings = GatewayGeneralSettings::default();

        assert!(!settings.keepawake);
        assert!(settings.preflight_model.is_thread_model());
    }

    #[test]
    fn cli_runtime_settings_default_to_enabled_codex() {
        let settings = GatewayCliRuntimeSettings::default();

        assert_eq!(settings.instances.len(), 1);
        assert_eq!(settings.instances[0].id, "codex");
        assert_eq!(settings.instances[0].kind, CLIAgentRuntimeKind::Codex);
        assert_eq!(settings.instances[0].display_name, "Codex CLI");
        assert!(settings.instances[0].enabled);
        assert_eq!(settings.instances[0].binary_path, "codex");
        assert_eq!(settings.instances[0].home_path, "~/.codex");
        assert!(settings.instances[0].shadow_home_path.is_none());
    }

    #[test]
    fn settings_general_update_roundtrips_preflight_model() {
        let update = GatewayGeneralSettingsUpdate {
            keepawake: Some(true),
            preflight_model: Some(GatewayMemoryModelSelection::custom(
                "planner-provider",
                "planner-model",
            )),
        };

        let serialized = serde_json::to_string(&update).expect("settings update should serialize");
        assert!(serialized.contains("preflight_model"));
        assert!(serialized.contains("planner-provider"));
        assert!(serialized.contains("planner-model"));

        let roundtrip: GatewayGeneralSettingsUpdate =
            serde_json::from_str(serialized.as_str()).expect("settings update should deserialize");
        assert_eq!(roundtrip, update);
    }

    #[test]
    fn settings_snapshot_roundtrips_thread_episodic_settings() {
        let snapshot = GatewaySettingsSnapshot {
            general: GatewayGeneralSettings::default(),
            memory: Default::default(),
            thread_episodic: GatewayThreadEpisodicSettings {
                enabled: true,
                indexing_enabled: false,
                recall_enabled: true,
                default_prompt_chars: 1_000,
                max_prompt_chars: 8_000,
                max_hit_chars: 800,
                default_max_candidates: 24,
                max_candidate_work: 96,
                max_segments: 8,
                min_relevancy: 0.3,
                min_results: 2,
                snippet_chars: 280,
                chunk_target_min_chars: 500,
                chunk_target_max_chars: 900,
                chunk_max_chars: 1_200,
                max_chunks_per_item: 32,
                index_batch_limit: 8,
                retry_base_delay_secs: 10,
                retry_max_delay_secs: 300,
                max_attempts: 3,
                near_capacity_percent: 85.0,
            },
            cli_runtimes: GatewayCliRuntimeSettings {
                instances: vec![GatewayCliRuntimeInstanceSettings {
                    id: "codex_work".to_owned(),
                    kind: CLIAgentRuntimeKind::Codex,
                    display_name: "Codex Work".to_owned(),
                    enabled: true,
                    binary_path: "codex".to_owned(),
                    home_path: "~/.codex".to_owned(),
                    shadow_home_path: Some("~/.pioneer/codex-work".to_owned()),
                }],
            },
            remote_access: Default::default(),
        };

        let serialized = serde_json::to_string(&snapshot).expect("snapshot should serialize");
        assert!(serialized.contains("thread_episodic"));
        assert!(serialized.contains("indexing_enabled"));
        assert!(serialized.contains("cli_runtimes"));
        assert!(serialized.contains("codex_work"));

        let roundtrip: GatewaySettingsSnapshot =
            serde_json::from_str(serialized.as_str()).expect("snapshot should deserialize");
        assert_eq!(roundtrip, snapshot);
    }

    #[test]
    fn settings_update_roundtrips_partial_thread_episodic_settings() {
        let update = GatewaySettingsUpdate {
            thread_episodic: Some(GatewayThreadEpisodicSettingsUpdate {
                enabled: Some(false),
                recall_enabled: Some(false),
                ..GatewayThreadEpisodicSettingsUpdate::default()
            }),
            cli_runtimes: Some(GatewayCliRuntimeSettings {
                instances: vec![GatewayCliRuntimeInstanceSettings {
                    id: "codex_personal".to_owned(),
                    kind: CLIAgentRuntimeKind::Codex,
                    display_name: "Codex Personal".to_owned(),
                    enabled: false,
                    binary_path: "/opt/homebrew/bin/codex".to_owned(),
                    home_path: "~/.codex-personal".to_owned(),
                    shadow_home_path: None,
                }],
            }),
            ..GatewaySettingsUpdate::default()
        };

        let serialized = serde_json::to_string(&update).expect("settings update should serialize");
        assert!(serialized.contains("thread_episodic"));
        assert!(serialized.contains("cli_runtimes"));

        let roundtrip: GatewaySettingsUpdate =
            serde_json::from_str(serialized.as_str()).expect("settings update should deserialize");
        assert_eq!(roundtrip.thread_episodic, update.thread_episodic);
        assert_eq!(roundtrip.cli_runtimes, update.cli_runtimes);
    }
}
