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
    pub index_batch_limit: u32,
    pub retry_base_delay_secs: i64,
    pub retry_max_delay_secs: i64,
    pub max_attempts: i64,
    pub near_capacity_percent: f64,
    #[serde(default)]
    pub vector_search: GatewayThreadEpisodicVectorSearchSettings,
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
            index_batch_limit: 16,
            retry_base_delay_secs: 30,
            retry_max_delay_secs: 900,
            max_attempts: 5,
            near_capacity_percent: 85.0,
            vector_search: GatewayThreadEpisodicVectorSearchSettings::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayThreadEpisodicVectorSearchSettings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<GatewayThreadEpisodicVectorProvider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_dimension: Option<u32>,
    #[serde(default = "default_gateway_thread_episodic_vector_normalized")]
    pub embedding_normalized: bool,
    #[serde(default)]
    pub provider_key: GatewayThreadEpisodicVectorProviderKeyStatus,
    #[serde(default)]
    pub refill_status: GatewayThreadEpisodicVectorRefillStatus,
    #[serde(default)]
    pub local_model_status: GatewayThreadEpisodicVectorLocalModelStatus,
}

impl Default for GatewayThreadEpisodicVectorSearchSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: None,
            model: None,
            local_model: None,
            embedding_dimension: None,
            embedding_normalized: default_gateway_thread_episodic_vector_normalized(),
            provider_key: GatewayThreadEpisodicVectorProviderKeyStatus::default(),
            refill_status: GatewayThreadEpisodicVectorRefillStatus::Disabled,
            local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::NotSelected,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GatewayThreadEpisodicVectorProvider {
    #[serde(rename = "openai")]
    OpenAi,
    #[serde(rename = "openrouter")]
    OpenRouter,
    #[serde(rename = "local")]
    Local,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayThreadEpisodicVectorProviderKeyStatus {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub present: bool,
}

impl Default for GatewayThreadEpisodicVectorProviderKeyStatus {
    fn default() -> Self {
        Self {
            required: false,
            present: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayThreadEpisodicVectorRefillStatus {
    Disabled,
    Unknown,
    Required,
    Running,
    Complete,
    Failed,
}

impl Default for GatewayThreadEpisodicVectorRefillStatus {
    fn default() -> Self {
        Self::Disabled
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayThreadEpisodicVectorRefillStatusChangedNotification {
    pub workspace_id: String,
    pub status: GatewayThreadEpisodicVectorRefillStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayThreadEpisodicVectorLocalModelStatus {
    NotSelected,
    Unknown,
    Missing,
    Downloading,
    Installed,
    Failed,
}

impl Default for GatewayThreadEpisodicVectorLocalModelStatus {
    fn default() -> Self {
        Self::NotSelected
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayThreadEpisodicVectorSearchSettingsUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<Option<GatewayThreadEpisodicVectorProvider>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_model: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_normalized: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayCliRuntimeSettings {
    #[serde(default)]
    pub instances: Vec<GatewayCliRuntimeInstanceSettings>,
}

impl Default for GatewayCliRuntimeSettings {
    fn default() -> Self {
        Self {
            instances: vec![
                GatewayCliRuntimeInstanceSettings::default_codex(),
                GatewayCliRuntimeInstanceSettings::default_claude(),
            ],
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

    pub fn default_claude() -> Self {
        Self {
            id: "claude".to_owned(),
            kind: CLIAgentRuntimeKind::Claude,
            display_name: "Claude CLI".to_owned(),
            enabled: true,
            binary_path: "claude".to_owned(),
            home_path: "~/.claude".to_owned(),
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
    pub index_batch_limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_base_delay_secs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_delay_secs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub near_capacity_percent: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_search: Option<GatewayThreadEpisodicVectorSearchSettingsUpdate>,
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

const fn default_gateway_thread_episodic_vector_normalized() -> bool {
    true
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
        GatewayGeneralSettingsUpdate, GatewayMemoryModelSelection, GatewayMemorySettings,
        GatewayRemoteAccessSettings, GatewaySettingsSnapshot, GatewaySettingsUpdate,
        GatewayThreadEpisodicSettings, GatewayThreadEpisodicSettingsUpdate,
        GatewayThreadEpisodicVectorLocalModelStatus, GatewayThreadEpisodicVectorProvider,
        GatewayThreadEpisodicVectorProviderKeyStatus, GatewayThreadEpisodicVectorRefillStatus,
        GatewayThreadEpisodicVectorSearchSettings, GatewayThreadEpisodicVectorSearchSettingsUpdate,
    };
    use crate::turn::CLIAgentRuntimeKind;

    #[test]
    fn settings_general_defaults_to_thread_preflight_model() {
        let settings = GatewayGeneralSettings::default();

        assert!(!settings.keepawake);
        assert!(settings.preflight_model.is_thread_model());
    }

    #[test]
    fn cli_runtime_settings_default_to_enabled_cli_runtimes() {
        let settings = GatewayCliRuntimeSettings::default();

        assert_eq!(settings.instances.len(), 2);
        assert_eq!(settings.instances[0].id, "codex");
        assert_eq!(settings.instances[0].kind, CLIAgentRuntimeKind::Codex);
        assert_eq!(settings.instances[0].display_name, "Codex CLI");
        assert!(settings.instances[0].enabled);
        assert_eq!(settings.instances[0].binary_path, "codex");
        assert_eq!(settings.instances[0].home_path, "~/.codex");
        assert!(settings.instances[0].shadow_home_path.is_none());
        assert_eq!(settings.instances[1].id, "claude");
        assert_eq!(settings.instances[1].kind, CLIAgentRuntimeKind::Claude);
        assert_eq!(settings.instances[1].display_name, "Claude CLI");
        assert!(settings.instances[1].enabled);
        assert_eq!(settings.instances[1].binary_path, "claude");
        assert_eq!(settings.instances[1].home_path, "~/.claude");
        assert!(settings.instances[1].shadow_home_path.is_none());
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
                index_batch_limit: 8,
                retry_base_delay_secs: 10,
                retry_max_delay_secs: 300,
                max_attempts: 3,
                near_capacity_percent: 85.0,
                vector_search: GatewayThreadEpisodicVectorSearchSettings::default(),
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

    #[test]
    fn vector_search_settings_roundtrip_disabled_default() {
        let settings = GatewayThreadEpisodicVectorSearchSettings::default();

        let serialized =
            serde_json::to_string(&settings).expect("vector settings should serialize");
        assert!(serialized.contains("\"enabled\":false"));
        assert!(!serialized.contains("api_key"));
        assert!(!serialized.contains("secret"));

        let roundtrip: GatewayThreadEpisodicVectorSearchSettings =
            serde_json::from_str(serialized.as_str()).expect("vector settings should deserialize");
        assert_eq!(roundtrip, settings);
        assert_eq!(roundtrip.provider, None);
        assert_eq!(roundtrip.model, None);
        assert_eq!(roundtrip.local_model, None);
        assert_eq!(
            roundtrip.refill_status,
            GatewayThreadEpisodicVectorRefillStatus::Disabled
        );
        assert_eq!(
            roundtrip.local_model_status,
            GatewayThreadEpisodicVectorLocalModelStatus::NotSelected
        );
    }

    #[test]
    fn vector_search_settings_roundtrip_api_provider_states() {
        for (provider, model) in [
            (
                GatewayThreadEpisodicVectorProvider::OpenAi,
                "text-embedding-3-small",
            ),
            (
                GatewayThreadEpisodicVectorProvider::OpenRouter,
                "openai/text-embedding-3-small",
            ),
        ] {
            let settings = GatewayThreadEpisodicVectorSearchSettings {
                enabled: true,
                provider: Some(provider),
                model: Some(model.to_owned()),
                local_model: Some("bge-small-en-v1.5".to_owned()),
                embedding_dimension: Some(1536),
                embedding_normalized: true,
                provider_key: GatewayThreadEpisodicVectorProviderKeyStatus {
                    required: true,
                    present: true,
                },
                refill_status: GatewayThreadEpisodicVectorRefillStatus::Complete,
                local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::NotSelected,
            };

            let serialized =
                serde_json::to_string(&settings).expect("vector settings should serialize");
            assert!(serialized.contains("\"provider_key\""));
            assert!(!serialized.contains("api_key"));
            assert!(!serialized.contains("sk-"));

            let roundtrip: GatewayThreadEpisodicVectorSearchSettings =
                serde_json::from_str(serialized.as_str())
                    .expect("vector settings should deserialize");
            assert_eq!(roundtrip, settings);
        }
    }

    #[test]
    fn gateway_settings_snapshot_reports_vector_key_presence_without_secret_values() {
        let snapshot = GatewaySettingsSnapshot {
            general: GatewayGeneralSettings::default(),
            memory: GatewayMemorySettings::default(),
            thread_episodic: GatewayThreadEpisodicSettings {
                vector_search: GatewayThreadEpisodicVectorSearchSettings {
                    enabled: true,
                    provider: Some(GatewayThreadEpisodicVectorProvider::OpenRouter),
                    model: Some("openai/text-embedding-3-small".to_owned()),
                    local_model: Some("bge-small-en-v1.5".to_owned()),
                    embedding_dimension: Some(1536),
                    embedding_normalized: true,
                    provider_key: GatewayThreadEpisodicVectorProviderKeyStatus {
                        required: true,
                        present: true,
                    },
                    refill_status: GatewayThreadEpisodicVectorRefillStatus::Complete,
                    local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::NotSelected,
                },
                ..GatewayThreadEpisodicSettings::default()
            },
            cli_runtimes: GatewayCliRuntimeSettings::default(),
            remote_access: GatewayRemoteAccessSettings::default(),
        };

        let serialized =
            serde_json::to_string(&snapshot).expect("settings snapshot should serialize");
        assert!(serialized.contains("\"provider_key\""));
        assert!(serialized.contains("\"present\":true"));
        assert!(!serialized.contains("api_key"));
        assert!(!serialized.contains("secret"));
        assert!(!serialized.contains("sk-"));
    }

    #[test]
    fn vector_search_settings_roundtrip_local_state() {
        let settings = GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProvider::Local),
            model: Some("bge-base-en-v1.5".to_owned()),
            local_model: Some("bge-base-en-v1.5".to_owned()),
            embedding_dimension: Some(768),
            embedding_normalized: true,
            provider_key: GatewayThreadEpisodicVectorProviderKeyStatus {
                required: false,
                present: false,
            },
            refill_status: GatewayThreadEpisodicVectorRefillStatus::Required,
            local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::Missing,
        };

        let serialized = serde_json::to_string(&settings).expect("local vector settings serialize");
        assert!(serialized.contains("\"provider\":\"local\""));
        assert!(!serialized.contains("api_key"));

        let roundtrip: GatewayThreadEpisodicVectorSearchSettings =
            serde_json::from_str(serialized.as_str()).expect("local vector settings deserialize");
        assert_eq!(roundtrip, settings);
    }

    #[test]
    fn vector_search_update_roundtrips_without_secret_values() {
        let update = GatewaySettingsUpdate {
            thread_episodic: Some(GatewayThreadEpisodicSettingsUpdate {
                vector_search: Some(GatewayThreadEpisodicVectorSearchSettingsUpdate {
                    enabled: Some(true),
                    provider: Some(Some(GatewayThreadEpisodicVectorProvider::OpenRouter)),
                    model: Some(Some("openai/text-embedding-3-large".to_owned())),
                    local_model: Some(Some("bge-small-en-v1.5".to_owned())),
                    embedding_normalized: Some(true),
                }),
                ..GatewayThreadEpisodicSettingsUpdate::default()
            }),
            ..GatewaySettingsUpdate::default()
        };

        let serialized = serde_json::to_string(&update).expect("settings update serializes");
        assert!(serialized.contains("\"vector_search\""));
        assert!(serialized.contains("\"openrouter\""));
        assert!(!serialized.contains("api_key"));
        assert!(!serialized.contains("secret"));
        assert!(!serialized.contains("sk-"));

        let roundtrip: GatewaySettingsUpdate =
            serde_json::from_str(serialized.as_str()).expect("settings update deserializes");
        assert_eq!(roundtrip.thread_episodic, update.thread_episodic);
    }
}
