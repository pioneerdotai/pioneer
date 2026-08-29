use anyhow::{Context, Result, bail};
use config::{Config, ConfigError, File, FileFormat};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};

const DEFAULT_CONFIG_TOML: &str = include_str!("../../../config/default.toml");

// Bootstrap constant: this file may override AppConfig values, so we need its name
// before AppConfig is available.
const USER_CONFIG_FILE_NAME: &str = "config.toml";

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub home_directory: String,
    pub install_state_file_name: String,
    pub install: InstallConfig,
    pub gateway: GatewayConfig,
    pub desktop: DesktopConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstallConfig {
    pub unix_root_directory_name: String,
    pub macos_root_directory_name: String,
    pub windows_root_directory_name: String,
    pub managed_directory_name: String,
    pub binary_name: String,
    pub command_name: String,
    pub macos_background_item_name: String,
    pub macos_associated_bundle_identifier: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    pub settings_version: u32,
    pub settings_file_name: String,
    pub service_name: String,
    #[serde(default)]
    pub legacy_service_names: Vec<String>,
    pub listen_addr: String,
    pub outbound_queue_capacity: usize,
    #[serde(default)]
    pub trusted_proxy_peers: Vec<IpAddr>,
    #[serde(default = "default_gateway_keepawake")]
    pub keepawake: bool,
    #[serde(default)]
    pub preflight_model: GatewayMemoryModelSelectionConfig,
    #[serde(default)]
    pub telemetry: GatewayTelemetryConfig,
    pub thread: GatewayThreadConfig,
    #[serde(default)]
    pub tools: GatewayToolsConfig,
    #[serde(default)]
    pub tasks: GatewayTasksConfig,
    #[serde(default)]
    pub skills: GatewaySkillsConfig,
    #[serde(default)]
    pub cli_agent_runtime: GatewayCliAgentRuntimeConfig,
    #[serde(default)]
    pub cli_agent_runtimes: GatewayCliAgentRuntimeInstancesConfig,
    #[serde(default)]
    pub remote_access: GatewayRemoteAccessConfig,
    #[serde(default)]
    pub voice: GatewayVoiceConfig,
    pub provider: GatewayProviderConfig,
    pub database: GatewayDatabaseConfig,
    #[serde(default)]
    pub memory: GatewayMemoryConfig,
    #[serde(default)]
    pub thread_episodic: GatewayThreadEpisodicConfig,
    #[serde(default)]
    pub hooks: GatewayHooksConfig,
    #[serde(default)]
    pub artifacts: GatewayArtifactsConfig,
    #[serde(default)]
    pub resilience: GatewayResilienceConfig,
    pub auth: GatewayAuthConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct GatewayVoiceConfig {
    #[serde(default = "default_gateway_voice_models_dir")]
    pub models_dir: String,
    #[serde(default)]
    pub transcription_strategy: GatewayVoiceTranscriptionStrategy,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub provider: Option<GatewayVoiceInputProviderConfig>,
    #[serde(default)]
    pub model: Option<String>,
}

impl Default for GatewayVoiceConfig {
    fn default() -> Self {
        Self {
            models_dir: default_gateway_voice_models_dir(),
            transcription_strategy: GatewayVoiceTranscriptionStrategy::default(),
            enabled: false,
            provider: None,
            model: None,
        }
    }
}

impl GatewayVoiceConfig {
    pub fn resolve_models_root(&self, runtime_home: &Path) -> Result<PathBuf> {
        let models_dir =
            normalize_runtime_dir_path(self.models_dir.as_str(), "gateway.voice.models_dir")?;
        Ok(runtime_home.join(models_dir))
    }
}

fn default_gateway_voice_models_dir() -> String {
    "models/voice".to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GatewayVoiceTranscriptionStrategy {
    #[default]
    BufferedGatewaySession,
    ExperimentalStreaming,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayVoiceInputProviderConfig {
    Local,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GatewayTasksConfig {
    #[serde(default)]
    pub review: GatewayTaskReviewConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayTaskReviewConfig {
    #[serde(default = "default_tasks_review_enabled")]
    pub enabled: bool,
    #[serde(default = "default_tasks_review_allow_task_create_review_policy")]
    pub allow_task_create_review_policy: bool,
    #[serde(default = "default_tasks_review_parent_review_for_attached_agent_tasks")]
    pub default_parent_review_for_immediate_attached_agent_tasks: bool,
    #[serde(default = "default_tasks_review_max_revision_rounds")]
    pub default_max_revision_rounds: u32,
    #[serde(default = "default_tasks_review_auto_accept_after_seconds")]
    pub auto_accept_after_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayRemoteAccessConfig {
    #[serde(default = "default_gateway_remote_access_runtime_dir")]
    pub runtime_dir: String,
    #[serde(default = "default_gateway_remote_access_relay_addr")]
    pub relay_addr: String,
    #[serde(default = "default_gateway_remote_access_local_addr")]
    pub local_addr: String,
    #[serde(default = "default_gateway_remote_access_service_name")]
    pub service_name: String,
    #[serde(default = "default_gateway_remote_access_restart_initial_ms")]
    pub restart_initial_ms: u64,
    #[serde(default = "default_gateway_remote_access_restart_max_ms")]
    pub restart_max_ms: u64,
    #[serde(default = "default_gateway_remote_access_restart_jitter_percent")]
    pub restart_jitter_percent: u8,
    #[serde(default = "default_gateway_remote_access_max_restarts")]
    pub max_restarts: u32,
}

impl Default for GatewayRemoteAccessConfig {
    fn default() -> Self {
        Self {
            runtime_dir: default_gateway_remote_access_runtime_dir(),
            relay_addr: default_gateway_remote_access_relay_addr(),
            local_addr: default_gateway_remote_access_local_addr(),
            service_name: default_gateway_remote_access_service_name(),
            restart_initial_ms: default_gateway_remote_access_restart_initial_ms(),
            restart_max_ms: default_gateway_remote_access_restart_max_ms(),
            restart_jitter_percent: default_gateway_remote_access_restart_jitter_percent(),
            max_restarts: default_gateway_remote_access_max_restarts(),
        }
    }
}

fn default_gateway_remote_access_runtime_dir() -> String {
    "remote-access".to_owned()
}

fn default_gateway_remote_access_relay_addr() -> String {
    "relay-eu-west-1.getpioneer.dev:2333".to_owned()
}

fn default_gateway_remote_access_local_addr() -> String {
    "127.0.0.1:17878".to_owned()
}

fn default_gateway_remote_access_service_name() -> String {
    "pioneer_gateway".to_owned()
}

const fn default_gateway_remote_access_restart_initial_ms() -> u64 {
    1_000
}

const fn default_gateway_remote_access_restart_max_ms() -> u64 {
    30_000
}

const fn default_gateway_remote_access_restart_jitter_percent() -> u8 {
    20
}

const fn default_gateway_remote_access_max_restarts() -> u32 {
    0
}

impl Default for GatewayTaskReviewConfig {
    fn default() -> Self {
        Self {
            enabled: default_tasks_review_enabled(),
            allow_task_create_review_policy: default_tasks_review_allow_task_create_review_policy(),
            default_parent_review_for_immediate_attached_agent_tasks:
                default_tasks_review_parent_review_for_attached_agent_tasks(),
            default_max_revision_rounds: default_tasks_review_max_revision_rounds(),
            auto_accept_after_seconds: default_tasks_review_auto_accept_after_seconds(),
        }
    }
}

const fn default_tasks_review_enabled() -> bool {
    true
}

const fn default_tasks_review_allow_task_create_review_policy() -> bool {
    false
}

const fn default_tasks_review_parent_review_for_attached_agent_tasks() -> bool {
    true
}

const fn default_tasks_review_max_revision_rounds() -> u32 {
    5
}

const fn default_tasks_review_auto_accept_after_seconds() -> u64 {
    300
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GatewayArtifactsConfig {
    #[serde(default = "default_gateway_artifacts_max_file_bytes")]
    pub max_file_bytes: u64,
    #[serde(default = "default_gateway_artifacts_max_workspace_bytes")]
    pub max_workspace_bytes: u64,
    #[serde(default = "default_gateway_artifacts_max_files_per_workspace")]
    pub max_files_per_workspace: u64,
    #[serde(default = "default_gateway_artifacts_upload_session_ttl_secs")]
    pub upload_session_ttl_secs: u64,
    #[serde(default = "default_gateway_artifacts_gc_grace_secs")]
    pub gc_grace_secs: u64,
    #[serde(default = "default_gateway_artifacts_output_dir_ttl_secs")]
    pub output_dir_ttl_secs: u64,
    #[serde(default = "default_gateway_artifacts_readable_copy_ttl_secs")]
    pub readable_copy_ttl_secs: u64,
    #[serde(default = "default_gateway_artifacts_quota_warn_at_percent")]
    pub quota_warn_at_percent: u8,
    #[serde(default = "default_gateway_artifacts_http_streams_global")]
    pub http_streams_global: usize,
    #[serde(default = "default_gateway_artifacts_http_streams_per_principal")]
    pub http_streams_per_principal: usize,
    #[serde(default = "default_gateway_artifacts_http_streams_per_session")]
    pub http_streams_per_session: usize,
    #[serde(default = "default_gateway_artifacts_http_open_handles")]
    pub http_open_handles: usize,
    #[serde(default = "default_gateway_artifacts_http_max_single_range_bytes")]
    pub http_max_single_range_bytes: u64,
    #[serde(default = "default_gateway_artifacts_http_tiny_range_bytes")]
    pub http_tiny_range_bytes: u64,
    #[serde(default = "default_gateway_artifacts_http_tiny_range_window_secs")]
    pub http_tiny_range_window_secs: u64,
    #[serde(default = "default_gateway_artifacts_http_tiny_range_max_requests")]
    pub http_tiny_range_max_requests: usize,
    #[serde(default = "default_gateway_artifacts_http_open_timeout_secs")]
    pub http_open_timeout_secs: u64,
    #[serde(default = "default_gateway_artifacts_http_body_idle_timeout_secs")]
    pub http_body_idle_timeout_secs: u64,
    #[serde(default = "default_gateway_artifacts_view_grant_ttl_secs")]
    pub view_grant_ttl_secs: u64,
    #[serde(default = "default_gateway_artifacts_view_grants_global")]
    pub view_grants_global: usize,
    #[serde(default = "default_gateway_artifacts_view_grants_per_session")]
    pub view_grants_per_session: usize,
    #[serde(default = "default_gateway_artifacts_view_grant_streams")]
    pub view_grant_streams: usize,
}

impl Default for GatewayArtifactsConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: default_gateway_artifacts_max_file_bytes(),
            max_workspace_bytes: default_gateway_artifacts_max_workspace_bytes(),
            max_files_per_workspace: default_gateway_artifacts_max_files_per_workspace(),
            upload_session_ttl_secs: default_gateway_artifacts_upload_session_ttl_secs(),
            gc_grace_secs: default_gateway_artifacts_gc_grace_secs(),
            output_dir_ttl_secs: default_gateway_artifacts_output_dir_ttl_secs(),
            readable_copy_ttl_secs: default_gateway_artifacts_readable_copy_ttl_secs(),
            quota_warn_at_percent: default_gateway_artifacts_quota_warn_at_percent(),
            http_streams_global: default_gateway_artifacts_http_streams_global(),
            http_streams_per_principal: default_gateway_artifacts_http_streams_per_principal(),
            http_streams_per_session: default_gateway_artifacts_http_streams_per_session(),
            http_open_handles: default_gateway_artifacts_http_open_handles(),
            http_max_single_range_bytes: default_gateway_artifacts_http_max_single_range_bytes(),
            http_tiny_range_bytes: default_gateway_artifacts_http_tiny_range_bytes(),
            http_tiny_range_window_secs: default_gateway_artifacts_http_tiny_range_window_secs(),
            http_tiny_range_max_requests: default_gateway_artifacts_http_tiny_range_max_requests(),
            http_open_timeout_secs: default_gateway_artifacts_http_open_timeout_secs(),
            http_body_idle_timeout_secs: default_gateway_artifacts_http_body_idle_timeout_secs(),
            view_grant_ttl_secs: default_gateway_artifacts_view_grant_ttl_secs(),
            view_grants_global: default_gateway_artifacts_view_grants_global(),
            view_grants_per_session: default_gateway_artifacts_view_grants_per_session(),
            view_grant_streams: default_gateway_artifacts_view_grant_streams(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct GatewayResilienceConfig {
    #[serde(default)]
    pub command_execution: GatewayCommandExecutionTimeoutConfig,
    #[serde(default)]
    pub provider_stream_items: GatewayProviderStreamItemTimeoutConfig,
    #[serde(default)]
    pub context_compaction: GatewayContextCompactionTimeoutConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GatewayContextCompactionTimeoutConfig {
    pub lease_secs: u64,
    pub idle_secs: u64,
    pub hard_secs: u64,
    pub recovery_grace_secs: u64,
}

impl Default for GatewayContextCompactionTimeoutConfig {
    fn default() -> Self {
        Self {
            lease_secs: default_context_compaction_lease_timeout_secs(),
            idle_secs: default_context_compaction_idle_timeout_secs(),
            hard_secs: default_context_compaction_hard_timeout_secs(),
            recovery_grace_secs: default_context_compaction_recovery_grace_secs(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct GatewayContextCompactionTimeoutConfigWire {
    #[serde(default = "default_context_compaction_lease_timeout_secs")]
    lease_secs: u64,
    #[serde(default = "default_context_compaction_idle_timeout_secs")]
    idle_secs: u64,
    #[serde(default = "default_context_compaction_hard_timeout_secs")]
    hard_secs: u64,
    #[serde(default = "default_context_compaction_recovery_grace_secs")]
    recovery_grace_secs: u64,
}

impl<'de> Deserialize<'de> for GatewayContextCompactionTimeoutConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GatewayContextCompactionTimeoutConfigWire::deserialize(deserializer)?;
        if wire.lease_secs == 0
            || wire.idle_secs == 0
            || wire.hard_secs == 0
            || wire.recovery_grace_secs == 0
        {
            return Err(serde::de::Error::custom(
                "context compaction timeout values must be greater than zero",
            ));
        }
        if wire.lease_secs > wire.hard_secs || wire.idle_secs > wire.hard_secs {
            return Err(serde::de::Error::custom(
                "context compaction lease and idle deadlines must not exceed the hard deadline",
            ));
        }
        if wire.recovery_grace_secs > wire.hard_secs {
            return Err(serde::de::Error::custom(
                "context compaction recovery grace must not exceed the hard deadline",
            ));
        }
        Ok(Self {
            lease_secs: wire.lease_secs,
            idle_secs: wire.idle_secs,
            hard_secs: wire.hard_secs,
            recovery_grace_secs: wire.recovery_grace_secs,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GatewayProviderStreamItemTimeoutConfig {
    pub lease_secs: u64,
    pub idle_secs: u64,
    pub hard_secs: u64,
}

impl Default for GatewayProviderStreamItemTimeoutConfig {
    fn default() -> Self {
        Self {
            lease_secs: default_provider_stream_item_lease_timeout_secs(),
            idle_secs: default_provider_stream_item_idle_timeout_secs(),
            hard_secs: default_provider_stream_item_hard_timeout_secs(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct GatewayProviderStreamItemTimeoutConfigWire {
    #[serde(default = "default_provider_stream_item_lease_timeout_secs")]
    lease_secs: u64,
    #[serde(default = "default_provider_stream_item_idle_timeout_secs")]
    idle_secs: u64,
    #[serde(default = "default_provider_stream_item_hard_timeout_secs")]
    hard_secs: u64,
}

impl<'de> Deserialize<'de> for GatewayProviderStreamItemTimeoutConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GatewayProviderStreamItemTimeoutConfigWire::deserialize(deserializer)?;
        Ok(Self {
            lease_secs: non_zero_or_default(
                wire.lease_secs,
                default_provider_stream_item_lease_timeout_secs(),
            ),
            idle_secs: non_zero_or_default(
                wire.idle_secs,
                default_provider_stream_item_idle_timeout_secs(),
            ),
            hard_secs: non_zero_or_default(
                wire.hard_secs,
                default_provider_stream_item_hard_timeout_secs(),
            ),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GatewayCommandExecutionTimeoutConfig {
    pub lease_secs: u64,
    pub idle_secs: u64,
    pub hard_secs: u64,
    pub recovery_max_wall_clock_secs: u64,
}

impl Default for GatewayCommandExecutionTimeoutConfig {
    fn default() -> Self {
        Self {
            lease_secs: default_command_execution_lease_timeout_secs(),
            idle_secs: default_command_execution_idle_timeout_secs(),
            hard_secs: default_command_execution_hard_timeout_secs(),
            recovery_max_wall_clock_secs: default_command_execution_recovery_max_wall_clock_secs(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct GatewayCommandExecutionTimeoutConfigWire {
    #[serde(default = "default_command_execution_lease_timeout_secs")]
    lease_secs: u64,
    #[serde(default = "default_command_execution_idle_timeout_secs")]
    idle_secs: u64,
    #[serde(default = "default_command_execution_hard_timeout_secs")]
    hard_secs: u64,
    #[serde(default = "default_command_execution_recovery_max_wall_clock_secs")]
    recovery_max_wall_clock_secs: u64,
}

impl<'de> Deserialize<'de> for GatewayCommandExecutionTimeoutConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GatewayCommandExecutionTimeoutConfigWire::deserialize(deserializer)?;
        Ok(Self {
            lease_secs: non_zero_or_default(
                wire.lease_secs,
                default_command_execution_lease_timeout_secs(),
            ),
            idle_secs: non_zero_or_default(
                wire.idle_secs,
                default_command_execution_idle_timeout_secs(),
            ),
            hard_secs: non_zero_or_default(
                wire.hard_secs,
                default_command_execution_hard_timeout_secs(),
            ),
            recovery_max_wall_clock_secs: non_zero_or_default(
                wire.recovery_max_wall_clock_secs,
                default_command_execution_recovery_max_wall_clock_secs(),
            ),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayMemoryModelSelectionConfig {
    pub source: GatewayMemoryModelSelectionSource,
    pub model_provider: Option<String>,
    pub model: Option<String>,
}

impl Default for GatewayMemoryModelSelectionConfig {
    fn default() -> Self {
        Self {
            source: GatewayMemoryModelSelectionSource::Thread,
            model_provider: None,
            model: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum GatewayMemoryModelSelectionConfigWire {
    Shorthand(String),
    Detailed {
        #[serde(default)]
        source: GatewayMemoryModelSelectionSource,
        #[serde(default)]
        model_provider: Option<String>,
        #[serde(default)]
        model: Option<String>,
    },
}

impl GatewayMemoryModelSelectionConfig {
    pub fn thread() -> Self {
        Self::default()
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

impl<'de> Deserialize<'de> for GatewayMemoryModelSelectionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match GatewayMemoryModelSelectionConfigWire::deserialize(deserializer)? {
            GatewayMemoryModelSelectionConfigWire::Shorthand(value) => {
                match value.trim().to_ascii_lowercase().as_str() {
                    "thread" => Ok(Self::thread()),
                    "custom" => Ok(Self {
                        source: GatewayMemoryModelSelectionSource::Custom,
                        model_provider: None,
                        model: None,
                    }),
                    other => Err(serde::de::Error::custom(format!(
                        "invalid memory model selection `{other}`; expected thread|custom"
                    ))),
                }
            }
            GatewayMemoryModelSelectionConfigWire::Detailed {
                source,
                model_provider,
                model,
            } => Ok(Self {
                source,
                model_provider,
                model,
            }),
        }
    }
}

impl Serialize for GatewayMemoryModelSelectionConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.is_thread_model() {
            return serializer.serialize_str("thread");
        }

        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("source", &self.source)?;
        if let Some(model_provider) = self.model_provider.as_deref() {
            map.serialize_entry("model_provider", model_provider)?;
        }
        if let Some(model) = self.model.as_deref() {
            map.serialize_entry("model", model)?;
        }
        map.end()
    }
}

/// Value stored by Gateway Settings for one workspace.
///
/// This deliberately is not a field of [`GatewayConfig`]: Self-improvement
/// activation and model selection are workspace-scoped.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewaySelfImprovementConfig {
    pub enabled: bool,
    pub default_model: Option<GatewaySelfImprovementModelSelectionConfig>,
    pub reviewer_model: Option<GatewaySelfImprovementModelSelectionConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatewaySelfImprovementModelSelectionConfig {
    pub provider: String,
    pub model: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewaySelfImprovementModelSelectionConfigWire {
    provider: String,
    model: String,
}

impl<'de> Deserialize<'de> for GatewaySelfImprovementModelSelectionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GatewaySelfImprovementModelSelectionConfigWire::deserialize(deserializer)?;
        let provider = wire.provider.trim();
        let model = wire.model.trim();
        if provider.is_empty() || model.is_empty() {
            return Err(serde::de::Error::custom(
                "self-improvement model selection requires both provider and model",
            ));
        }
        Ok(Self {
            provider: provider.to_owned(),
            model: model.to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct GatewayMemoryConfig {
    /// Enables the durable memory product surface for the gateway.
    #[serde(default = "default_gateway_memory_enabled")]
    pub enabled: bool,
    /// Relative runtime-home directory where memvid capsules are stored.
    #[serde(default = "default_gateway_memory_capsules_dir")]
    pub capsules_dir: String,
    /// Allows user/global memories to be used when a request has no narrower workspace scope.
    #[serde(default = "default_gateway_memory_allow_global_user")]
    pub allow_global_user_by_default: bool,
    /// Allows agent/global memories to be used when explicitly enabled by config.
    #[serde(default = "default_gateway_memory_allow_global_agent")]
    pub allow_global_agent_by_default: bool,
    /// Enables deterministic pre-turn recall into the prompt context.
    #[serde(default = "default_gateway_memory_deterministic_recall_enabled")]
    pub deterministic_recall_enabled: bool,
    /// Enables active recall planning before prompt compilation.
    #[serde(default = "default_gateway_memory_active_recall_enabled")]
    pub active_recall_enabled: bool,
    /// Exposes explicit memory tools to capable model providers.
    #[serde(default = "default_gateway_memory_tools_enabled")]
    pub tools_enabled: bool,
    /// Allows post-turn extraction to propose durable facts through the quality gate.
    #[serde(default = "default_gateway_memory_proactive_writes_enabled")]
    pub proactive_writes_enabled: bool,
    /// Runs post-turn extraction as background hook work instead of blocking the user turn.
    #[serde(default = "default_gateway_memory_background_extraction_enabled")]
    pub background_extraction_enabled: bool,
    /// Provider/model used by active recall planner. Defaults to the thread model.
    #[serde(default)]
    pub active_recall_model: GatewayMemoryModelSelectionConfig,
    /// Provider/model used by proactive memory writes. Defaults to the thread model.
    #[serde(default)]
    pub proactive_writes_model: GatewayMemoryModelSelectionConfig,
    /// Enables user/developer-visible memory debug trace surfaces.
    #[serde(default)]
    pub debug_trace_enabled: bool,
    /// Enables strict developer diagnostics without changing product safety gates.
    #[serde(default)]
    pub strict_diagnostics_enabled: bool,
}

impl Default for GatewayMemoryConfig {
    fn default() -> Self {
        Self {
            enabled: default_gateway_memory_enabled(),
            capsules_dir: default_gateway_memory_capsules_dir(),
            allow_global_user_by_default: default_gateway_memory_allow_global_user(),
            allow_global_agent_by_default: default_gateway_memory_allow_global_agent(),
            deterministic_recall_enabled: default_gateway_memory_deterministic_recall_enabled(),
            active_recall_enabled: default_gateway_memory_active_recall_enabled(),
            tools_enabled: default_gateway_memory_tools_enabled(),
            proactive_writes_enabled: default_gateway_memory_proactive_writes_enabled(),
            background_extraction_enabled: default_gateway_memory_background_extraction_enabled(),
            active_recall_model: GatewayMemoryModelSelectionConfig::default(),
            proactive_writes_model: GatewayMemoryModelSelectionConfig::default(),
            debug_trace_enabled: false,
            strict_diagnostics_enabled: false,
        }
    }
}

impl GatewayMemoryConfig {
    pub fn resolve_capsules_root(&self, runtime_home: &Path) -> Result<PathBuf> {
        let capsules_dir =
            normalize_runtime_dir_path(self.capsules_dir.as_str(), "gateway.memory.capsules_dir")?;
        Ok(runtime_home.join(capsules_dir))
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct GatewayThreadEpisodicConfig {
    /// Enables the thread-local episodic context subsystem.
    #[serde(default = "default_gateway_thread_episodic_enabled")]
    pub enabled: bool,
    /// Enables indexing of committed visible thread items.
    #[serde(default = "default_gateway_thread_episodic_indexing_enabled")]
    pub indexing_enabled: bool,
    /// Enables pre-prompt current-thread recall hook contributions.
    #[serde(default = "default_gateway_thread_episodic_recall_enabled")]
    pub recall_enabled: bool,
    /// Default prompt budget used by current-thread recall.
    #[serde(default = "default_gateway_thread_episodic_prompt_chars")]
    pub default_prompt_chars: u32,
    /// Hard cap for per-turn thread context prompt budget.
    #[serde(default = "default_gateway_thread_episodic_max_prompt_chars")]
    pub max_prompt_chars: u32,
    /// Maximum chars per hydrated hit.
    #[serde(default = "default_gateway_thread_episodic_max_hit_chars")]
    pub max_hit_chars: usize,
    /// Default candidate count requested from the search backend.
    #[serde(default = "default_gateway_thread_episodic_max_candidates")]
    pub default_max_candidates: u32,
    /// Hard cap for backend candidate work.
    #[serde(default = "default_gateway_thread_episodic_max_candidate_work")]
    pub max_candidate_work: u32,
    /// Maximum current-thread segments searched per recall.
    #[serde(default = "default_gateway_thread_episodic_max_segments")]
    pub max_segments: u64,
    /// Adaptive retrieval minimum relevancy cutoff.
    #[serde(default = "default_gateway_thread_episodic_min_relevancy")]
    pub min_relevancy: f32,
    /// Adaptive retrieval minimum result count.
    #[serde(default = "default_gateway_thread_episodic_min_results")]
    pub min_results: u32,
    /// Snippet chars requested from memvid search.
    #[serde(default = "default_gateway_thread_episodic_snippet_chars")]
    pub snippet_chars: u32,
    /// Index worker batch limit.
    #[serde(default = "default_gateway_thread_episodic_index_batch_limit")]
    pub index_batch_limit: u64,
    /// Base retry delay for retryable index failures.
    #[serde(default = "default_gateway_thread_episodic_retry_base_delay_secs")]
    pub retry_base_delay_secs: i64,
    /// Max retry delay for retryable index failures.
    #[serde(default = "default_gateway_thread_episodic_retry_max_delay_secs")]
    pub retry_max_delay_secs: i64,
    /// Max retry attempts before terminal failure.
    #[serde(default = "default_gateway_thread_episodic_max_attempts")]
    pub max_attempts: i64,
    /// Segment rotation threshold based on memvid utilization stats.
    #[serde(default = "default_gateway_thread_episodic_near_capacity_percent")]
    pub near_capacity_percent: f64,
    /// Opt-in vector search projection settings for thread episodic workspace capsules.
    #[serde(default)]
    pub vector_search: GatewayThreadEpisodicVectorSearchConfig,
}

impl Default for GatewayThreadEpisodicConfig {
    fn default() -> Self {
        Self {
            enabled: default_gateway_thread_episodic_enabled(),
            indexing_enabled: default_gateway_thread_episodic_indexing_enabled(),
            recall_enabled: default_gateway_thread_episodic_recall_enabled(),
            default_prompt_chars: default_gateway_thread_episodic_prompt_chars(),
            max_prompt_chars: default_gateway_thread_episodic_max_prompt_chars(),
            max_hit_chars: default_gateway_thread_episodic_max_hit_chars(),
            default_max_candidates: default_gateway_thread_episodic_max_candidates(),
            max_candidate_work: default_gateway_thread_episodic_max_candidate_work(),
            max_segments: default_gateway_thread_episodic_max_segments(),
            min_relevancy: default_gateway_thread_episodic_min_relevancy(),
            min_results: default_gateway_thread_episodic_min_results(),
            snippet_chars: default_gateway_thread_episodic_snippet_chars(),
            index_batch_limit: default_gateway_thread_episodic_index_batch_limit(),
            retry_base_delay_secs: default_gateway_thread_episodic_retry_base_delay_secs(),
            retry_max_delay_secs: default_gateway_thread_episodic_retry_max_delay_secs(),
            max_attempts: default_gateway_thread_episodic_max_attempts(),
            near_capacity_percent: default_gateway_thread_episodic_near_capacity_percent(),
            vector_search: GatewayThreadEpisodicVectorSearchConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct GatewayThreadEpisodicVectorSearchConfig {
    /// Enables semantic/vector search for the derived thread episodic projection.
    #[serde(default)]
    pub enabled: bool,
    /// Selected embedding provider. Ignored while `enabled` is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<GatewayThreadEpisodicVectorProviderConfig>,
    /// Selected provider model id. For local provider this is still kept as the
    /// active embedding model id used in projection identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Local model choice persisted separately so switching away from Local does
    /// not lose the user's local selection. Ignored while provider is not Local.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_model: Option<String>,
    /// Whether embeddings are normalized before being written/searched.
    #[serde(default = "default_gateway_thread_episodic_vector_normalized")]
    pub embedding_normalized: bool,
    /// Adds a retrieval task instruction to query embeddings. Document/refill
    /// embeddings remain unchanged.
    #[serde(default)]
    pub use_search_instructions: bool,
}

impl Default for GatewayThreadEpisodicVectorSearchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: None,
            model: None,
            local_model: None,
            embedding_normalized: default_gateway_thread_episodic_vector_normalized(),
            use_search_instructions: false,
        }
    }
}

impl GatewayThreadEpisodicVectorSearchConfig {
    pub fn selected_embedding_model(&self) -> Option<&str> {
        let model = if self.provider == Some(GatewayThreadEpisodicVectorProviderConfig::Local) {
            self.model.as_deref().or(self.local_model.as_deref())
        } else {
            self.model.as_deref()
        }?;
        let model = model.trim();
        (!model.is_empty()).then_some(model)
    }

    pub fn has_selected_embedding_model(&self) -> bool {
        self.enabled && self.selected_embedding_model().is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum GatewayThreadEpisodicVectorProviderConfig {
    #[serde(rename = "openai")]
    OpenAi,
    #[serde(rename = "openrouter")]
    OpenRouter,
    #[serde(rename = "local")]
    Local,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GatewayHooksConfig {
    #[serde(default)]
    pub recovery: GatewayHookRecoveryConfig,
}

impl Default for GatewayHooksConfig {
    fn default() -> Self {
        Self {
            recovery: GatewayHookRecoveryConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GatewayHookRecoveryConfig {
    #[serde(default = "default_gateway_hook_recovery_enabled")]
    pub enabled: bool,
    #[serde(default = "default_gateway_hook_recovery_startup_scan")]
    pub startup_scan: bool,
    #[serde(default = "default_gateway_hook_recovery_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_gateway_hook_recovery_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_gateway_hook_recovery_max_concurrent")]
    pub max_concurrent: usize,
    #[serde(default = "default_gateway_hook_recovery_stale_running_after_ms")]
    pub stale_running_after_ms: u64,
    #[serde(default)]
    pub strict_debug: bool,
}

impl Default for GatewayHookRecoveryConfig {
    fn default() -> Self {
        Self {
            enabled: default_gateway_hook_recovery_enabled(),
            startup_scan: default_gateway_hook_recovery_startup_scan(),
            poll_interval_ms: default_gateway_hook_recovery_poll_interval_ms(),
            batch_size: default_gateway_hook_recovery_batch_size(),
            max_concurrent: default_gateway_hook_recovery_max_concurrent(),
            stale_running_after_ms: default_gateway_hook_recovery_stale_running_after_ms(),
            strict_debug: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayThreadConfig {
    pub default_model: String,
    pub default_model_provider: String,
    #[serde(default)]
    pub summary_model: Option<String>,
    #[serde(default)]
    pub summary_model_provider: Option<String>,
    #[serde(default)]
    pub title_model: Option<String>,
    #[serde(default)]
    pub title_model_provider: Option<String>,
    pub max_context_tokens: usize,
    pub response_reserve_tokens: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayProviderConfig {
    #[serde(
        default = "default_provider_non_stream_request_timeout_secs",
        alias = "default_timeout_secs"
    )]
    pub non_stream_request_timeout_secs: u64,
    #[serde(default = "default_provider_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_provider_first_chunk_timeout_secs")]
    pub first_chunk_timeout_secs: u64,
    #[serde(default = "default_provider_inter_chunk_idle_timeout_secs")]
    pub inter_chunk_idle_timeout_secs: u64,
    #[serde(default)]
    pub max_stream_duration_secs: Option<u64>,
    #[serde(default)]
    pub attachments: GatewayProviderAttachmentsConfig,
}

impl Default for GatewayProviderConfig {
    fn default() -> Self {
        Self {
            non_stream_request_timeout_secs: default_provider_non_stream_request_timeout_secs(),
            connect_timeout_secs: default_provider_connect_timeout_secs(),
            first_chunk_timeout_secs: default_provider_first_chunk_timeout_secs(),
            inter_chunk_idle_timeout_secs: default_provider_inter_chunk_idle_timeout_secs(),
            max_stream_duration_secs: None,
            attachments: GatewayProviderAttachmentsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatewayCliAgentRuntimeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub mcp_tools: GatewayCliAgentRuntimeMcpToolsConfig,
    pub idle_session_ttl_secs: u64,
    pub startup_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub event_channel_capacity: usize,
    pub stderr_ring_lines: usize,
    #[serde(default)]
    pub debug_native_events: bool,
    #[serde(default)]
    pub command_heartbeat: GatewayCliAgentRuntimeCommandHeartbeatConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GatewayCliAgentRuntimeMcpToolsConfig {
    pub max_tools: usize,
    pub max_total_schema_bytes: usize,
    pub max_concurrent_calls_per_turn: usize,
}

impl Default for GatewayCliAgentRuntimeMcpToolsConfig {
    fn default() -> Self {
        Self {
            max_tools: default_cli_agent_runtime_mcp_max_tools(),
            max_total_schema_bytes: default_cli_agent_runtime_mcp_max_total_schema_bytes(),
            max_concurrent_calls_per_turn:
                default_cli_agent_runtime_mcp_max_concurrent_calls_per_turn(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct GatewayCliAgentRuntimeMcpToolsConfigWire {
    #[serde(default = "default_cli_agent_runtime_mcp_max_tools")]
    max_tools: usize,
    #[serde(default = "default_cli_agent_runtime_mcp_max_total_schema_bytes")]
    max_total_schema_bytes: usize,
    #[serde(default = "default_cli_agent_runtime_mcp_max_concurrent_calls_per_turn")]
    max_concurrent_calls_per_turn: usize,
}

impl<'de> Deserialize<'de> for GatewayCliAgentRuntimeMcpToolsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GatewayCliAgentRuntimeMcpToolsConfigWire::deserialize(deserializer)?;
        Ok(Self {
            max_tools: non_zero_or_default_usize(
                wire.max_tools,
                default_cli_agent_runtime_mcp_max_tools(),
            ),
            max_total_schema_bytes: non_zero_or_default_usize(
                wire.max_total_schema_bytes,
                default_cli_agent_runtime_mcp_max_total_schema_bytes(),
            ),
            max_concurrent_calls_per_turn: non_zero_or_default_usize(
                wire.max_concurrent_calls_per_turn,
                default_cli_agent_runtime_mcp_max_concurrent_calls_per_turn(),
            ),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GatewayCliAgentRuntimeCommandHeartbeatConfig {
    pub interval_secs: u64,
}

impl Default for GatewayCliAgentRuntimeCommandHeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_cli_agent_runtime_command_heartbeat_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct GatewayCliAgentRuntimeCommandHeartbeatConfigWire {
    #[serde(default = "default_cli_agent_runtime_command_heartbeat_interval_secs")]
    interval_secs: u64,
}

impl<'de> Deserialize<'de> for GatewayCliAgentRuntimeCommandHeartbeatConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GatewayCliAgentRuntimeCommandHeartbeatConfigWire::deserialize(deserializer)?;
        Ok(Self {
            interval_secs: non_zero_or_default(
                wire.interval_secs,
                default_cli_agent_runtime_command_heartbeat_interval_secs(),
            ),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct GatewayCliAgentRuntimeInstancesConfig {
    pub instances: BTreeMap<String, GatewayCliAgentRuntimeInstanceConfig>,
}

impl GatewayCliAgentRuntimeInstancesConfig {
    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }

    pub fn values(&self) -> impl Iterator<Item = &GatewayCliAgentRuntimeInstanceConfig> {
        self.instances.values()
    }
}

impl<'de> Deserialize<'de> for GatewayCliAgentRuntimeInstancesConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BTreeMap::<String, GatewayCliAgentRuntimeInstanceConfigWire>::deserialize(
            deserializer,
        )?;
        let mut instances = BTreeMap::new();
        for (raw_id, wire) in wire {
            let id = normalize_cli_agent_runtime_instance_id(raw_id.as_str())
                .map_err(serde::de::Error::custom)?;
            if instances.contains_key(id.as_str()) {
                return Err(serde::de::Error::custom(format!(
                    "duplicate CLI agent runtime instance id `{id}` after normalization"
                )));
            }
            instances.insert(
                id.clone(),
                GatewayCliAgentRuntimeInstanceConfig::from_wire(id, wire),
            );
        }
        Ok(Self { instances })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatewayCliAgentRuntimeInstanceConfig {
    pub id: String,
    pub kind: GatewayCliAgentRuntimeKindConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_home_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_models: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub app_server_args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startup_probe_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_session_ttl_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_channel_capacity: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_ring_lines: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub debug_native_events: Option<bool>,
}

impl GatewayCliAgentRuntimeInstanceConfig {
    fn from_wire(id: String, wire: GatewayCliAgentRuntimeInstanceConfigWire) -> Self {
        Self {
            id,
            kind: wire.kind,
            display_name: normalize_optional_cli_agent_runtime_text(wire.display_name),
            enabled: wire.enabled,
            binary_path: normalize_optional_cli_agent_runtime_text(wire.binary_path),
            home_path: normalize_optional_cli_agent_runtime_text(wire.home_path),
            shadow_home_path: normalize_optional_cli_agent_runtime_text(wire.shadow_home_path),
            custom_models: normalize_cli_agent_runtime_string_list(wire.custom_models),
            app_server_args: wire.app_server_args,
            startup_probe_timeout_ms: wire.startup_probe_timeout_ms.map(|timeout| {
                non_zero_or_default(timeout, default_cli_agent_runtime_startup_timeout_ms())
            }),
            request_timeout_ms: wire.request_timeout_ms.map(|timeout| {
                non_zero_or_default(timeout, default_cli_agent_runtime_request_timeout_ms())
            }),
            idle_session_ttl_secs: wire.idle_session_ttl_secs.map(|ttl| {
                non_zero_or_default(ttl, default_cli_agent_runtime_idle_session_ttl_secs())
            }),
            event_channel_capacity: wire.event_channel_capacity.map(|capacity| {
                non_zero_or_default_usize(
                    capacity,
                    default_cli_agent_runtime_event_channel_capacity(),
                )
            }),
            stderr_ring_lines: wire.stderr_ring_lines.map(|lines| {
                non_zero_or_default_usize(lines, default_cli_agent_runtime_stderr_ring_lines())
            }),
            debug_native_events: wire.debug_native_events,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayCliAgentRuntimeInstanceConfigWire {
    #[serde(default)]
    kind: GatewayCliAgentRuntimeKindConfig,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    binary_path: Option<String>,
    #[serde(default)]
    home_path: Option<String>,
    #[serde(default)]
    shadow_home_path: Option<String>,
    #[serde(default)]
    custom_models: Vec<String>,
    #[serde(default)]
    app_server_args: Vec<String>,
    #[serde(default, alias = "startup_timeout_ms")]
    startup_probe_timeout_ms: Option<u64>,
    #[serde(default)]
    request_timeout_ms: Option<u64>,
    #[serde(default)]
    idle_session_ttl_secs: Option<u64>,
    #[serde(default)]
    event_channel_capacity: Option<usize>,
    #[serde(default)]
    stderr_ring_lines: Option<usize>,
    #[serde(default)]
    debug_native_events: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayCliAgentRuntimeKindConfig {
    #[default]
    Codex,
    Claude,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EffectiveGatewayCliAgentRuntimeInstanceConfig {
    pub id: String,
    pub kind: GatewayCliAgentRuntimeKindConfig,
    pub display_name: String,
    pub enabled: bool,
    pub binary_path: String,
    pub home_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_home_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_models: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub app_server_args: Vec<String>,
    pub startup_probe_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub idle_session_ttl_secs: u64,
    pub event_channel_capacity: usize,
    pub stderr_ring_lines: usize,
    pub debug_native_events: bool,
}

impl Default for GatewayCliAgentRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mcp_tools: GatewayCliAgentRuntimeMcpToolsConfig::default(),
            idle_session_ttl_secs: default_cli_agent_runtime_idle_session_ttl_secs(),
            startup_timeout_ms: default_cli_agent_runtime_startup_timeout_ms(),
            request_timeout_ms: default_cli_agent_runtime_request_timeout_ms(),
            event_channel_capacity: default_cli_agent_runtime_event_channel_capacity(),
            stderr_ring_lines: default_cli_agent_runtime_stderr_ring_lines(),
            debug_native_events: false,
            command_heartbeat: GatewayCliAgentRuntimeCommandHeartbeatConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct GatewayCliAgentRuntimeConfigWire {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    mcp_tools: GatewayCliAgentRuntimeMcpToolsConfig,
    #[serde(default = "default_cli_agent_runtime_idle_session_ttl_secs")]
    idle_session_ttl_secs: u64,
    #[serde(default = "default_cli_agent_runtime_startup_timeout_ms")]
    startup_timeout_ms: u64,
    #[serde(default = "default_cli_agent_runtime_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_cli_agent_runtime_event_channel_capacity")]
    event_channel_capacity: usize,
    #[serde(default = "default_cli_agent_runtime_stderr_ring_lines")]
    stderr_ring_lines: usize,
    #[serde(default)]
    debug_native_events: bool,
    #[serde(default)]
    command_heartbeat: GatewayCliAgentRuntimeCommandHeartbeatConfig,
}

impl<'de> Deserialize<'de> for GatewayCliAgentRuntimeConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GatewayCliAgentRuntimeConfigWire::deserialize(deserializer)?;
        Ok(Self {
            enabled: wire.enabled,
            mcp_tools: wire.mcp_tools,
            idle_session_ttl_secs: non_zero_or_default(
                wire.idle_session_ttl_secs,
                default_cli_agent_runtime_idle_session_ttl_secs(),
            ),
            startup_timeout_ms: non_zero_or_default(
                wire.startup_timeout_ms,
                default_cli_agent_runtime_startup_timeout_ms(),
            ),
            request_timeout_ms: non_zero_or_default(
                wire.request_timeout_ms,
                default_cli_agent_runtime_request_timeout_ms(),
            ),
            event_channel_capacity: non_zero_or_default_usize(
                wire.event_channel_capacity,
                default_cli_agent_runtime_event_channel_capacity(),
            ),
            stderr_ring_lines: non_zero_or_default_usize(
                wire.stderr_ring_lines,
                default_cli_agent_runtime_stderr_ring_lines(),
            ),
            debug_native_events: wire.debug_native_events,
            command_heartbeat: wire.command_heartbeat,
        })
    }
}

impl GatewayConfig {
    pub fn effective_cli_agent_runtime_instances(
        &self,
    ) -> Vec<EffectiveGatewayCliAgentRuntimeInstanceConfig> {
        if self.cli_agent_runtimes.is_empty() {
            if self.cli_agent_runtime.enabled {
                return default_cli_agent_runtime_instances(&self.cli_agent_runtime);
            }
            return Vec::new();
        }

        let mut instances = self
            .cli_agent_runtimes
            .values()
            .map(|instance| effective_cli_agent_runtime_instance(instance, &self.cli_agent_runtime))
            .collect::<Vec<_>>();
        if self.cli_agent_runtime.enabled {
            for default_instance in default_cli_agent_runtime_instances(&self.cli_agent_runtime) {
                if !instances
                    .iter()
                    .any(|instance| instance.id == default_instance.id)
                {
                    instances.push(default_instance);
                }
            }
        }
        instances
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayProviderAttachmentsConfig {
    #[serde(default = "default_provider_attachments_max_bytes_per_attachment")]
    pub max_bytes_per_attachment: usize,
    #[serde(default = "default_provider_attachments_max_total_bytes_per_request")]
    pub max_total_bytes_per_request: usize,
    #[serde(default = "default_provider_attachments_max_attachments_per_request")]
    pub max_attachments_per_request: usize,
    #[serde(default = "default_provider_attachments_upload_preferred_min_bytes")]
    pub upload_preferred_min_bytes: usize,
    #[serde(default = "default_provider_attachments_upload_registry_enabled")]
    pub upload_registry_enabled: bool,
    #[serde(default = "default_provider_attachments_upload_registry_ttl_secs")]
    pub upload_registry_ttl_secs: u64,
    #[serde(default = "default_provider_attachments_enforce_path_allowlist")]
    pub enforce_path_allowlist: bool,
    #[serde(default)]
    pub allowed_path_roots: Vec<String>,
    #[serde(default = "default_provider_attachments_allow_url_sources")]
    pub allow_url_sources: bool,
    #[serde(default = "default_provider_attachments_allow_http")]
    pub allow_http: bool,
    #[serde(default = "default_provider_attachments_allow_private_network")]
    pub allow_private_network: bool,
    #[serde(default = "default_provider_attachments_max_url_redirects")]
    pub max_url_redirects: usize,
    #[serde(default = "default_provider_attachments_url_fetch_timeout_ms")]
    pub url_fetch_timeout_ms: u64,
    #[serde(default = "default_provider_attachments_url_fetch_max_bytes")]
    pub url_fetch_max_bytes: usize,
    #[serde(default)]
    pub url_allowed_domains: Vec<String>,
    #[serde(default = "default_provider_attachments_url_blocked_domains")]
    pub url_blocked_domains: Vec<String>,
    #[serde(default = "default_provider_attachments_security_dry_run")]
    pub security_dry_run: bool,
    #[serde(default = "default_provider_attachments_strict_mime_match")]
    pub strict_mime_match: bool,
    #[serde(default = "default_provider_attachments_max_base64_chars")]
    pub max_base64_chars: usize,
    #[serde(default = "default_provider_attachments_max_filename_chars")]
    pub max_filename_chars: usize,
    #[serde(default = "default_provider_attachments_retry_max_attempts")]
    pub retry_max_attempts: usize,
    #[serde(default = "default_provider_attachments_retry_initial_backoff_ms")]
    pub retry_initial_backoff_ms: u64,
    #[serde(default = "default_provider_attachments_retry_max_backoff_ms")]
    pub retry_max_backoff_ms: u64,
    #[serde(default = "default_provider_attachments_retry_jitter_ms")]
    pub retry_jitter_ms: u64,
    #[serde(default = "default_provider_attachments_circuit_breaker_failure_threshold")]
    pub circuit_breaker_failure_threshold: u32,
    #[serde(default = "default_provider_attachments_circuit_breaker_open_ms")]
    pub circuit_breaker_open_ms: u64,
}

impl Default for GatewayProviderAttachmentsConfig {
    fn default() -> Self {
        Self {
            max_bytes_per_attachment: default_provider_attachments_max_bytes_per_attachment(),
            max_total_bytes_per_request: default_provider_attachments_max_total_bytes_per_request(),
            max_attachments_per_request: default_provider_attachments_max_attachments_per_request(),
            upload_preferred_min_bytes: default_provider_attachments_upload_preferred_min_bytes(),
            upload_registry_enabled: default_provider_attachments_upload_registry_enabled(),
            upload_registry_ttl_secs: default_provider_attachments_upload_registry_ttl_secs(),
            enforce_path_allowlist: default_provider_attachments_enforce_path_allowlist(),
            allowed_path_roots: Vec::new(),
            allow_url_sources: default_provider_attachments_allow_url_sources(),
            allow_http: default_provider_attachments_allow_http(),
            allow_private_network: default_provider_attachments_allow_private_network(),
            max_url_redirects: default_provider_attachments_max_url_redirects(),
            url_fetch_timeout_ms: default_provider_attachments_url_fetch_timeout_ms(),
            url_fetch_max_bytes: default_provider_attachments_url_fetch_max_bytes(),
            url_allowed_domains: Vec::new(),
            url_blocked_domains: default_provider_attachments_url_blocked_domains(),
            security_dry_run: default_provider_attachments_security_dry_run(),
            strict_mime_match: default_provider_attachments_strict_mime_match(),
            max_base64_chars: default_provider_attachments_max_base64_chars(),
            max_filename_chars: default_provider_attachments_max_filename_chars(),
            retry_max_attempts: default_provider_attachments_retry_max_attempts(),
            retry_initial_backoff_ms: default_provider_attachments_retry_initial_backoff_ms(),
            retry_max_backoff_ms: default_provider_attachments_retry_max_backoff_ms(),
            retry_jitter_ms: default_provider_attachments_retry_jitter_ms(),
            circuit_breaker_failure_threshold:
                default_provider_attachments_circuit_breaker_failure_threshold(),
            circuit_breaker_open_ms: default_provider_attachments_circuit_breaker_open_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GatewayToolsConfig {
    #[serde(default)]
    pub web: GatewayWebToolsConfig,
    #[serde(default)]
    pub computer_use: GatewayComputerUseToolsConfig,
    #[serde(default)]
    pub budget: GatewayToolLoopBudgetConfig,
    #[serde(default)]
    pub execution_windows: Option<GatewayExecutionWindowsConfig>,
    #[serde(default)]
    pub retry: GatewayToolRetryBudgetConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayToolLoopBudgetConfig {
    #[serde(default = "default_tool_loop_max_agent_rounds_per_turn")]
    pub max_agent_rounds_per_turn: u32,
    #[serde(default = "default_tool_loop_max_tool_calls_per_turn")]
    pub max_tool_calls_per_turn: u32,
}

impl Default for GatewayToolLoopBudgetConfig {
    fn default() -> Self {
        Self {
            max_agent_rounds_per_turn: default_tool_loop_max_agent_rounds_per_turn(),
            max_tool_calls_per_turn: default_tool_loop_max_tool_calls_per_turn(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GatewayExecutionWindowsConfig {
    #[serde(flatten)]
    pub window: GatewayExecutionWindowBudgetConfig,
    #[serde(default)]
    pub total: GatewayExecutionWindowTotalBudgetConfig,
}

impl Default for GatewayExecutionWindowsConfig {
    fn default() -> Self {
        Self {
            window: GatewayExecutionWindowBudgetConfig::default(),
            total: GatewayExecutionWindowTotalBudgetConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GatewayExecutionWindowBudgetConfig {
    #[serde(default = "default_execution_window_max_agent_rounds_per_window")]
    pub max_agent_rounds_per_window: u32,
    #[serde(default = "default_execution_window_max_tool_calls_per_window")]
    pub max_tool_calls_per_window: u32,
    #[serde(default = "default_execution_window_max_wall_clock_ms_per_window")]
    pub max_wall_clock_ms_per_window: Option<u64>,
    #[serde(default)]
    pub max_provider_tokens_per_window: Option<u64>,
}

impl Default for GatewayExecutionWindowBudgetConfig {
    fn default() -> Self {
        Self {
            max_agent_rounds_per_window: default_execution_window_max_agent_rounds_per_window(),
            max_tool_calls_per_window: default_execution_window_max_tool_calls_per_window(),
            max_wall_clock_ms_per_window: default_execution_window_max_wall_clock_ms_per_window(),
            max_provider_tokens_per_window: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GatewayExecutionWindowTotalBudgetConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_windows_per_turn: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls_per_turn: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_clock_ms_per_turn: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_provider_tokens_per_turn: Option<u64>,
    #[serde(
        default = "default_execution_window_total_max_consecutive_no_progress_windows",
        alias = "max_consecutive_failed_windows"
    )]
    pub max_consecutive_no_progress_windows: u32,
}

impl Default for GatewayExecutionWindowTotalBudgetConfig {
    fn default() -> Self {
        Self {
            max_windows_per_turn: None,
            max_tool_calls_per_turn: None,
            max_wall_clock_ms_per_turn: None,
            max_provider_tokens_per_turn: None,
            max_consecutive_no_progress_windows:
                default_execution_window_total_max_consecutive_no_progress_windows(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayToolRetryBudgetConfig {
    #[serde(default = "default_tool_retry_max_recoverable_retry_rounds_per_episode")]
    pub max_recoverable_retry_rounds_per_episode: u32,
    #[serde(default = "default_tool_retry_max_same_tool_error_retries_per_episode")]
    pub max_same_tool_error_retries_per_episode: u32,
    #[serde(default = "default_tool_retry_max_retries_per_tool_name_per_episode")]
    pub max_retries_per_tool_name_per_episode: u32,
}

impl Default for GatewayToolRetryBudgetConfig {
    fn default() -> Self {
        Self {
            max_recoverable_retry_rounds_per_episode:
                default_tool_retry_max_recoverable_retry_rounds_per_episode(),
            max_same_tool_error_retries_per_episode:
                default_tool_retry_max_same_tool_error_retries_per_episode(),
            max_retries_per_tool_name_per_episode:
                default_tool_retry_max_retries_per_tool_name_per_episode(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySkillsConfig {
    #[serde(default = "default_skills_enabled")]
    pub enabled: bool,
    #[serde(default = "default_skills_max_skills_per_source")]
    pub max_skills_per_source: usize,
    #[serde(default = "default_skills_max_skill_file_bytes")]
    pub max_skill_file_bytes: usize,
    #[serde(default = "default_skills_prompt_max_chars")]
    pub prompt_max_chars: usize,
    #[serde(default = "default_skills_allow_implicit_invocation")]
    pub allow_implicit_invocation: bool,
    #[serde(default)]
    pub paths: GatewaySkillsPathsConfig,
    #[serde(default)]
    pub validation: GatewaySkillsValidationConfig,
    #[serde(default)]
    pub security: GatewaySkillsSecurityConfig,
    #[serde(default)]
    pub dependencies: GatewaySkillsDependenciesConfig,
    #[serde(default)]
    pub runtime: GatewaySkillsRuntimeConfig,
}

impl Default for GatewaySkillsConfig {
    fn default() -> Self {
        Self {
            enabled: default_skills_enabled(),
            max_skills_per_source: default_skills_max_skills_per_source(),
            max_skill_file_bytes: default_skills_max_skill_file_bytes(),
            prompt_max_chars: default_skills_prompt_max_chars(),
            allow_implicit_invocation: default_skills_allow_implicit_invocation(),
            paths: GatewaySkillsPathsConfig::default(),
            validation: GatewaySkillsValidationConfig::default(),
            security: GatewaySkillsSecurityConfig::default(),
            dependencies: GatewaySkillsDependenciesConfig::default(),
            runtime: GatewaySkillsRuntimeConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySkillsPathsConfig {
    #[serde(default = "default_skills_system_paths")]
    pub system: Vec<String>,
    #[serde(default = "default_skills_user_paths")]
    pub user: Vec<String>,
    #[serde(default = "default_skills_registry_paths")]
    pub registry: Vec<String>,
}

impl Default for GatewaySkillsPathsConfig {
    fn default() -> Self {
        Self {
            system: default_skills_system_paths(),
            user: default_skills_user_paths(),
            registry: default_skills_registry_paths(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySkillsValidationConfig {
    #[serde(default = "default_skills_validation_strict_agentskills")]
    pub strict_agentskills: bool,
    #[serde(default = "default_skills_validation_accept_openclaw_profile")]
    pub accept_openclaw_profile: bool,
}

impl Default for GatewaySkillsValidationConfig {
    fn default() -> Self {
        Self {
            strict_agentskills: default_skills_validation_strict_agentskills(),
            accept_openclaw_profile: default_skills_validation_accept_openclaw_profile(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySkillsSecurityConfig {
    #[serde(default = "default_skills_security_allow_untrusted_install")]
    pub allow_untrusted_install: bool,
    #[serde(default = "default_skills_security_min_trust_for_shell_tools")]
    pub min_trust_for_shell_tools: String,
    #[serde(default = "default_skills_security_min_trust_for_http_tools")]
    pub min_trust_for_http_tools: String,
    #[serde(default = "default_skills_security_min_trust_for_function_proxy_tools")]
    pub min_trust_for_function_proxy_tools: String,
    #[serde(default = "default_skills_security_max_install_archive_compressed_bytes")]
    pub max_install_archive_compressed_bytes: usize,
    #[serde(default = "default_skills_security_max_install_archive_uncompressed_bytes")]
    pub max_install_archive_uncompressed_bytes: usize,
    #[serde(default = "default_skills_security_max_install_archive_entries")]
    pub max_install_archive_entries: usize,
    #[serde(default = "default_skills_security_max_install_file_bytes")]
    pub max_install_file_bytes: usize,
    #[serde(default = "default_skills_security_upload_ttl_secs")]
    pub upload_ttl_secs: u64,
    #[serde(default = "default_skills_security_upload_recommended_chunk_size_bytes")]
    pub upload_recommended_chunk_size_bytes: usize,
    #[serde(default = "default_skills_security_upload_max_chunk_size_bytes")]
    pub upload_max_chunk_size_bytes: usize,
}

impl Default for GatewaySkillsSecurityConfig {
    fn default() -> Self {
        Self {
            allow_untrusted_install: default_skills_security_allow_untrusted_install(),
            min_trust_for_shell_tools: default_skills_security_min_trust_for_shell_tools(),
            min_trust_for_http_tools: default_skills_security_min_trust_for_http_tools(),
            min_trust_for_function_proxy_tools:
                default_skills_security_min_trust_for_function_proxy_tools(),
            max_install_archive_compressed_bytes:
                default_skills_security_max_install_archive_compressed_bytes(),
            max_install_archive_uncompressed_bytes:
                default_skills_security_max_install_archive_uncompressed_bytes(),
            max_install_archive_entries: default_skills_security_max_install_archive_entries(),
            max_install_file_bytes: default_skills_security_max_install_file_bytes(),
            upload_ttl_secs: default_skills_security_upload_ttl_secs(),
            upload_recommended_chunk_size_bytes:
                default_skills_security_upload_recommended_chunk_size_bytes(),
            upload_max_chunk_size_bytes: default_skills_security_upload_max_chunk_size_bytes(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySkillsDependenciesConfig {
    #[serde(default = "default_skills_dependencies_preflight_on_resolve")]
    pub preflight_on_resolve: bool,
    #[serde(default = "default_skills_dependencies_runtime_recheck_on_tool_call")]
    pub runtime_recheck_on_tool_call: bool,
}

impl Default for GatewaySkillsDependenciesConfig {
    fn default() -> Self {
        Self {
            preflight_on_resolve: default_skills_dependencies_preflight_on_resolve(),
            runtime_recheck_on_tool_call: default_skills_dependencies_runtime_recheck_on_tool_call(
            ),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySkillsRuntimeConfig {
    #[serde(default = "default_skills_runtime_enable_dynamic_tools")]
    pub enable_dynamic_tools: bool,
    #[serde(default = "default_skills_runtime_enable_read_skill")]
    pub enable_read_skill: bool,
    #[serde(default = "default_skills_runtime_max_dynamic_tools_per_skill")]
    pub max_dynamic_tools_per_skill: usize,
    #[serde(default = "default_skills_runtime_read_skill_max_chars")]
    pub read_skill_max_chars: usize,
    #[serde(default = "default_skills_runtime_compact_mode_threshold")]
    pub compact_mode_threshold: usize,
    #[serde(default = "default_skills_runtime_allow_shell_tools")]
    pub allow_shell_tools: bool,
    #[serde(default = "default_skills_runtime_allow_http_tools")]
    pub allow_http_tools: bool,
    #[serde(default = "default_skills_runtime_allow_function_proxy_tools")]
    pub allow_function_proxy_tools: bool,
}

impl Default for GatewaySkillsRuntimeConfig {
    fn default() -> Self {
        Self {
            enable_dynamic_tools: default_skills_runtime_enable_dynamic_tools(),
            enable_read_skill: default_skills_runtime_enable_read_skill(),
            max_dynamic_tools_per_skill: default_skills_runtime_max_dynamic_tools_per_skill(),
            read_skill_max_chars: default_skills_runtime_read_skill_max_chars(),
            compact_mode_threshold: default_skills_runtime_compact_mode_threshold(),
            allow_shell_tools: default_skills_runtime_allow_shell_tools(),
            allow_http_tools: default_skills_runtime_allow_http_tools(),
            allow_function_proxy_tools: default_skills_runtime_allow_function_proxy_tools(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayWebToolsConfig {
    #[serde(default = "default_web_tool_timeout_ms")]
    pub default_timeout_ms: u64,
    #[serde(default = "default_web_tool_hard_timeout_ms")]
    pub hard_max_timeout_ms: u64,
    #[serde(default = "default_web_tool_fetch_max_bytes")]
    pub default_fetch_max_bytes: usize,
    #[serde(default = "default_web_tool_hard_fetch_max_bytes")]
    pub hard_fetch_max_bytes: usize,
    #[serde(default = "default_web_tool_download_max_bytes")]
    pub default_download_max_bytes: usize,
    #[serde(default = "default_web_tool_hard_download_max_bytes")]
    pub hard_download_max_bytes: usize,
    #[serde(default = "default_web_tool_max_results")]
    pub default_max_results: usize,
    #[serde(default = "default_web_tool_hard_max_results")]
    pub hard_max_results: usize,
    #[serde(default = "default_web_tool_snippet_chars")]
    pub default_snippet_chars: usize,
    #[serde(default = "default_web_tool_hard_snippet_chars")]
    pub hard_max_snippet_chars: usize,
    #[serde(default = "default_web_tool_link_count")]
    pub default_link_count: usize,
    #[serde(default = "default_web_tool_hard_link_count")]
    pub hard_link_count: usize,
    #[serde(default = "default_web_tool_render_max_chars")]
    pub default_render_max_chars: usize,
    #[serde(default = "default_web_tool_ddg_html_search_url")]
    pub ddg_html_search_url: String,
    #[serde(default = "default_web_tool_ddg_instant_api_url")]
    pub ddg_instant_api_url: String,
    #[serde(default = "default_web_tool_user_agent")]
    pub default_user_agent: String,
}

impl Default for GatewayWebToolsConfig {
    fn default() -> Self {
        Self {
            default_timeout_ms: default_web_tool_timeout_ms(),
            hard_max_timeout_ms: default_web_tool_hard_timeout_ms(),
            default_fetch_max_bytes: default_web_tool_fetch_max_bytes(),
            hard_fetch_max_bytes: default_web_tool_hard_fetch_max_bytes(),
            default_download_max_bytes: default_web_tool_download_max_bytes(),
            hard_download_max_bytes: default_web_tool_hard_download_max_bytes(),
            default_max_results: default_web_tool_max_results(),
            hard_max_results: default_web_tool_hard_max_results(),
            default_snippet_chars: default_web_tool_snippet_chars(),
            hard_max_snippet_chars: default_web_tool_hard_snippet_chars(),
            default_link_count: default_web_tool_link_count(),
            hard_link_count: default_web_tool_hard_link_count(),
            default_render_max_chars: default_web_tool_render_max_chars(),
            ddg_html_search_url: default_web_tool_ddg_html_search_url(),
            ddg_instant_api_url: default_web_tool_ddg_instant_api_url(),
            default_user_agent: default_web_tool_user_agent(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayComputerUseToolsConfig {
    #[serde(default = "default_computer_use_artifacts_subdir")]
    pub artifacts_subdir: String,
    #[serde(default = "default_computer_use_retention_hours")]
    pub retention_hours: u64,
    #[serde(default = "default_computer_use_max_total_bytes")]
    pub max_total_bytes: u64,
    #[serde(default = "default_computer_use_run_max_steps_default")]
    pub run_max_steps_default: u32,
    #[serde(default = "default_computer_use_snapshot_transport_max_bytes")]
    pub snapshot_transport_max_bytes: usize,
    #[serde(default = "default_computer_use_snapshot_transport_max_side_px")]
    pub snapshot_transport_max_side_px: u32,
    #[serde(default = "default_computer_use_snapshot_transport_min_side_px")]
    pub snapshot_transport_min_side_px: u32,
    #[serde(default = "default_computer_use_snapshot_downscale_factor")]
    pub snapshot_downscale_factor: f64,
    #[serde(default = "default_computer_use_accessibility_tree_max_depth")]
    pub accessibility_tree_max_depth: usize,
    #[serde(default = "default_computer_use_accessibility_tree_max_nodes")]
    pub accessibility_tree_max_nodes: usize,
    #[serde(default = "default_computer_use_accessibility_tree_max_serialized_bytes")]
    pub accessibility_tree_max_serialized_bytes: usize,
    #[serde(default = "default_computer_use_accessibility_tree_text_max_chars")]
    pub accessibility_tree_text_max_chars: usize,
    #[serde(default = "default_computer_use_semantic_action_timeout_ms")]
    pub semantic_action_timeout_ms: u64,
    #[serde(default = "default_computer_use_app_activation_timeout_ms")]
    pub app_activation_timeout_ms: u64,
    #[serde(default = "default_computer_use_input_simulation_enabled")]
    pub input_simulation_enabled: bool,
    #[serde(default = "default_computer_use_launch_if_missing_default")]
    pub launch_if_missing_default: bool,
    #[serde(default)]
    pub allowed_launch_commands: Vec<String>,
    #[serde(default = "default_computer_use_preflight_screenshot_probe_enabled")]
    pub preflight_screenshot_probe_enabled: bool,
    #[serde(default = "default_computer_use_max_consecutive_same_snapshot_hash")]
    pub max_consecutive_same_snapshot_hash: u32,
    #[serde(default = "default_computer_use_max_consecutive_same_action_signature")]
    pub max_consecutive_same_action_signature: u32,
    #[serde(default = "default_computer_use_max_consecutive_no_progress_steps")]
    pub max_consecutive_no_progress_steps: u32,
    #[serde(default = "default_computer_use_max_recovery_attempts_per_step")]
    pub max_recovery_attempts_per_step: u32,
    #[serde(default = "default_computer_use_max_recovery_attempts_per_run")]
    pub max_recovery_attempts_per_run: u32,
}

impl Default for GatewayComputerUseToolsConfig {
    fn default() -> Self {
        Self {
            artifacts_subdir: default_computer_use_artifacts_subdir(),
            retention_hours: default_computer_use_retention_hours(),
            max_total_bytes: default_computer_use_max_total_bytes(),
            run_max_steps_default: default_computer_use_run_max_steps_default(),
            snapshot_transport_max_bytes: default_computer_use_snapshot_transport_max_bytes(),
            snapshot_transport_max_side_px: default_computer_use_snapshot_transport_max_side_px(),
            snapshot_transport_min_side_px: default_computer_use_snapshot_transport_min_side_px(),
            snapshot_downscale_factor: default_computer_use_snapshot_downscale_factor(),
            accessibility_tree_max_depth: default_computer_use_accessibility_tree_max_depth(),
            accessibility_tree_max_nodes: default_computer_use_accessibility_tree_max_nodes(),
            accessibility_tree_max_serialized_bytes:
                default_computer_use_accessibility_tree_max_serialized_bytes(),
            accessibility_tree_text_max_chars:
                default_computer_use_accessibility_tree_text_max_chars(),
            semantic_action_timeout_ms: default_computer_use_semantic_action_timeout_ms(),
            app_activation_timeout_ms: default_computer_use_app_activation_timeout_ms(),
            input_simulation_enabled: default_computer_use_input_simulation_enabled(),
            launch_if_missing_default: default_computer_use_launch_if_missing_default(),
            allowed_launch_commands: Vec::new(),
            preflight_screenshot_probe_enabled:
                default_computer_use_preflight_screenshot_probe_enabled(),
            max_consecutive_same_snapshot_hash:
                default_computer_use_max_consecutive_same_snapshot_hash(),
            max_consecutive_same_action_signature:
                default_computer_use_max_consecutive_same_action_signature(),
            max_consecutive_no_progress_steps:
                default_computer_use_max_consecutive_no_progress_steps(),
            max_recovery_attempts_per_step: default_computer_use_max_recovery_attempts_per_step(),
            max_recovery_attempts_per_run: default_computer_use_max_recovery_attempts_per_run(),
        }
    }
}

const fn default_web_tool_timeout_ms() -> u64 {
    20_000
}

const fn default_web_tool_hard_timeout_ms() -> u64 {
    120_000
}

const fn default_web_tool_fetch_max_bytes() -> usize {
    2 * 1024 * 1024
}

const fn default_web_tool_hard_fetch_max_bytes() -> usize {
    8 * 1024 * 1024
}

const fn default_web_tool_download_max_bytes() -> usize {
    128 * 1024 * 1024
}

const fn default_web_tool_hard_download_max_bytes() -> usize {
    1024 * 1024 * 1024
}

const fn default_web_tool_max_results() -> usize {
    8
}

const fn default_web_tool_hard_max_results() -> usize {
    20
}

const fn default_web_tool_snippet_chars() -> usize {
    420
}

const fn default_web_tool_hard_snippet_chars() -> usize {
    4_096
}

const fn default_web_tool_link_count() -> usize {
    40
}

const fn default_web_tool_hard_link_count() -> usize {
    200
}

const fn default_web_tool_render_max_chars() -> usize {
    40_000
}

fn default_web_tool_ddg_html_search_url() -> String {
    "https://duckduckgo.com/html/".to_owned()
}

fn default_web_tool_ddg_instant_api_url() -> String {
    "https://api.duckduckgo.com/".to_owned()
}

fn default_web_tool_user_agent() -> String {
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36".to_owned()
}

const fn default_tool_loop_max_agent_rounds_per_turn() -> u32 {
    32
}

const fn default_tool_loop_max_tool_calls_per_turn() -> u32 {
    128
}

const fn default_execution_window_max_agent_rounds_per_window() -> u32 {
    32
}

const fn default_execution_window_max_tool_calls_per_window() -> u32 {
    128
}

const fn default_execution_window_max_wall_clock_ms_per_window() -> Option<u64> {
    Some(1_800_000)
}

const fn default_execution_window_total_max_consecutive_no_progress_windows() -> u32 {
    3
}

const fn default_tool_retry_max_recoverable_retry_rounds_per_episode() -> u32 {
    32
}

const fn default_tool_retry_max_same_tool_error_retries_per_episode() -> u32 {
    3
}

const fn default_tool_retry_max_retries_per_tool_name_per_episode() -> u32 {
    16
}

fn default_computer_use_artifacts_subdir() -> String {
    "tools/computer_use".to_owned()
}

const fn default_computer_use_retention_hours() -> u64 {
    24
}

const fn default_computer_use_max_total_bytes() -> u64 {
    1024 * 1024 * 1024
}

const fn default_computer_use_run_max_steps_default() -> u32 {
    300
}

const fn default_gateway_keepawake() -> bool {
    false
}

const fn default_gateway_memory_enabled() -> bool {
    true
}

fn default_gateway_memory_capsules_dir() -> String {
    "memory/capsules".to_owned()
}

const fn default_gateway_memory_allow_global_user() -> bool {
    true
}

const fn default_gateway_memory_allow_global_agent() -> bool {
    false
}

const fn default_gateway_memory_deterministic_recall_enabled() -> bool {
    true
}

const fn default_gateway_memory_active_recall_enabled() -> bool {
    true
}

const fn default_gateway_memory_tools_enabled() -> bool {
    true
}

const fn default_gateway_memory_proactive_writes_enabled() -> bool {
    true
}

const fn default_gateway_memory_background_extraction_enabled() -> bool {
    true
}

const fn default_gateway_thread_episodic_enabled() -> bool {
    true
}

const fn default_gateway_thread_episodic_indexing_enabled() -> bool {
    true
}

const fn default_gateway_thread_episodic_recall_enabled() -> bool {
    true
}

const fn default_gateway_thread_episodic_prompt_chars() -> u32 {
    2_400
}

const fn default_gateway_thread_episodic_max_prompt_chars() -> u32 {
    12_000
}

const fn default_gateway_thread_episodic_max_hit_chars() -> usize {
    1_200
}

const fn default_gateway_thread_episodic_max_candidates() -> u32 {
    32
}

const fn default_gateway_thread_episodic_max_candidate_work() -> u32 {
    128
}

const fn default_gateway_thread_episodic_max_segments() -> u64 {
    16
}

const fn default_gateway_thread_episodic_min_relevancy() -> f32 {
    0.25
}

const fn default_gateway_thread_episodic_min_results() -> u32 {
    1
}

const fn default_gateway_thread_episodic_snippet_chars() -> u32 {
    360
}

const fn default_gateway_thread_episodic_index_batch_limit() -> u64 {
    16
}

const fn default_gateway_thread_episodic_retry_base_delay_secs() -> i64 {
    30
}

const fn default_gateway_thread_episodic_retry_max_delay_secs() -> i64 {
    15 * 60
}

const fn default_gateway_thread_episodic_max_attempts() -> i64 {
    5
}

const fn default_gateway_thread_episodic_near_capacity_percent() -> f64 {
    85.0
}

const fn default_gateway_thread_episodic_vector_normalized() -> bool {
    true
}

fn normalized_optional_model_selection_text(
    value: Option<&str>,
    max_chars: usize,
) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.chars().take(max_chars).collect())
}

const fn default_gateway_hook_recovery_enabled() -> bool {
    true
}

const fn default_gateway_hook_recovery_startup_scan() -> bool {
    true
}

const fn default_gateway_hook_recovery_poll_interval_ms() -> u64 {
    2_000
}

const fn default_gateway_hook_recovery_batch_size() -> usize {
    64
}

const fn default_gateway_hook_recovery_max_concurrent() -> usize {
    4
}

const fn default_gateway_hook_recovery_stale_running_after_ms() -> u64 {
    120_000
}

const fn default_computer_use_snapshot_transport_max_bytes() -> usize {
    8 * 1024 * 1024
}

const fn default_computer_use_snapshot_transport_max_side_px() -> u32 {
    1280
}

const fn default_computer_use_snapshot_transport_min_side_px() -> u32 {
    320
}

const fn default_computer_use_snapshot_downscale_factor() -> f64 {
    0.85
}

const fn default_computer_use_accessibility_tree_max_depth() -> usize {
    6
}

const fn default_computer_use_accessibility_tree_max_nodes() -> usize {
    200
}

const fn default_computer_use_accessibility_tree_max_serialized_bytes() -> usize {
    192 * 1024
}

const fn default_computer_use_accessibility_tree_text_max_chars() -> usize {
    160
}

const fn default_computer_use_semantic_action_timeout_ms() -> u64 {
    30_000
}

const fn default_computer_use_app_activation_timeout_ms() -> u64 {
    5_000
}

const fn default_computer_use_input_simulation_enabled() -> bool {
    true
}

const fn default_computer_use_launch_if_missing_default() -> bool {
    false
}

const fn default_computer_use_preflight_screenshot_probe_enabled() -> bool {
    true
}

const fn default_computer_use_max_consecutive_same_snapshot_hash() -> u32 {
    6
}

const fn default_computer_use_max_consecutive_same_action_signature() -> u32 {
    8
}

const fn default_computer_use_max_consecutive_no_progress_steps() -> u32 {
    4
}

const fn default_computer_use_max_recovery_attempts_per_step() -> u32 {
    2
}

const fn default_computer_use_max_recovery_attempts_per_run() -> u32 {
    12
}

const fn default_provider_non_stream_request_timeout_secs() -> u64 {
    120
}

const fn default_provider_connect_timeout_secs() -> u64 {
    30
}

const fn default_provider_first_chunk_timeout_secs() -> u64 {
    180
}

const fn default_provider_inter_chunk_idle_timeout_secs() -> u64 {
    180
}

const fn default_cli_agent_runtime_idle_session_ttl_secs() -> u64 {
    1_800
}

const fn default_cli_agent_runtime_startup_timeout_ms() -> u64 {
    30_000
}

const fn default_cli_agent_runtime_request_timeout_ms() -> u64 {
    120_000
}

const fn default_cli_agent_runtime_event_channel_capacity() -> usize {
    2_048
}

const fn default_cli_agent_runtime_stderr_ring_lines() -> usize {
    200
}

const fn default_cli_agent_runtime_command_heartbeat_interval_secs() -> u64 {
    60
}

const fn default_cli_agent_runtime_mcp_max_tools() -> usize {
    512
}

const fn default_cli_agent_runtime_mcp_max_total_schema_bytes() -> usize {
    3_145_728
}

const fn default_cli_agent_runtime_mcp_max_concurrent_calls_per_turn() -> usize {
    16
}

const fn default_command_execution_lease_timeout_secs() -> u64 {
    10 * 60
}

const fn default_command_execution_idle_timeout_secs() -> u64 {
    30 * 60
}

const fn default_command_execution_hard_timeout_secs() -> u64 {
    60 * 60
}

const fn default_command_execution_recovery_max_wall_clock_secs() -> u64 {
    60 * 60
}

const fn default_context_compaction_lease_timeout_secs() -> u64 {
    10 * 60
}

const fn default_context_compaction_idle_timeout_secs() -> u64 {
    30 * 60
}

const fn default_context_compaction_hard_timeout_secs() -> u64 {
    4 * 60 * 60
}

const fn default_context_compaction_recovery_grace_secs() -> u64 {
    15 * 60
}

const fn default_provider_stream_item_lease_timeout_secs() -> u64 {
    16 * 60
}

const fn default_provider_stream_item_idle_timeout_secs() -> u64 {
    15 * 60
}

const fn default_provider_stream_item_hard_timeout_secs() -> u64 {
    30 * 60
}

const fn non_zero_or_default(value: u64, default_value: u64) -> u64 {
    if value == 0 { default_value } else { value }
}

const fn non_zero_or_default_usize(value: usize, default_value: usize) -> usize {
    if value == 0 { default_value } else { value }
}

fn default_codex_cli_agent_runtime_instance(
    defaults: &GatewayCliAgentRuntimeConfig,
) -> EffectiveGatewayCliAgentRuntimeInstanceConfig {
    EffectiveGatewayCliAgentRuntimeInstanceConfig {
        id: "codex".to_owned(),
        kind: GatewayCliAgentRuntimeKindConfig::Codex,
        display_name: "Codex".to_owned(),
        enabled: defaults.enabled,
        binary_path: "codex".to_owned(),
        home_path: "~/.codex".to_owned(),
        shadow_home_path: None,
        custom_models: Vec::new(),
        app_server_args: Vec::new(),
        startup_probe_timeout_ms: defaults.startup_timeout_ms,
        request_timeout_ms: defaults.request_timeout_ms,
        idle_session_ttl_secs: defaults.idle_session_ttl_secs,
        event_channel_capacity: defaults.event_channel_capacity,
        stderr_ring_lines: defaults.stderr_ring_lines,
        debug_native_events: defaults.debug_native_events,
    }
}

fn default_claude_cli_agent_runtime_instance(
    defaults: &GatewayCliAgentRuntimeConfig,
) -> EffectiveGatewayCliAgentRuntimeInstanceConfig {
    EffectiveGatewayCliAgentRuntimeInstanceConfig {
        id: "claude".to_owned(),
        kind: GatewayCliAgentRuntimeKindConfig::Claude,
        display_name: "Claude".to_owned(),
        enabled: defaults.enabled,
        binary_path: "claude".to_owned(),
        home_path: "~/.claude".to_owned(),
        shadow_home_path: None,
        custom_models: Vec::new(),
        app_server_args: Vec::new(),
        startup_probe_timeout_ms: defaults.startup_timeout_ms,
        request_timeout_ms: defaults.request_timeout_ms,
        idle_session_ttl_secs: defaults.idle_session_ttl_secs,
        event_channel_capacity: defaults.event_channel_capacity,
        stderr_ring_lines: defaults.stderr_ring_lines,
        debug_native_events: defaults.debug_native_events,
    }
}

fn default_cli_agent_runtime_instances(
    defaults: &GatewayCliAgentRuntimeConfig,
) -> Vec<EffectiveGatewayCliAgentRuntimeInstanceConfig> {
    vec![
        default_codex_cli_agent_runtime_instance(defaults),
        default_claude_cli_agent_runtime_instance(defaults),
    ]
}

fn effective_cli_agent_runtime_instance(
    instance: &GatewayCliAgentRuntimeInstanceConfig,
    defaults: &GatewayCliAgentRuntimeConfig,
) -> EffectiveGatewayCliAgentRuntimeInstanceConfig {
    let display_name = instance
        .display_name
        .clone()
        .unwrap_or_else(|| display_name_from_cli_agent_runtime_id(instance.id.as_str()));
    EffectiveGatewayCliAgentRuntimeInstanceConfig {
        id: instance.id.clone(),
        kind: instance.kind,
        display_name,
        enabled: instance.enabled.unwrap_or(true),
        binary_path: instance
            .binary_path
            .clone()
            .unwrap_or_else(|| default_cli_agent_runtime_binary_path(instance.kind)),
        home_path: instance
            .home_path
            .clone()
            .unwrap_or_else(|| default_cli_agent_runtime_home_path(instance.kind)),
        shadow_home_path: instance.shadow_home_path.clone(),
        custom_models: instance.custom_models.clone(),
        app_server_args: instance.app_server_args.clone(),
        startup_probe_timeout_ms: instance
            .startup_probe_timeout_ms
            .unwrap_or(defaults.startup_timeout_ms),
        request_timeout_ms: instance
            .request_timeout_ms
            .unwrap_or(defaults.request_timeout_ms),
        idle_session_ttl_secs: instance
            .idle_session_ttl_secs
            .unwrap_or(defaults.idle_session_ttl_secs),
        event_channel_capacity: instance
            .event_channel_capacity
            .unwrap_or(defaults.event_channel_capacity),
        stderr_ring_lines: instance
            .stderr_ring_lines
            .unwrap_or(defaults.stderr_ring_lines),
        debug_native_events: instance
            .debug_native_events
            .unwrap_or(defaults.debug_native_events),
    }
}

fn default_cli_agent_runtime_binary_path(kind: GatewayCliAgentRuntimeKindConfig) -> String {
    match kind {
        GatewayCliAgentRuntimeKindConfig::Codex => "codex".to_owned(),
        GatewayCliAgentRuntimeKindConfig::Claude => "claude".to_owned(),
    }
}

fn default_cli_agent_runtime_home_path(kind: GatewayCliAgentRuntimeKindConfig) -> String {
    match kind {
        GatewayCliAgentRuntimeKindConfig::Codex => "~/.codex".to_owned(),
        GatewayCliAgentRuntimeKindConfig::Claude => "~/.claude".to_owned(),
    }
}

fn normalize_cli_agent_runtime_instance_id(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("CLI agent runtime instance id must not be empty");
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
            bail!("CLI agent runtime instance id `{raw}` contains unsupported character `{ch}`");
        }
    }

    let normalized = normalized.trim_matches('_').to_owned();
    if normalized.is_empty() {
        bail!("CLI agent runtime instance id `{raw}` must contain an ASCII letter or digit");
    }
    Ok(normalized)
}

fn normalize_optional_cli_agent_runtime_text(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

fn normalize_cli_agent_runtime_string_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .filter_map(|value| normalize_optional_cli_agent_runtime_text(Some(value)))
        .collect()
}

fn display_name_from_cli_agent_runtime_id(id: &str) -> String {
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

const fn default_provider_attachments_max_bytes_per_attachment() -> usize {
    100 * 1024 * 1024
}

const fn default_provider_attachments_max_total_bytes_per_request() -> usize {
    200 * 1024 * 1024
}

const fn default_provider_attachments_max_attachments_per_request() -> usize {
    64
}

const fn default_provider_attachments_upload_preferred_min_bytes() -> usize {
    512 * 1024
}

const fn default_provider_attachments_upload_registry_enabled() -> bool {
    true
}

const fn default_provider_attachments_upload_registry_ttl_secs() -> u64 {
    7 * 24 * 3600
}

const fn default_provider_attachments_enforce_path_allowlist() -> bool {
    false
}

const fn default_provider_attachments_allow_url_sources() -> bool {
    false
}

const fn default_provider_attachments_allow_http() -> bool {
    false
}

const fn default_provider_attachments_allow_private_network() -> bool {
    false
}

const fn default_provider_attachments_max_url_redirects() -> usize {
    3
}

const fn default_provider_attachments_url_fetch_timeout_ms() -> u64 {
    15_000
}

const fn default_provider_attachments_url_fetch_max_bytes() -> usize {
    20 * 1024 * 1024
}

fn default_provider_attachments_url_blocked_domains() -> Vec<String> {
    vec!["localhost".to_owned()]
}

const fn default_provider_attachments_security_dry_run() -> bool {
    false
}

const fn default_provider_attachments_strict_mime_match() -> bool {
    false
}

const fn default_provider_attachments_max_base64_chars() -> usize {
    200 * 1024 * 1024
}

const fn default_provider_attachments_max_filename_chars() -> usize {
    128
}

const fn default_provider_attachments_retry_max_attempts() -> usize {
    3
}

const fn default_provider_attachments_retry_initial_backoff_ms() -> u64 {
    200
}

const fn default_provider_attachments_retry_max_backoff_ms() -> u64 {
    2_000
}

const fn default_provider_attachments_retry_jitter_ms() -> u64 {
    80
}

const fn default_provider_attachments_circuit_breaker_failure_threshold() -> u32 {
    5
}

const fn default_provider_attachments_circuit_breaker_open_ms() -> u64 {
    30_000
}

const fn default_gateway_artifacts_max_file_bytes() -> u64 {
    512 * 1024 * 1024
}

const fn default_gateway_artifacts_max_workspace_bytes() -> u64 {
    10 * 1024 * 1024 * 1024
}

const fn default_gateway_artifacts_max_files_per_workspace() -> u64 {
    100_000
}

const fn default_gateway_artifacts_upload_session_ttl_secs() -> u64 {
    60 * 60
}

const fn default_gateway_artifacts_gc_grace_secs() -> u64 {
    24 * 60 * 60
}

const fn default_gateway_artifacts_output_dir_ttl_secs() -> u64 {
    24 * 60 * 60
}

const fn default_gateway_artifacts_readable_copy_ttl_secs() -> u64 {
    24 * 60 * 60
}

const fn default_gateway_artifacts_quota_warn_at_percent() -> u8 {
    80
}

const fn default_gateway_artifacts_http_streams_global() -> usize {
    32
}

const fn default_gateway_artifacts_http_streams_per_principal() -> usize {
    8
}

const fn default_gateway_artifacts_http_streams_per_session() -> usize {
    4
}

const fn default_gateway_artifacts_http_open_handles() -> usize {
    32
}

const fn default_gateway_artifacts_http_max_single_range_bytes() -> u64 {
    128 * 1024 * 1024
}

const fn default_gateway_artifacts_http_tiny_range_bytes() -> u64 {
    4 * 1024
}

const fn default_gateway_artifacts_http_tiny_range_window_secs() -> u64 {
    10
}

const fn default_gateway_artifacts_http_tiny_range_max_requests() -> usize {
    32
}

const fn default_gateway_artifacts_http_open_timeout_secs() -> u64 {
    15
}

const fn default_gateway_artifacts_http_body_idle_timeout_secs() -> u64 {
    30
}

const fn default_gateway_artifacts_view_grant_ttl_secs() -> u64 {
    3 * 60
}

const fn default_gateway_artifacts_view_grants_global() -> usize {
    4_096
}

const fn default_gateway_artifacts_view_grants_per_session() -> usize {
    16
}

const fn default_gateway_artifacts_view_grant_streams() -> usize {
    4
}

const fn default_skills_enabled() -> bool {
    true
}

const fn default_skills_max_skills_per_source() -> usize {
    256
}

const fn default_skills_max_skill_file_bytes() -> usize {
    1024 * 1024
}

const fn default_skills_prompt_max_chars() -> usize {
    24_000
}

const fn default_skills_allow_implicit_invocation() -> bool {
    false
}

const fn default_skills_validation_strict_agentskills() -> bool {
    true
}

const fn default_skills_validation_accept_openclaw_profile() -> bool {
    true
}

const fn default_skills_security_allow_untrusted_install() -> bool {
    true
}

fn default_skills_security_min_trust_for_shell_tools() -> String {
    "untrusted".to_owned()
}

fn default_skills_security_min_trust_for_http_tools() -> String {
    "untrusted".to_owned()
}

fn default_skills_security_min_trust_for_function_proxy_tools() -> String {
    "untrusted".to_owned()
}

const fn default_skills_security_max_install_archive_compressed_bytes() -> usize {
    10 * 1024 * 1024
}

const fn default_skills_security_max_install_archive_uncompressed_bytes() -> usize {
    50 * 1024 * 1024
}

const fn default_skills_security_max_install_archive_entries() -> usize {
    2048
}

const fn default_skills_security_max_install_file_bytes() -> usize {
    1024 * 1024
}

const fn default_skills_security_upload_ttl_secs() -> u64 {
    3600
}

const fn default_skills_security_upload_recommended_chunk_size_bytes() -> usize {
    256 * 1024
}

const fn default_skills_security_upload_max_chunk_size_bytes() -> usize {
    1024 * 1024
}

const fn default_skills_dependencies_preflight_on_resolve() -> bool {
    true
}

const fn default_skills_dependencies_runtime_recheck_on_tool_call() -> bool {
    true
}

fn default_skills_system_paths() -> Vec<String> {
    Vec::new()
}

fn default_skills_user_paths() -> Vec<String> {
    vec!["{homeDirectory}/skills/workspace/{workspaceId}/user".to_owned()]
}

fn default_skills_registry_paths() -> Vec<String> {
    vec!["{homeDirectory}/skills/workspace/{workspaceId}/registry".to_owned()]
}

const fn default_skills_runtime_enable_dynamic_tools() -> bool {
    true
}

const fn default_skills_runtime_enable_read_skill() -> bool {
    true
}

const fn default_skills_runtime_max_dynamic_tools_per_skill() -> usize {
    64
}

const fn default_skills_runtime_read_skill_max_chars() -> usize {
    72_000
}

const fn default_skills_runtime_compact_mode_threshold() -> usize {
    6
}

const fn default_skills_runtime_allow_shell_tools() -> bool {
    true
}

const fn default_skills_runtime_allow_http_tools() -> bool {
    true
}

const fn default_skills_runtime_allow_function_proxy_tools() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayDatabaseConfig {
    pub file_name: String,
    pub max_connections: u32,
    pub connect_timeout_ms: u64,
    pub acquire_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub sqlx_logging: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct GatewayTelemetryConfig {
    #[serde(default = "default_gateway_telemetry_enabled")]
    pub enabled: bool,
    #[serde(default = "default_gateway_telemetry_otlp_metrics_endpoint")]
    pub otlp_metrics_endpoint: String,
    #[serde(default = "default_gateway_telemetry_otlp_traces_endpoint")]
    pub otlp_traces_endpoint: String,
    #[serde(default = "default_gateway_telemetry_export_interval_ms")]
    pub export_interval_ms: u64,
    #[serde(default = "default_gateway_telemetry_export_timeout_ms")]
    pub export_timeout_ms: u64,
}

impl Default for GatewayTelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: default_gateway_telemetry_enabled(),
            otlp_metrics_endpoint: default_gateway_telemetry_otlp_metrics_endpoint(),
            otlp_traces_endpoint: default_gateway_telemetry_otlp_traces_endpoint(),
            export_interval_ms: default_gateway_telemetry_export_interval_ms(),
            export_timeout_ms: default_gateway_telemetry_export_timeout_ms(),
        }
    }
}

const fn default_gateway_telemetry_enabled() -> bool {
    true
}

fn default_gateway_telemetry_otlp_metrics_endpoint() -> String {
    "https://telemetry.getpioneer.dev/v1/metrics".to_owned()
}

fn default_gateway_telemetry_otlp_traces_endpoint() -> String {
    "https://telemetry.getpioneer.dev/v1/traces".to_owned()
}

const fn default_gateway_telemetry_export_interval_ms() -> u64 {
    30_000
}

const fn default_gateway_telemetry_export_timeout_ms() -> u64 {
    3_000
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayAuthConfig {
    pub jwt_issuer: String,
    pub jwt_audience: String,
    pub secret_size_bytes: usize,
    pub token_refresh_leeway_seconds: u64,
    #[serde(default = "default_gateway_access_token_ttl_seconds")]
    pub access_token_ttl_seconds: u64,
    #[serde(default = "default_gateway_refresh_token_ttl_seconds")]
    pub refresh_token_ttl_seconds: u64,
    #[serde(default = "default_gateway_device_activation_code_ttl_seconds")]
    pub device_activation_code_ttl_seconds: u64,
    #[serde(default = "default_gateway_auth_exchange_timeout_seconds")]
    pub auth_exchange_timeout_seconds: u64,
    #[serde(default = "default_gateway_auth_database_acquire_timeout_ms")]
    pub database_acquire_timeout_ms: u64,
}

const fn default_gateway_access_token_ttl_seconds() -> u64 {
    15 * 60
}

const fn default_gateway_refresh_token_ttl_seconds() -> u64 {
    90 * 24 * 60 * 60
}

const fn default_gateway_device_activation_code_ttl_seconds() -> u64 {
    10 * 60
}

const fn default_gateway_auth_exchange_timeout_seconds() -> u64 {
    15
}

const fn default_gateway_auth_database_acquire_timeout_ms() -> u64 {
    500
}

const MAX_GATEWAY_REFRESH_TOKEN_TTL_SECONDS: u64 = 365 * 24 * 60 * 60;

impl GatewayAuthConfig {
    pub fn validate_session_security(&self) -> Result<()> {
        if self.jwt_issuer.trim().is_empty() || self.jwt_audience.trim().is_empty() {
            bail!("gateway.auth JWT issuer and audience must not be empty");
        }
        if self.secret_size_bytes < 32 {
            bail!("gateway.auth.secret_size_bytes must be at least 32");
        }
        if !(60..=3_600).contains(&self.access_token_ttl_seconds) {
            bail!("gateway.auth.access_token_ttl_seconds must be between 60 and 3600");
        }
        if self.refresh_token_ttl_seconds <= self.access_token_ttl_seconds {
            bail!("gateway.auth.refresh_token_ttl_seconds must exceed access_token_ttl_seconds");
        }
        if self.refresh_token_ttl_seconds > MAX_GATEWAY_REFRESH_TOKEN_TTL_SECONDS {
            bail!("gateway.auth.refresh_token_ttl_seconds must not exceed 365 days");
        }
        if !(60..=3_600).contains(&self.device_activation_code_ttl_seconds) {
            bail!("gateway.auth.device_activation_code_ttl_seconds must be between 60 and 3600");
        }
        if !(1..=60).contains(&self.auth_exchange_timeout_seconds) {
            bail!("gateway.auth.auth_exchange_timeout_seconds must be between 1 and 60");
        }
        if !(50..=5_000).contains(&self.database_acquire_timeout_ms) {
            bail!("gateway.auth.database_acquire_timeout_ms must be between 50 and 5000");
        }
        if self.token_refresh_leeway_seconds == 0
            || self.token_refresh_leeway_seconds >= self.access_token_ttl_seconds
        {
            bail!(
                "gateway.auth.token_refresh_leeway_seconds must be positive and less than access_token_ttl_seconds"
            );
        }
        Ok(())
    }
}

impl Default for GatewayAuthConfig {
    fn default() -> Self {
        Self {
            jwt_issuer: "pioneer".to_owned(),
            jwt_audience: "pioneer-clients".to_owned(),
            secret_size_bytes: 64,
            token_refresh_leeway_seconds: 300,
            access_token_ttl_seconds: default_gateway_access_token_ttl_seconds(),
            refresh_token_ttl_seconds: default_gateway_refresh_token_ttl_seconds(),
            device_activation_code_ttl_seconds: default_gateway_device_activation_code_ttl_seconds(
            ),
            auth_exchange_timeout_seconds: default_gateway_auth_exchange_timeout_seconds(),
            database_acquire_timeout_ms: default_gateway_auth_database_acquire_timeout_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DesktopConfig {
    pub gateway: GatewayRuntimeConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayRuntimeConfig {
    pub connect_timeout_ms: u64,
    pub startup_timeout_ms: u64,
    pub poll_interval_ms: u64,
    pub ws_ping_interval_ms: u64,
    pub ws_pong_timeout_ms: u64,
    pub ws_reconnect_initial_ms: u64,
    pub ws_reconnect_max_ms: u64,
    pub ws_reconnect_jitter_percent: u8,
    pub registry_file_name: String,
    pub local_gateway_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstallManagedBy {
    Script,
    Desktop,
    Manual,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallState {
    pub version: u32,
    pub managed_by: InstallManagedBy,
    pub installed_version: String,
    pub install_root: Option<PathBuf>,
    pub binary_path: PathBuf,
    pub updated_at_unix: u64,
}

impl InstallState {
    pub const CURRENT_VERSION: u32 = 1;
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        load_config_from_sources(DEFAULT_CONFIG_TOML, config_override_candidates())
    }

    pub fn runtime_home_dir(&self) -> Result<PathBuf> {
        let home_dir = dirs::home_dir().context("failed to resolve current user home directory")?;
        let configured = self.home_directory.trim();

        if configured.is_empty() {
            bail!("home_directory in config must not be empty");
        }

        let configured_path = Path::new(configured);
        if configured_path.is_absolute() {
            bail!("home_directory in config must be relative, got `{configured}`");
        }

        if configured_path.components().any(is_disallowed_component) {
            bail!(
                "home_directory in config must not contain parent or root components, got `{configured}`"
            );
        }

        Ok(home_dir.join(configured_path))
    }

    pub fn ensure_runtime_home_dir(&self) -> Result<PathBuf> {
        let runtime_home = self.runtime_home_dir()?;
        std::fs::create_dir_all(&runtime_home).with_context(|| {
            format!(
                "failed to create runtime home directory `{}`",
                runtime_home.display()
            )
        })?;
        Ok(runtime_home)
    }

    pub fn install_state_path(&self) -> Result<PathBuf> {
        let file_name = normalize_runtime_file_name(
            self.install_state_file_name.as_str(),
            "install_state_file_name",
        )?;
        Ok(self.runtime_home_dir()?.join(file_name))
    }

    pub fn install_root_directory_name(&self) -> Result<String> {
        #[cfg(windows)]
        {
            normalize_runtime_leaf_name(
                self.install.windows_root_directory_name.as_str(),
                "install.windows_root_directory_name",
            )
        }

        #[cfg(target_os = "macos")]
        {
            normalize_runtime_leaf_name(
                self.install.macos_root_directory_name.as_str(),
                "install.macos_root_directory_name",
            )
        }

        #[cfg(not(any(windows, target_os = "macos")))]
        {
            normalize_runtime_leaf_name(
                self.install.unix_root_directory_name.as_str(),
                "install.unix_root_directory_name",
            )
        }
    }

    pub fn install_managed_directory_name(&self) -> Result<String> {
        normalize_runtime_leaf_name(
            self.install.managed_directory_name.as_str(),
            "install.managed_directory_name",
        )
    }

    pub fn install_binary_file_name(&self) -> Result<String> {
        executable_file_name(self.install.binary_name.as_str(), "install.binary_name")
    }

    pub fn install_staged_binary_file_name(&self) -> Result<String> {
        executable_file_name_with_suffix(self.install.binary_name.as_str(), "new")
    }

    pub fn install_rollback_binary_file_name(&self) -> Result<String> {
        executable_file_name_with_suffix(self.install.binary_name.as_str(), "rollback")
    }

    pub fn install_command_file_name(&self) -> Result<String> {
        executable_file_name(self.install.command_name.as_str(), "install.command_name")
    }
}

fn is_disallowed_component(component: Component<'_>) -> bool {
    matches!(
        component,
        Component::ParentDir | Component::RootDir | Component::Prefix(_)
    )
}

fn normalize_runtime_file_name(value: &str, field_name: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{field_name} in config must not be empty");
    }

    let path = Path::new(trimmed);
    if path.is_absolute() {
        bail!("{field_name} in config must be relative");
    }

    if path.components().any(is_disallowed_component) {
        bail!("{field_name} in config must not contain parent or root components");
    }

    Ok(trimmed.to_owned())
}

fn normalize_runtime_dir_path(value: &str, field_name: &str) -> Result<PathBuf> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{field_name} in config must not be empty");
    }
    if trimmed.contains('\\') {
        bail!("{field_name} in config must use `/` as path separator");
    }

    let path = Path::new(trimmed);
    if path.is_absolute() {
        bail!("{field_name} in config must be relative");
    }
    if path.components().any(is_disallowed_component) {
        bail!("{field_name} in config must not contain parent or root components");
    }
    if !path
        .components()
        .any(|component| matches!(component, Component::Normal(_)))
    {
        bail!("{field_name} in config must contain at least one directory component");
    }

    Ok(path.to_path_buf())
}

fn normalize_runtime_leaf_name(value: &str, field_name: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{field_name} in config must not be empty");
    }

    if trimmed.contains('/') || trimmed.contains('\\') {
        bail!("{field_name} in config must not contain path separators");
    }

    let path = Path::new(trimmed);
    if path.is_absolute() {
        bail!("{field_name} in config must be relative");
    }

    if path.components().any(is_disallowed_component) {
        bail!("{field_name} in config must not contain parent or root components");
    }

    Ok(trimmed.to_owned())
}

fn executable_file_name(value: &str, field_name: &str) -> Result<String> {
    let name = normalize_runtime_leaf_name(value, field_name)?;

    #[cfg(windows)]
    {
        if name.to_ascii_lowercase().ends_with(".exe") {
            Ok(name)
        } else {
            Ok(format!("{name}.exe"))
        }
    }

    #[cfg(not(windows))]
    {
        Ok(name)
    }
}

fn executable_file_name_with_suffix(value: &str, suffix: &str) -> Result<String> {
    let name = normalize_runtime_leaf_name(value, "install.binary_name")?;

    #[cfg(windows)]
    {
        let stem = strip_windows_exe_suffix(name.as_str());
        Ok(format!("{stem}.{suffix}.exe"))
    }

    #[cfg(not(windows))]
    {
        Ok(format!("{name}.{suffix}"))
    }
}

#[cfg(windows)]
fn strip_windows_exe_suffix(value: &str) -> &str {
    if value.len() >= 4 && value[value.len() - 4..].eq_ignore_ascii_case(".exe") {
        &value[..value.len() - 4]
    } else {
        value
    }
}

pub fn load_install_state(path: &Path) -> Result<Option<InstallState>> {
    if !path.exists() {
        return Ok(None);
    }

    let path_display = path.display().to_string();
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read install state `{path_display}`"))?;
    let state = toml::from_str::<InstallState>(&content)
        .with_context(|| format!("failed to parse install state `{path_display}`"))?;

    if state.version != InstallState::CURRENT_VERSION {
        bail!(
            "unsupported install state version `{}` in `{}`; expected `{}`",
            state.version,
            path.display(),
            InstallState::CURRENT_VERSION
        );
    }

    Ok(Some(state))
}

pub fn save_install_state(path: &Path, state: &InstallState) -> Result<()> {
    if state.version != InstallState::CURRENT_VERSION {
        bail!(
            "install state version must be `{}`",
            InstallState::CURRENT_VERSION
        );
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create install state parent directory `{}`",
                parent.display()
            )
        })?;
    }

    let content =
        toml::to_string_pretty(state).context("failed to serialize install state contents")?;
    std::fs::write(path, content)
        .with_context(|| format!("failed to write install state `{}`", path.display()))?;

    Ok(())
}

fn config_override_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    // Workspace-local override is for developer workflow only.
    // Production builds should not pick up repository-local config/local.toml.
    if cfg!(debug_assertions) {
        candidates.push(workspace_local_config_path());
    }

    if let Some(path) = user_override_config_path() {
        candidates.push(path);
    }

    if let Some(path) = std::env::var_os("PIONEER_CONFIG").map(PathBuf::from) {
        candidates.push(path);
    }

    candidates
}

fn load_config_from_sources(
    default_config_toml: &str,
    override_paths: Vec<PathBuf>,
) -> Result<AppConfig, ConfigError> {
    let mut builder =
        Config::builder().add_source(File::from_str(default_config_toml, FileFormat::Toml));

    for path in &override_paths {
        builder = builder.add_source(File::from(path.as_path()).required(false));
    }

    let mut config: AppConfig = builder.build()?.try_deserialize()?;
    migrate_gateway_preflight_model_from_legacy_overrides(&mut config, override_paths.as_slice());
    Ok(config)
}

fn migrate_gateway_preflight_model_from_legacy_overrides(
    config: &mut AppConfig,
    override_paths: &[PathBuf],
) {
    if override_paths_contain_toml_path(override_paths, &["gateway", "preflight_model"]) {
        return;
    }

    if override_paths_contain_toml_path(
        override_paths,
        &["gateway", "memory", "active_recall_model"],
    ) {
        config.gateway.preflight_model = config.gateway.memory.active_recall_model.clone();
    }
}

fn override_paths_contain_toml_path(override_paths: &[PathBuf], path: &[&str]) -> bool {
    override_paths.iter().any(|override_path| {
        let Ok(content) = std::fs::read_to_string(override_path) else {
            return false;
        };
        let Ok(value) = toml::from_str::<toml::Value>(content.as_str()) else {
            return false;
        };
        toml_value_has_path(&value, path)
    })
}

fn toml_value_has_path(value: &toml::Value, path: &[&str]) -> bool {
    if path.is_empty() {
        return true;
    }

    for end in 1..=path.len() {
        let key = path[..end].join(".");
        if let Some(next) = value.get(key.as_str())
            && toml_value_has_path(next, &path[end..])
        {
            return true;
        }
    }

    false
}

fn workspace_local_config_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("config")
        .join("local.toml")
}

fn user_override_config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|path| {
        path.join(user_config_directory_name())
            .join(USER_CONFIG_FILE_NAME)
    })
}

fn user_config_directory_name() -> &'static str {
    if cfg!(debug_assertions) {
        "pioneer-dev"
    } else {
        "pioneer"
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_CONFIG_TOML, GatewayCliAgentRuntimeConfig, GatewayCliAgentRuntimeKindConfig,
        GatewayMemoryConfig, GatewayMemoryModelSelectionConfig, GatewayResilienceConfig,
        GatewaySelfImprovementConfig, GatewayTelemetryConfig, InstallManagedBy, InstallState,
        load_config_from_sources, load_install_state, save_install_state,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn install_state_roundtrip() {
        let path = unique_temp_file_path("install-state");
        let install_root = std::env::temp_dir().join("pioneer-install-root");
        let state = InstallState {
            version: InstallState::CURRENT_VERSION,
            managed_by: InstallManagedBy::Manual,
            installed_version: "0.1.0".to_owned(),
            install_root: Some(install_root.clone()),
            binary_path: install_root.join("pioneer"),
            updated_at_unix: 1_700_000_000,
        };

        save_install_state(&path, &state).expect("save install state");
        let loaded = load_install_state(&path)
            .expect("load install state")
            .expect("install state should exist");

        assert_eq!(loaded, state);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn missing_install_state_returns_none() {
        let path = unique_temp_file_path("missing-install-state");
        let loaded = load_install_state(&path).expect("load install state should succeed");
        assert!(loaded.is_none());
    }

    #[test]
    fn gateway_telemetry_defaults_are_enabled_and_point_to_otlp_http() {
        let telemetry =
            toml::from_str::<GatewayTelemetryConfig>("").expect("telemetry defaults must parse");

        assert!(telemetry.enabled);
        assert_eq!(
            telemetry.otlp_metrics_endpoint,
            "https://telemetry.getpioneer.dev/v1/metrics"
        );
        assert_eq!(
            telemetry.otlp_traces_endpoint,
            "https://telemetry.getpioneer.dev/v1/traces"
        );
        assert_eq!(telemetry.export_interval_ms, 30_000);
        assert_eq!(telemetry.export_timeout_ms, 3_000);
    }

    #[test]
    fn config_loader_applies_workspace_override() {
        let workspace_override = unique_temp_file_path("workspace-local-config");
        write_file(
            &workspace_override,
            r#"
            home_directory = ".pioneer.local.test"
            [gateway]
            service_name = "com.pioneer.gateway.local.test"
            "#,
        );

        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, vec![workspace_override.clone()])
                .expect("load config with workspace override");

        assert_eq!(config.home_directory, ".pioneer.local.test");
        assert_eq!(config.install_state_file_name, "install-state.toml");
        assert_eq!(
            config.install_binary_file_name().unwrap(),
            expected_executable_name("pioneer")
        );
        assert_eq!(
            config.install_command_file_name().unwrap(),
            expected_executable_name("pioneer")
        );
        assert_eq!(
            config.gateway.service_name,
            "com.pioneer.gateway.local.test"
        );

        let _ = fs::remove_file(workspace_override);
    }

    #[test]
    fn config_loader_uses_last_override_as_highest_priority() {
        let workspace_override = unique_temp_file_path("workspace-config");
        let user_override = unique_temp_file_path("user-config");
        let env_override = unique_temp_file_path("env-config");

        write_file(
            &workspace_override,
            r#"
[gateway]
service_name = "com.pioneer.gateway.workspace"
"#,
        );
        write_file(
            &user_override,
            r#"
[gateway]
service_name = "com.pioneer.gateway.user"
"#,
        );
        write_file(
            &env_override,
            r#"
[gateway]
service_name = "com.pioneer.gateway.env"
"#,
        );

        let config = load_config_from_sources(
            DEFAULT_CONFIG_TOML,
            vec![
                workspace_override.clone(),
                user_override.clone(),
                env_override.clone(),
            ],
        )
        .expect("load config with layered overrides");

        assert_eq!(config.gateway.service_name, "com.pioneer.gateway.env");

        let _ = fs::remove_file(workspace_override);
        let _ = fs::remove_file(user_override);
        let _ = fs::remove_file(env_override);
    }

    #[test]
    fn config_loader_works_without_override_files() {
        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, Vec::new()).expect("load default config");

        assert_eq!(config.home_directory, ".pioneer");
        assert_eq!(config.install_state_file_name, "install-state.toml");
        assert_eq!(
            config.install_root_directory_name().unwrap(),
            expected_install_root_directory_name()
        );
        assert_eq!(config.install_managed_directory_name().unwrap(), "managed");
        assert_eq!(
            config.install_binary_file_name().unwrap(),
            expected_executable_name("pioneer")
        );
        assert_eq!(
            config.install_staged_binary_file_name().unwrap(),
            expected_executable_name("pioneer.new")
        );
        assert_eq!(
            config.install_rollback_binary_file_name().unwrap(),
            expected_executable_name("pioneer.rollback")
        );
        assert_eq!(
            config.install_command_file_name().unwrap(),
            expected_executable_name("pioneer")
        );
        assert_eq!(config.gateway.service_name, "com.pioneer.gateway");
        assert!(!config.gateway.keepawake);
        assert!(config.gateway.preflight_model.is_thread_model());
        assert!(config.gateway.tasks.review.enabled);
        assert!(!config.gateway.tasks.review.allow_task_create_review_policy);
        assert!(
            config
                .gateway
                .tasks
                .review
                .default_parent_review_for_immediate_attached_agent_tasks
        );
        assert_eq!(config.gateway.tasks.review.default_max_revision_rounds, 5);
        assert_eq!(config.gateway.tasks.review.auto_accept_after_seconds, 300);
        assert!(!config.gateway.cli_agent_runtime.enabled);
        assert_eq!(config.gateway.cli_agent_runtime.mcp_tools.max_tools, 512);
        assert_eq!(
            config
                .gateway
                .cli_agent_runtime
                .mcp_tools
                .max_total_schema_bytes,
            3_145_728
        );
        assert_eq!(
            config
                .gateway
                .cli_agent_runtime
                .mcp_tools
                .max_concurrent_calls_per_turn,
            16
        );
        assert_eq!(
            config.gateway.cli_agent_runtime.idle_session_ttl_secs,
            1_800
        );
        assert_eq!(config.gateway.cli_agent_runtime.startup_timeout_ms, 30_000);
        assert_eq!(config.gateway.cli_agent_runtime.request_timeout_ms, 120_000);
        assert_eq!(
            config.gateway.cli_agent_runtime.event_channel_capacity,
            2_048
        );
        assert_eq!(config.gateway.cli_agent_runtime.stderr_ring_lines, 200);
        assert!(!config.gateway.cli_agent_runtime.debug_native_events);
        assert!(config.gateway.cli_agent_runtimes.is_empty());
        assert!(
            config
                .gateway
                .effective_cli_agent_runtime_instances()
                .is_empty()
        );
    }

    #[test]
    fn gateway_cli_agent_runtime_config_defaults_are_runtime_disabled_and_mcp_bounded() {
        let config = GatewayCliAgentRuntimeConfig::default();

        assert!(!config.enabled);
        assert_eq!(config.mcp_tools.max_tools, 512);
        assert_eq!(config.mcp_tools.max_total_schema_bytes, 3_145_728);
        assert_eq!(config.mcp_tools.max_concurrent_calls_per_turn, 16);
        assert_eq!(config.idle_session_ttl_secs, 1_800);
        assert_eq!(config.startup_timeout_ms, 30_000);
        assert_eq!(config.request_timeout_ms, 120_000);
        assert_eq!(config.event_channel_capacity, 2_048);
        assert_eq!(config.stderr_ring_lines, 200);
        assert!(!config.debug_native_events);
    }

    #[test]
    fn gateway_cli_agent_runtime_config_accepts_override() {
        let workspace_override = unique_temp_file_path("gateway-cli-agent-runtime-config");
        write_file(
            &workspace_override,
            r#"
[gateway.cli_agent_runtime]
enabled = true
idle_session_ttl_secs = 60
startup_timeout_ms = 1000
request_timeout_ms = 2000
event_channel_capacity = 64
stderr_ring_lines = 10
debug_native_events = true
"#,
        );

        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, vec![workspace_override.clone()])
                .expect("load config with cli agent runtime override");

        assert!(config.gateway.cli_agent_runtime.enabled);
        assert_eq!(config.gateway.cli_agent_runtime.idle_session_ttl_secs, 60);
        assert_eq!(config.gateway.cli_agent_runtime.startup_timeout_ms, 1_000);
        assert_eq!(config.gateway.cli_agent_runtime.request_timeout_ms, 2_000);
        assert_eq!(config.gateway.cli_agent_runtime.event_channel_capacity, 64);
        assert_eq!(config.gateway.cli_agent_runtime.stderr_ring_lines, 10);
        assert!(config.gateway.cli_agent_runtime.debug_native_events);

        let _ = fs::remove_file(workspace_override);
    }

    #[test]
    fn gateway_cli_agent_runtime_cli_mcp_limits_apply_to_every_runtime() {
        let workspace_override = unique_temp_file_path("gateway-cli-agent-runtime-mcp-limits");
        write_file(
            &workspace_override,
            r#"
[gateway.cli_agent_runtime]
enabled = true

[gateway.cli_agent_runtime.mcp_tools]
max_tools = 64
max_total_schema_bytes = 524288
max_concurrent_calls_per_turn = 4

[gateway.cli_agent_runtimes.codex]
kind = "codex"

[gateway.cli_agent_runtimes.claude]
kind = "claude"
"#,
        );

        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, vec![workspace_override.clone()])
                .expect("load config with CLI MCP limits");
        assert_eq!(config.gateway.cli_agent_runtime.mcp_tools.max_tools, 64);
        assert_eq!(
            config
                .gateway
                .cli_agent_runtime
                .mcp_tools
                .max_total_schema_bytes,
            524_288
        );
        assert_eq!(
            config
                .gateway
                .cli_agent_runtime
                .mcp_tools
                .max_concurrent_calls_per_turn,
            4
        );

        assert_eq!(
            config.gateway.effective_cli_agent_runtime_instances().len(),
            2
        );

        let _ = fs::remove_file(workspace_override);
    }

    #[test]
    fn gateway_cli_agent_runtime_cli_mcp_omitted_and_zero_values_use_current_defaults() {
        let omitted = toml::from_str::<GatewayCliAgentRuntimeConfig>("enabled = true")
            .expect("CLI runtime config with omitted MCP limits should deserialize");
        assert_eq!(omitted.mcp_tools.max_tools, 512);

        let normalized = toml::from_str::<GatewayCliAgentRuntimeConfig>(
            r#"
enabled = true

[mcp_tools]
max_tools = 0
max_total_schema_bytes = 0
max_concurrent_calls_per_turn = 0
"#,
        )
        .expect("CLI MCP limits should deserialize");
        assert_eq!(normalized.mcp_tools.max_tools, 512);
        assert_eq!(normalized.mcp_tools.max_total_schema_bytes, 3_145_728);
        assert_eq!(normalized.mcp_tools.max_concurrent_calls_per_turn, 16);
    }

    #[test]
    fn gateway_cli_agent_runtime_default_instances_load_when_runtime_enabled() {
        let workspace_override = unique_temp_file_path("gateway-cli-agent-runtime-default-codex");
        write_file(
            &workspace_override,
            r#"
[gateway.cli_agent_runtime]
enabled = true
startup_timeout_ms = 15000
request_timeout_ms = 60000
"#,
        );

        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, vec![workspace_override.clone()])
                .expect("load config with default CLI runtimes enabled");
        let instances = config.gateway.effective_cli_agent_runtime_instances();

        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].id, "codex");
        assert_eq!(instances[0].kind, GatewayCliAgentRuntimeKindConfig::Codex);
        assert_eq!(instances[0].display_name, "Codex");
        assert!(instances[0].enabled);
        assert_eq!(instances[0].binary_path, "codex");
        assert_eq!(instances[0].home_path, "~/.codex");
        assert_eq!(instances[0].startup_probe_timeout_ms, 15_000);
        assert_eq!(instances[0].request_timeout_ms, 60_000);
        assert_eq!(instances[1].id, "claude");
        assert_eq!(instances[1].kind, GatewayCliAgentRuntimeKindConfig::Claude);
        assert_eq!(instances[1].display_name, "Claude");
        assert!(instances[1].enabled);
        assert_eq!(instances[1].binary_path, "claude");
        assert_eq!(instances[1].home_path, "~/.claude");
        assert_eq!(instances[1].startup_probe_timeout_ms, 15_000);
        assert_eq!(instances[1].request_timeout_ms, 60_000);

        let _ = fs::remove_file(workspace_override);
    }

    #[test]
    fn gateway_cli_agent_runtime_config_loads_one_codex_instance() {
        let workspace_override = unique_temp_file_path("gateway-cli-agent-runtime-one-instance");
        write_file(
            &workspace_override,
            r#"
[gateway.cli_agent_runtime]
enabled = true
startup_timeout_ms = 15000
request_timeout_ms = 60000

[gateway.cli_agent_runtimes.codex_personal]
kind = "codex"
display_name = "Codex Personal"
enabled = true
binary_path = "codex"
home_path = "~/.codex"
shadow_home_path = "~/.pioneer/codex/personal"
custom_models = ["gpt-5.4-codex"]
app_server_args = ["--experimental"]
startup_probe_timeout_ms = 5000
request_timeout_ms = 45000
"#,
        );

        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, vec![workspace_override.clone()])
                .expect("load config with one codex runtime instance");
        assert_eq!(config.gateway.cli_agent_runtimes.instances.len(), 1);

        let instances = config.gateway.effective_cli_agent_runtime_instances();
        assert_eq!(instances.len(), 3);
        let instance = &instances[0];
        assert_eq!(instance.id, "codex_personal");
        assert_eq!(instance.kind, GatewayCliAgentRuntimeKindConfig::Codex);
        assert_eq!(instance.display_name, "Codex Personal");
        assert!(instance.enabled);
        assert_eq!(instance.binary_path, "codex");
        assert_eq!(instance.home_path, "~/.codex");
        assert_eq!(
            instance.shadow_home_path.as_deref(),
            Some("~/.pioneer/codex/personal")
        );
        assert_eq!(instance.custom_models, vec!["gpt-5.4-codex"]);
        assert_eq!(instance.app_server_args, vec!["--experimental"]);
        assert_eq!(instance.startup_probe_timeout_ms, 5_000);
        assert_eq!(instance.request_timeout_ms, 45_000);
        assert_eq!(instance.idle_session_ttl_secs, 1_800);
        assert_eq!(instances[1].id, "codex");
        assert_eq!(instances[2].id, "claude");

        let _ = fs::remove_file(workspace_override);
    }

    #[test]
    fn gateway_cli_agent_runtime_config_loads_multiple_instances_and_keeps_disabled_visible() {
        let workspace_override =
            unique_temp_file_path("gateway-cli-agent-runtime-multiple-instances");
        write_file(
            &workspace_override,
            r#"
[gateway.cli_agent_runtime]
enabled = true
request_timeout_ms = 90000

[gateway.cli_agent_runtimes."Codex Personal"]
kind = "codex"
home_path = "~/.codex"
shadow_home_path = "~/.pioneer/codex/personal"

[gateway.cli_agent_runtimes.codex-work]
kind = "codex"
display_name = "Codex Work"
enabled = false
home_path = "~/.codex-work"
shadow_home_path = "~/.pioneer/codex/work"
request_timeout_ms = 0
"#,
        );

        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, vec![workspace_override.clone()])
                .expect("load config with multiple codex runtime instances");
        let instances = config.gateway.effective_cli_agent_runtime_instances();

        assert_eq!(instances.len(), 4);
        assert_eq!(instances[0].id, "codex_personal");
        assert_eq!(instances[0].display_name, "Codex Personal");
        assert!(instances[0].enabled);
        assert_eq!(instances[0].request_timeout_ms, 90_000);

        assert_eq!(instances[1].id, "codex_work");
        assert_eq!(instances[1].display_name, "Codex Work");
        assert!(!instances[1].enabled);
        assert_eq!(instances[1].home_path, "~/.codex-work");
        assert_eq!(instances[1].request_timeout_ms, 120_000);
        assert_eq!(instances[2].id, "codex");
        assert_eq!(instances[3].id, "claude");

        let _ = fs::remove_file(workspace_override);
    }

    #[test]
    fn gateway_cli_agent_runtime_config_normalizes_zero_limits() {
        let config = toml::from_str::<GatewayCliAgentRuntimeConfig>(
            r#"
enabled = true
idle_session_ttl_secs = 0
startup_timeout_ms = 0
request_timeout_ms = 0
event_channel_capacity = 0
stderr_ring_lines = 0
debug_native_events = true

[command_heartbeat]
interval_secs = 0
"#,
        )
        .expect("cli agent runtime config should deserialize with normalized limits");

        assert!(config.enabled);
        assert_eq!(config.idle_session_ttl_secs, 1_800);
        assert_eq!(config.startup_timeout_ms, 30_000);
        assert_eq!(config.request_timeout_ms, 120_000);
        assert_eq!(config.event_channel_capacity, 2_048);
        assert_eq!(config.stderr_ring_lines, 200);
        assert!(config.debug_native_events);
        assert_eq!(config.command_heartbeat.interval_secs, 60);
    }

    #[test]
    fn gateway_resilience_config_normalizes_zero_command_execution_timeouts() {
        let config = toml::from_str::<GatewayResilienceConfig>(
            r#"
[command_execution]
lease_secs = 0
idle_secs = 0
hard_secs = 0
recovery_max_wall_clock_secs = 0
"#,
        )
        .expect("gateway resilience config should deserialize with normalized timeouts");

        assert_eq!(config.command_execution.lease_secs, 600);
        assert_eq!(config.command_execution.idle_secs, 1_800);
        assert_eq!(config.command_execution.hard_secs, 3_600);
        assert_eq!(config.command_execution.recovery_max_wall_clock_secs, 3_600);
    }

    #[test]
    fn gateway_resilience_config_normalizes_zero_provider_stream_item_timeouts() {
        let config = toml::from_str::<GatewayResilienceConfig>(
            r#"
[provider_stream_items]
lease_secs = 0
idle_secs = 0
hard_secs = 0
"#,
        )
        .expect(
            "gateway resilience config should deserialize with normalized stream item timeouts",
        );

        assert_eq!(config.provider_stream_items.lease_secs, 960);
        assert_eq!(config.provider_stream_items.idle_secs, 900);
        assert_eq!(config.provider_stream_items.hard_secs, 1_800);
    }

    #[test]
    fn gateway_resilience_config_preserves_valid_context_compaction_policy() {
        let config = toml::from_str::<GatewayResilienceConfig>(
            r#"
[context_compaction]
lease_secs = 120
idle_secs = 300
hard_secs = 1800
recovery_grace_secs = 600
"#,
        )
        .expect("valid context compaction policy should deserialize");

        assert_eq!(config.context_compaction.lease_secs, 120);
        assert_eq!(config.context_compaction.idle_secs, 300);
        assert_eq!(config.context_compaction.hard_secs, 1_800);
        assert_eq!(config.context_compaction.recovery_grace_secs, 600);
    }

    #[test]
    fn gateway_resilience_config_rejects_unbounded_context_compaction_policy() {
        for invalid in [
            r#"
[context_compaction]
lease_secs = 0
idle_secs = 300
hard_secs = 1800
recovery_grace_secs = 600
"#,
            r#"
[context_compaction]
lease_secs = 120
idle_secs = 300
hard_secs = 100
recovery_grace_secs = 60
"#,
            r#"
[context_compaction]
lease_secs = 120
idle_secs = 300
hard_secs = 1800
recovery_grace_secs = 1801
"#,
        ] {
            assert!(
                toml::from_str::<GatewayResilienceConfig>(invalid).is_err(),
                "invalid context compaction policy must fail closed: {invalid}"
            );
        }
    }

    #[test]
    fn settings_gateway_preflight_model_config_defaults_to_thread() {
        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, Vec::new()).expect("load default config");

        assert!(config.gateway.preflight_model.is_thread_model());
    }

    #[test]
    fn settings_gateway_preflight_model_config_accepts_custom_override() {
        let workspace_override = unique_temp_file_path("gateway-preflight-model-config");
        write_file(
            &workspace_override,
            r#"
[gateway]
preflight_model = { source = "custom", model_provider = "planner-provider", model = "planner-model" }
"#,
        );

        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, vec![workspace_override.clone()])
                .expect("load config with preflight model override");

        assert_eq!(
            config.gateway.preflight_model,
            GatewayMemoryModelSelectionConfig::custom("planner-provider", "planner-model")
        );

        let _ = fs::remove_file(workspace_override);
    }

    #[test]
    fn settings_migration_gateway_preflight_model_copies_legacy_memory_active_recall_model() {
        let workspace_override = unique_temp_file_path("gateway-preflight-model-migration");
        write_file(
            &workspace_override,
            r#"
[gateway.memory]
active_recall_model = { source = "custom", model_provider = "legacy-provider", model = "legacy-model" }
"#,
        );

        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, vec![workspace_override.clone()])
                .expect("load config with legacy active recall model");

        assert_eq!(
            config.gateway.preflight_model,
            GatewayMemoryModelSelectionConfig::custom("legacy-provider", "legacy-model")
        );
        assert_eq!(
            config.gateway.memory.active_recall_model,
            GatewayMemoryModelSelectionConfig::custom("legacy-provider", "legacy-model")
        );

        let _ = fs::remove_file(workspace_override);
    }

    #[test]
    fn migration_toml_path_detection_handles_nested_tables() {
        let content = r#"
[gateway.memory]
active_recall_model = { source = "custom", model_provider = "legacy-provider", model = "legacy-model" }
"#;
        let value = toml::from_str::<toml::Value>(content).expect("toml should parse");

        assert!(super::toml_value_has_path(
            &value,
            &["gateway", "memory", "active_recall_model"]
        ));
        assert!(!super::toml_value_has_path(
            &value,
            &["gateway", "preflight_model"]
        ));
    }

    #[test]
    fn settings_migration_gateway_preflight_model_preserves_explicit_new_setting() {
        let workspace_override = unique_temp_file_path("gateway-preflight-model-mixed");
        write_file(
            &workspace_override,
            r#"
[gateway]
preflight_model = { source = "custom", model_provider = "new-provider", model = "new-model" }

[gateway.memory]
active_recall_model = { source = "custom", model_provider = "legacy-provider", model = "legacy-model" }
"#,
        );

        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, vec![workspace_override.clone()])
                .expect("load config with mixed preflight settings");

        assert_eq!(
            config.gateway.preflight_model,
            GatewayMemoryModelSelectionConfig::custom("new-provider", "new-model")
        );
        assert_eq!(
            config.gateway.memory.active_recall_model,
            GatewayMemoryModelSelectionConfig::custom("legacy-provider", "legacy-model")
        );

        let _ = fs::remove_file(workspace_override);
    }

    #[test]
    fn config_loader_uses_scoped_skill_path_templates_by_default() {
        let config =
            load_config_from_sources(DEFAULT_CONFIG_TOML, Vec::new()).expect("load default config");

        assert!(config.gateway.skills.paths.system.is_empty());
        assert_eq!(
            config.gateway.skills.paths.user,
            vec!["{homeDirectory}/skills/workspace/{workspaceId}/user".to_owned()]
        );
        assert_eq!(
            config.gateway.skills.paths.registry,
            vec!["{homeDirectory}/skills/workspace/{workspaceId}/registry".to_owned()]
        );
        assert!(config.gateway.skills.runtime.enable_dynamic_tools);
        assert!(config.gateway.skills.runtime.enable_read_skill);
        assert_eq!(
            config.gateway.skills.runtime.max_dynamic_tools_per_skill,
            64
        );
        assert_eq!(config.gateway.skills.runtime.read_skill_max_chars, 72_000);
        assert_eq!(config.gateway.skills.runtime.compact_mode_threshold, 6);
        assert!(config.gateway.skills.runtime.allow_shell_tools);
        assert!(config.gateway.skills.runtime.allow_http_tools);
        assert!(config.gateway.skills.runtime.allow_function_proxy_tools);
        assert!(config.gateway.skills.validation.strict_agentskills);
        assert!(config.gateway.skills.validation.accept_openclaw_profile);
        assert!(config.gateway.skills.security.allow_untrusted_install);
        assert_eq!(
            config.gateway.skills.security.min_trust_for_shell_tools,
            "untrusted"
        );
        assert_eq!(
            config.gateway.skills.security.min_trust_for_http_tools,
            "untrusted"
        );
        assert_eq!(
            config
                .gateway
                .skills
                .security
                .min_trust_for_function_proxy_tools,
            "untrusted"
        );
        assert_eq!(
            config
                .gateway
                .skills
                .security
                .max_install_archive_compressed_bytes,
            10 * 1024 * 1024
        );
        assert_eq!(
            config
                .gateway
                .skills
                .security
                .max_install_archive_uncompressed_bytes,
            50 * 1024 * 1024
        );
        assert_eq!(
            config.gateway.skills.security.max_install_archive_entries,
            2048
        );
        assert_eq!(
            config.gateway.skills.security.max_install_file_bytes,
            1024 * 1024
        );
        assert_eq!(config.gateway.skills.security.upload_ttl_secs, 3600);
        assert_eq!(
            config
                .gateway
                .skills
                .security
                .upload_recommended_chunk_size_bytes,
            256 * 1024
        );
        assert_eq!(
            config.gateway.skills.security.upload_max_chunk_size_bytes,
            1024 * 1024
        );
        assert!(config.gateway.skills.dependencies.preflight_on_resolve);
        assert!(
            config
                .gateway
                .skills
                .dependencies
                .runtime_recheck_on_tool_call
        );
        assert_eq!(
            config.gateway.tools.computer_use.artifacts_subdir,
            "tools/computer_use"
        );
        assert_eq!(config.gateway.tools.computer_use.retention_hours, 24);
        assert_eq!(
            config.gateway.tools.computer_use.max_total_bytes,
            1024 * 1024 * 1024
        );
        assert_eq!(config.gateway.tools.computer_use.run_max_steps_default, 300);
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .snapshot_transport_max_bytes,
            8 * 1024 * 1024
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .snapshot_transport_max_side_px,
            1280
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .snapshot_transport_min_side_px,
            320
        );
        assert_eq!(
            config.gateway.tools.computer_use.snapshot_downscale_factor,
            0.85
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .accessibility_tree_max_depth,
            6
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .accessibility_tree_max_nodes,
            200
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .accessibility_tree_max_serialized_bytes,
            192 * 1024
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .accessibility_tree_text_max_chars,
            160
        );
        assert_eq!(
            config.gateway.tools.computer_use.semantic_action_timeout_ms,
            30_000
        );
        assert_eq!(
            config.gateway.tools.computer_use.app_activation_timeout_ms,
            5_000
        );
        assert!(config.gateway.tools.computer_use.input_simulation_enabled);
        assert!(!config.gateway.tools.computer_use.launch_if_missing_default);
        assert!(
            config
                .gateway
                .tools
                .computer_use
                .allowed_launch_commands
                .is_empty()
        );
        assert!(
            config
                .gateway
                .tools
                .computer_use
                .preflight_screenshot_probe_enabled
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .max_consecutive_same_snapshot_hash,
            6
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .max_consecutive_same_action_signature,
            8
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .max_consecutive_no_progress_steps,
            4
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .max_recovery_attempts_per_step,
            2
        );
        assert_eq!(
            config
                .gateway
                .tools
                .computer_use
                .max_recovery_attempts_per_run,
            12
        );
        assert_eq!(
            config.gateway.provider.attachments.max_bytes_per_attachment,
            100 * 1024 * 1024
        );
        assert_eq!(
            config
                .gateway
                .provider
                .attachments
                .max_total_bytes_per_request,
            200 * 1024 * 1024
        );
        assert_eq!(
            config
                .gateway
                .provider
                .attachments
                .max_attachments_per_request,
            64
        );
        assert!(config.gateway.provider.attachments.upload_registry_enabled);
        assert_eq!(
            config.gateway.provider.attachments.upload_registry_ttl_secs,
            7 * 24 * 3600
        );
        assert!(!config.gateway.provider.attachments.allow_url_sources);
        assert!(!config.gateway.provider.attachments.allow_http);
        assert!(!config.gateway.provider.attachments.allow_private_network);
        assert_eq!(config.gateway.provider.attachments.max_url_redirects, 3);
        assert_eq!(
            config.gateway.provider.attachments.url_fetch_timeout_ms,
            15_000
        );
        assert_eq!(
            config.gateway.provider.attachments.url_fetch_max_bytes,
            20 * 1024 * 1024
        );
        let execution_windows = config
            .gateway
            .tools
            .execution_windows
            .as_ref()
            .expect("default config should include execution_windows");
        assert_eq!(execution_windows.window.max_agent_rounds_per_window, 32);
        assert_eq!(execution_windows.window.max_tool_calls_per_window, 128);
        assert_eq!(
            execution_windows.window.max_wall_clock_ms_per_window,
            Some(1_800_000)
        );
        assert_eq!(execution_windows.total.max_windows_per_turn, None);
        assert_eq!(execution_windows.total.max_tool_calls_per_turn, None);
        assert!(config.gateway.memory.enabled);
        assert_eq!(config.gateway.memory.capsules_dir, "memory/capsules");
        assert!(config.gateway.memory.allow_global_user_by_default);
        assert!(!config.gateway.memory.allow_global_agent_by_default);
        assert!(config.gateway.memory.deterministic_recall_enabled);
        assert!(config.gateway.memory.active_recall_enabled);
        assert!(config.gateway.memory.tools_enabled);
        assert!(config.gateway.memory.proactive_writes_enabled);
        assert!(config.gateway.memory.background_extraction_enabled);
        assert!(config.gateway.memory.active_recall_model.is_thread_model());
        assert!(
            config
                .gateway
                .memory
                .proactive_writes_model
                .is_thread_model()
        );
        assert!(!config.gateway.memory.debug_trace_enabled);
        assert!(!config.gateway.memory.strict_diagnostics_enabled);
        assert_eq!(
            config
                .gateway
                .memory
                .resolve_capsules_root(PathBuf::from("/tmp/pioneer-runtime").as_path())
                .expect("memory capsules root should resolve"),
            PathBuf::from("/tmp/pioneer-runtime/memory/capsules")
        );
        assert_eq!(config.gateway.voice.models_dir, "models/voice");
        assert_eq!(
            config.gateway.voice.transcription_strategy,
            super::GatewayVoiceTranscriptionStrategy::BufferedGatewaySession
        );
        assert!(!config.gateway.voice.enabled);
        assert_eq!(config.gateway.voice.provider, None);
        assert_eq!(config.gateway.voice.model, None);
        assert_eq!(
            config
                .gateway
                .voice
                .resolve_models_root(PathBuf::from("/tmp/pioneer-runtime").as_path())
                .expect("voice models root should resolve"),
            PathBuf::from("/tmp/pioneer-runtime/models/voice")
        );
        assert!(config.gateway.thread_episodic.enabled);
        assert!(config.gateway.thread_episodic.indexing_enabled);
        assert!(config.gateway.thread_episodic.recall_enabled);
        assert_eq!(config.gateway.thread_episodic.default_prompt_chars, 2_400);
        assert_eq!(config.gateway.thread_episodic.max_prompt_chars, 12_000);
        assert_eq!(config.gateway.thread_episodic.max_hit_chars, 1_200);
        assert_eq!(config.gateway.thread_episodic.default_max_candidates, 32);
        assert_eq!(config.gateway.thread_episodic.max_candidate_work, 128);
        assert_eq!(config.gateway.thread_episodic.max_segments, 16);
        assert_eq!(config.gateway.thread_episodic.min_relevancy, 0.25);
        assert_eq!(config.gateway.thread_episodic.min_results, 1);
        assert_eq!(config.gateway.thread_episodic.snippet_chars, 360);
        assert_eq!(config.gateway.thread_episodic.index_batch_limit, 16);
        assert_eq!(config.gateway.thread_episodic.retry_base_delay_secs, 30);
        assert_eq!(config.gateway.thread_episodic.retry_max_delay_secs, 900);
        assert_eq!(config.gateway.thread_episodic.max_attempts, 5);
        assert_eq!(config.gateway.thread_episodic.near_capacity_percent, 85.0);
        assert!(!config.gateway.thread_episodic.vector_search.enabled);
        assert_eq!(config.gateway.thread_episodic.vector_search.provider, None);
        assert_eq!(config.gateway.thread_episodic.vector_search.model, None);
        assert_eq!(
            config.gateway.thread_episodic.vector_search.local_model,
            None
        );
        assert!(
            config
                .gateway
                .thread_episodic
                .vector_search
                .embedding_normalized
        );
        assert!(
            !config
                .gateway
                .thread_episodic
                .vector_search
                .use_search_instructions
        );
    }

    #[test]
    fn gateway_memory_config_rejects_unsafe_capsule_dirs() {
        for capsules_dir in ["", "/tmp/memory", "../memory", "memory/../capsules", "."] {
            let config = super::GatewayMemoryConfig {
                capsules_dir: capsules_dir.to_owned(),
                ..super::GatewayMemoryConfig::default()
            };

            assert!(
                config
                    .resolve_capsules_root(PathBuf::from("/tmp/pioneer-runtime").as_path())
                    .is_err(),
                "capsules_dir `{capsules_dir}` must be rejected"
            );
        }
    }

    #[test]
    fn gateway_voice_config_rejects_unsafe_model_dirs() {
        for models_dir in ["", "/tmp/models", "../models", "models/../voice", "."] {
            let config = super::GatewayVoiceConfig {
                models_dir: models_dir.to_owned(),
                ..super::GatewayVoiceConfig::default()
            };

            assert!(
                config
                    .resolve_models_root(PathBuf::from("/tmp/pioneer-runtime").as_path())
                    .is_err(),
                "models_dir `{models_dir}` must be rejected"
            );
        }
    }

    #[test]
    fn gateway_voice_config_selection_defaults_disabled_and_roundtrips() {
        let defaults = super::GatewayVoiceConfig::default();
        assert!(!defaults.enabled);
        assert_eq!(defaults.provider, None);
        assert_eq!(defaults.model, None);

        let serialized = toml::to_string(&super::GatewayVoiceConfig {
            enabled: true,
            provider: Some(super::GatewayVoiceInputProviderConfig::Local),
            model: Some("parakeet-tdt-0.6b-v3".to_owned()),
            ..super::GatewayVoiceConfig::default()
        })
        .expect("voice config should serialize");
        let roundtrip: super::GatewayVoiceConfig =
            toml::from_str(serialized.as_str()).expect("voice config should deserialize");

        assert!(roundtrip.enabled);
        assert_eq!(
            roundtrip.provider,
            Some(super::GatewayVoiceInputProviderConfig::Local)
        );
        assert_eq!(roundtrip.model.as_deref(), Some("parakeet-tdt-0.6b-v3"));
        assert_eq!(roundtrip.models_dir, "models/voice");
    }

    #[test]
    fn gateway_memory_product_config_defaults_are_safe_and_useful() {
        let config = GatewayMemoryConfig::default();

        assert!(config.enabled);
        assert!(config.allow_global_user_by_default);
        assert!(!config.allow_global_agent_by_default);
        assert!(config.deterministic_recall_enabled);
        assert!(config.active_recall_enabled);
        assert!(config.tools_enabled);
        assert!(config.proactive_writes_enabled);
        assert!(config.background_extraction_enabled);
        assert!(config.active_recall_model.is_thread_model());
        assert!(config.proactive_writes_model.is_thread_model());
        assert!(!config.debug_trace_enabled);
        assert!(!config.strict_diagnostics_enabled);
    }

    #[test]
    fn gateway_thread_episodic_config_defaults_match_runtime_contract() {
        let config = super::GatewayThreadEpisodicConfig::default();

        assert!(config.enabled);
        assert!(config.indexing_enabled);
        assert!(config.recall_enabled);
        assert_eq!(config.default_prompt_chars, 2_400);
        assert_eq!(config.max_prompt_chars, 12_000);
        assert_eq!(config.max_hit_chars, 1_200);
        assert_eq!(config.default_max_candidates, 32);
        assert_eq!(config.max_candidate_work, 128);
        assert_eq!(config.max_segments, 16);
        assert_eq!(config.min_relevancy, 0.25);
        assert_eq!(config.min_results, 1);
        assert_eq!(config.snippet_chars, 360);
        assert_eq!(config.index_batch_limit, 16);
        assert_eq!(config.retry_base_delay_secs, 30);
        assert_eq!(config.retry_max_delay_secs, 900);
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.near_capacity_percent, 85.0);
        assert!(!config.vector_search.enabled);
        assert_eq!(config.vector_search.provider, None);
        assert_eq!(config.vector_search.model, None);
        assert_eq!(config.vector_search.local_model, None);
        assert!(config.vector_search.embedding_normalized);
        assert!(!config.vector_search.use_search_instructions);
    }

    #[test]
    fn gateway_thread_episodic_config_deserializes_missing_fields_with_defaults() {
        let config = toml::from_str::<super::GatewayThreadEpisodicConfig>(
            r#"
enabled = false
default_prompt_chars = 1000
"#,
        )
        .expect("gateway thread episodic config should deserialize with defaults");

        assert!(!config.enabled);
        assert!(config.indexing_enabled);
        assert!(config.recall_enabled);
        assert_eq!(config.default_prompt_chars, 1_000);
        assert_eq!(config.max_prompt_chars, 12_000);
        assert_eq!(config.index_batch_limit, 16);
        assert_eq!(config.near_capacity_percent, 85.0);
        assert!(!config.vector_search.enabled);
        assert_eq!(config.vector_search.provider, None);
        assert_eq!(config.vector_search.model, None);
        assert_eq!(config.vector_search.local_model, None);
        assert!(!config.vector_search.use_search_instructions);
    }

    #[test]
    fn gateway_thread_episodic_vector_search_config_represents_provider_identity_without_keys() {
        let config = toml::from_str::<super::GatewayThreadEpisodicConfig>(
            r#"
enabled = true

[vector_search]
enabled = true
provider = "openrouter"
model = "openai/text-embedding-3-small"
local_model = "bge-base-en-v1.5"
embedding_dimension = 1536
embedding_normalized = true
use_search_instructions = true
"#,
        )
        .expect("gateway thread episodic vector search config should deserialize");

        assert!(config.vector_search.enabled);
        assert_eq!(
            config.vector_search.provider,
            Some(super::GatewayThreadEpisodicVectorProviderConfig::OpenRouter)
        );
        assert_eq!(
            config.vector_search.model.as_deref(),
            Some("openai/text-embedding-3-small")
        );
        assert_eq!(
            config.vector_search.local_model.as_deref(),
            Some("bge-base-en-v1.5")
        );
        assert!(config.vector_search.embedding_normalized);
        assert!(config.vector_search.use_search_instructions);

        let serialized =
            toml::to_string(&config.vector_search).expect("vector config should serialize");
        assert!(serialized.contains("provider = \"openrouter\""));
        assert!(serialized.contains("use_search_instructions = true"));
        assert!(!serialized.contains("embedding_dimension"));
        assert!(!serialized.contains("api_key"));
        assert!(!serialized.contains("secret"));

        let local_config = toml::from_str::<super::GatewayThreadEpisodicConfig>(
            r#"
enabled = true

[vector_search]
enabled = true
provider = "local"
model = "bge-small-en-v1.5"
local_model = "bge-small-en-v1.5"
embedding_dimension = 384
"#,
        )
        .expect("gateway thread episodic local vector config should deserialize");
        assert_eq!(
            local_config.vector_search.provider,
            Some(super::GatewayThreadEpisodicVectorProviderConfig::Local)
        );
        assert_eq!(
            local_config.vector_search.model.as_deref(),
            Some("bge-small-en-v1.5")
        );
        assert_eq!(
            local_config.vector_search.local_model.as_deref(),
            Some("bge-small-en-v1.5")
        );
    }

    #[test]
    fn gateway_execution_windows_config_defaults_are_deterministic() {
        let config = super::GatewayExecutionWindowsConfig::default();

        assert_eq!(config.window.max_agent_rounds_per_window, 32);
        assert_eq!(config.window.max_tool_calls_per_window, 128);
        assert_eq!(config.window.max_wall_clock_ms_per_window, Some(1_800_000));
        assert_eq!(config.window.max_provider_tokens_per_window, None);
        assert_eq!(config.total.max_windows_per_turn, None);
        assert_eq!(config.total.max_tool_calls_per_turn, None);
        assert_eq!(config.total.max_wall_clock_ms_per_turn, None);
        assert_eq!(config.total.max_provider_tokens_per_turn, None);
        assert_eq!(config.total.max_consecutive_no_progress_windows, 3);
    }

    #[test]
    fn gateway_tools_config_deserializes_missing_execution_windows_with_defaults() {
        let config = toml::from_str::<super::GatewayToolsConfig>(
            r#"
[budget]
max_agent_rounds_per_turn = 7
max_tool_calls_per_turn = 11
"#,
        )
        .expect("gateway tools config should deserialize with omitted execution_windows");

        assert_eq!(config.budget.max_agent_rounds_per_turn, 7);
        assert_eq!(config.budget.max_tool_calls_per_turn, 11);
        assert!(config.execution_windows.is_none());
    }

    #[test]
    fn gateway_execution_windows_config_serializes_stable_wire_format() {
        let config = super::GatewayExecutionWindowsConfig {
            window: super::GatewayExecutionWindowBudgetConfig {
                max_agent_rounds_per_window: 31,
                max_tool_calls_per_window: 37,
                max_wall_clock_ms_per_window: Some(1_000),
                max_provider_tokens_per_window: Some(2_000),
            },
            total: super::GatewayExecutionWindowTotalBudgetConfig {
                max_windows_per_turn: Some(5),
                max_tool_calls_per_turn: Some(41),
                max_wall_clock_ms_per_turn: Some(3_000),
                max_provider_tokens_per_turn: Some(4_000),
                max_consecutive_no_progress_windows: 2,
            },
        };

        let serialized =
            toml::to_string(&config).expect("execution windows config should serialize");

        assert!(serialized.contains("max_agent_rounds_per_window = 31"));
        assert!(serialized.contains("max_tool_calls_per_window = 37"));
        assert!(serialized.contains("max_wall_clock_ms_per_window = 1000"));
        assert!(serialized.contains("max_provider_tokens_per_window = 2000"));
        assert!(serialized.contains("[total]"));
        assert!(serialized.contains("max_windows_per_turn = 5"));
        assert!(serialized.contains("max_tool_calls_per_turn = 41"));
        assert!(serialized.contains("max_wall_clock_ms_per_turn = 3000"));
        assert!(serialized.contains("max_provider_tokens_per_turn = 4000"));
        assert!(serialized.contains("max_consecutive_no_progress_windows = 2"));

        let roundtrip = toml::from_str::<super::GatewayExecutionWindowsConfig>(&serialized)
            .expect("execution windows config should deserialize");
        assert_eq!(roundtrip, config);
    }

    #[test]
    fn gateway_tools_config_deserializes_new_execution_windows_section() {
        let config = toml::from_str::<super::GatewayToolsConfig>(
            r#"
[execution_windows]
max_agent_rounds_per_window = 31
max_tool_calls_per_window = 37
max_wall_clock_ms_per_window = 1000
max_provider_tokens_per_window = 2000

[execution_windows.total]
max_windows_per_turn = 5
max_tool_calls_per_turn = 41
max_wall_clock_ms_per_turn = 3000
max_provider_tokens_per_turn = 4000
max_consecutive_no_progress_windows = 2
"#,
        )
        .expect("gateway tools config should deserialize with execution_windows");

        let execution_windows = config
            .execution_windows
            .expect("execution_windows section should be present");
        assert_eq!(execution_windows.window.max_agent_rounds_per_window, 31);
        assert_eq!(execution_windows.window.max_tool_calls_per_window, 37);
        assert_eq!(
            execution_windows.window.max_wall_clock_ms_per_window,
            Some(1_000)
        );
        assert_eq!(
            execution_windows.window.max_provider_tokens_per_window,
            Some(2_000)
        );
        assert_eq!(execution_windows.total.max_windows_per_turn, Some(5));
        assert_eq!(execution_windows.total.max_tool_calls_per_turn, Some(41));
        assert_eq!(
            execution_windows.total.max_wall_clock_ms_per_turn,
            Some(3_000)
        );
        assert_eq!(
            execution_windows.total.max_provider_tokens_per_turn,
            Some(4_000)
        );
        assert_eq!(
            execution_windows.total.max_consecutive_no_progress_windows,
            2
        );
    }

    #[test]
    fn gateway_execution_windows_accepts_legacy_failed_window_breaker_name() {
        let config = toml::from_str::<super::GatewayExecutionWindowsConfig>(
            r#"
[total]
max_consecutive_failed_windows = 7
"#,
        )
        .expect("legacy failed-window breaker name should remain compatible");

        assert_eq!(config.total.max_consecutive_no_progress_windows, 7);
        assert_eq!(config.total.max_windows_per_turn, None);
        assert_eq!(config.total.max_tool_calls_per_turn, None);
        assert_eq!(config.total.max_wall_clock_ms_per_turn, None);
    }

    #[test]
    fn gateway_tools_config_parses_legacy_budget_snapshot_without_execution_windows() {
        let config = toml::from_str::<super::GatewayToolsConfig>(
            r#"
[web]
default_timeout_ms = 20000

[budget]
max_agent_rounds_per_turn = 512
max_tool_calls_per_turn = 2048

[retry]
max_recoverable_retry_rounds_per_episode = 32
max_same_tool_error_retries_per_episode = 3
max_retries_per_tool_name_per_episode = 16
"#,
        )
        .expect("legacy gateway tools config snapshot should parse");

        assert_eq!(config.budget.max_agent_rounds_per_turn, 512);
        assert_eq!(config.budget.max_tool_calls_per_turn, 2048);
        assert!(config.execution_windows.is_none());
    }

    #[test]
    fn gateway_tasks_review_config_deserializes_missing_fields_with_defaults() {
        let config = toml::from_str::<super::GatewayTasksConfig>(
            r#"
[review]
enabled = false
"#,
        )
        .expect("gateway tasks config should deserialize with defaults");

        assert!(!config.review.enabled);
        assert!(!config.review.allow_task_create_review_policy);
        assert!(
            config
                .review
                .default_parent_review_for_immediate_attached_agent_tasks
        );
        assert_eq!(config.review.default_max_revision_rounds, 5);
        assert_eq!(config.review.auto_accept_after_seconds, 300);
    }

    #[test]
    fn gateway_memory_product_config_deserializes_missing_fields_with_defaults() {
        let config = toml::from_str::<GatewayMemoryConfig>(
            r#"
enabled = false
capsules_dir = "memory/custom-capsules"
"#,
        )
        .expect("gateway memory config should deserialize with defaults");

        assert!(!config.enabled);
        assert_eq!(config.capsules_dir, "memory/custom-capsules");
        assert!(config.allow_global_user_by_default);
        assert!(!config.allow_global_agent_by_default);
        assert!(config.deterministic_recall_enabled);
        assert!(config.active_recall_enabled);
        assert!(config.tools_enabled);
        assert!(config.proactive_writes_enabled);
        assert!(config.background_extraction_enabled);
        assert!(config.active_recall_model.is_thread_model());
        assert!(config.proactive_writes_model.is_thread_model());
        assert!(!config.debug_trace_enabled);
        assert!(!config.strict_diagnostics_enabled);
    }

    #[test]
    fn gateway_memory_model_selection_uses_complete_custom_override_only() {
        #[derive(serde::Deserialize)]
        struct SelectionWrapper {
            selection: GatewayMemoryModelSelectionConfig,
        }

        let thread = toml::from_str::<SelectionWrapper>("selection = \"thread\"")
            .expect("thread shorthand parses");
        assert!(thread.selection.is_thread_model());

        let complete = GatewayMemoryModelSelectionConfig::custom("provider", "model");
        assert_eq!(
            complete.model_provider_override().as_deref(),
            Some("provider")
        );
        assert_eq!(complete.model_override().as_deref(), Some("model"));

        let missing_model = GatewayMemoryModelSelectionConfig {
            source: super::GatewayMemoryModelSelectionSource::Custom,
            model_provider: Some("provider".to_owned()),
            model: None,
        };
        assert!(missing_model.model_provider_override().is_none());
        assert!(missing_model.model_override().is_none());
    }

    #[test]
    fn gateway_memory_model_selection_rejects_removed_turn_alias() {
        #[derive(Debug, serde::Deserialize)]
        struct SelectionWrapper {
            #[allow(dead_code)]
            selection: GatewayMemoryModelSelectionConfig,
        }

        let error = toml::from_str::<SelectionWrapper>("selection = \"turn\"")
            .expect_err("turn alias must not parse");
        assert!(
            format!("{error:#}").contains("expected thread|custom"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn gateway_self_improvement_config_is_default_off_when_absent() {
        let config = toml::from_str::<GatewaySelfImprovementConfig>("")
            .expect("absent self-improvement fields must use defaults");
        assert!(!config.enabled);
        assert!(config.default_model.is_none());
        assert!(config.reviewer_model.is_none());
    }

    #[test]
    fn gateway_self_improvement_config_strict_roundtrip_normalizes_models() {
        let config = toml::from_str::<GatewaySelfImprovementConfig>(
            r#"
enabled = true

[default_model]
provider = " openai "
model = " gpt-5.4 "

[reviewer_model]
provider = " anthropic "
model = " claude-sonnet "
"#,
        )
        .expect("complete selections must parse");

        assert!(config.enabled);
        let default_model = config.default_model.as_ref().expect("default model");
        assert_eq!(default_model.provider, "openai");
        assert_eq!(default_model.model, "gpt-5.4");
        let reviewer_model = config.reviewer_model.as_ref().expect("reviewer model");
        assert_eq!(reviewer_model.provider, "anthropic");
        assert_eq!(reviewer_model.model, "claude-sonnet");

        let serialized = toml::to_string(&config).expect("config must serialize");
        let roundtrip = toml::from_str::<GatewaySelfImprovementConfig>(&serialized)
            .expect("serialized config must parse");
        assert_eq!(roundtrip, config);
    }

    #[test]
    fn gateway_self_improvement_config_rejects_partial_or_unknown_models() {
        for invalid in [
            "[default_model]\nprovider = \"openai\"\n",
            "[default_model]\nprovider = \"openai\"\nmodel = \"  \"\n",
            "[default_model]\nprovider = \"openai\"\nmodel = \"gpt\"\nsource = \"thread\"\n",
        ] {
            assert!(
                toml::from_str::<GatewaySelfImprovementConfig>(invalid).is_err(),
                "invalid selection must be rejected: {invalid}"
            );
        }
    }

    #[test]
    fn gateway_memory_product_config_serializes_stable_field_names() {
        let config = GatewayMemoryConfig {
            enabled: true,
            capsules_dir: "memory/capsules".to_owned(),
            allow_global_user_by_default: true,
            allow_global_agent_by_default: false,
            deterministic_recall_enabled: true,
            active_recall_enabled: true,
            tools_enabled: false,
            proactive_writes_enabled: false,
            background_extraction_enabled: false,
            active_recall_model: GatewayMemoryModelSelectionConfig::custom(
                "planner-provider",
                "planner-model",
            ),
            proactive_writes_model: GatewayMemoryModelSelectionConfig::custom(
                "extractor-provider",
                "extractor-model",
            ),
            debug_trace_enabled: true,
            strict_diagnostics_enabled: true,
        };

        let serialized = toml::to_string(&config).expect("gateway memory config should serialize");
        assert!(serialized.contains("enabled = true"));
        assert!(serialized.contains("capsules_dir = \"memory/capsules\""));
        assert!(serialized.contains("allow_global_user_by_default = true"));
        assert!(serialized.contains("allow_global_agent_by_default = false"));
        assert!(serialized.contains("deterministic_recall_enabled = true"));
        assert!(serialized.contains("active_recall_enabled = true"));
        assert!(serialized.contains("tools_enabled = false"));
        assert!(serialized.contains("proactive_writes_enabled = false"));
        assert!(serialized.contains("background_extraction_enabled = false"));
        assert!(serialized.contains("active_recall_model"));
        assert!(serialized.contains("model_provider = \"planner-provider\""));
        assert!(serialized.contains("model = \"planner-model\""));
        assert!(serialized.contains("proactive_writes_model"));
        assert!(serialized.contains("model_provider = \"extractor-provider\""));
        assert!(serialized.contains("model = \"extractor-model\""));
        assert!(serialized.contains("debug_trace_enabled = true"));
        assert!(serialized.contains("strict_diagnostics_enabled = true"));

        let roundtrip = toml::from_str::<GatewayMemoryConfig>(serialized.as_str())
            .expect("gateway memory config should deserialize");
        assert_eq!(roundtrip, config);
    }

    #[test]
    fn gateway_memory_product_config_serializes_thread_model_as_shorthand() {
        let serialized = toml::to_string(&GatewayMemoryConfig::default())
            .expect("gateway memory config should serialize");

        assert!(serialized.contains("active_recall_model = \"thread\""));
        assert!(serialized.contains("proactive_writes_model = \"thread\""));
        assert!(!serialized.contains("[active_recall_model]"));
        assert!(!serialized.contains("[proactive_writes_model]"));
    }

    #[test]
    fn gateway_auth_session_defaults_are_secure_and_valid() {
        let config = toml::from_str::<super::GatewayAuthConfig>(
            r#"
jwt_issuer = "pioneer"
jwt_audience = "pioneer-clients"
secret_size_bytes = 64
token_refresh_leeway_seconds = 300
"#,
        )
        .expect("minimal auth config remains parseable");

        assert_eq!(config.access_token_ttl_seconds, 900);
        assert_eq!(config.refresh_token_ttl_seconds, 7_776_000);
        assert_eq!(config.device_activation_code_ttl_seconds, 600);
        assert_eq!(config.auth_exchange_timeout_seconds, 15);
        assert_eq!(config.database_acquire_timeout_ms, 500);
        config.validate_session_security().expect("secure defaults");
    }

    #[test]
    fn gateway_auth_config_rejects_removed_fields() {
        for removed_field in [
            r#"superuser_subject = "superuser""#,
            r#"superuser_role = "superuser""#,
            "token_ttl_seconds = 31536000",
            "opaque_token_size_bytes = 32",
        ] {
            let config = format!(
                r#"
jwt_issuer = "pioneer"
jwt_audience = "pioneer-clients"
secret_size_bytes = 64
token_refresh_leeway_seconds = 300
{removed_field}
"#
            );
            let error = toml::from_str::<super::GatewayAuthConfig>(&config)
                .expect_err("removed auth config must fail closed");
            assert!(
                error.to_string().contains("unknown field"),
                "unexpected error for `{removed_field}`: {error}"
            );
        }
    }

    #[test]
    fn gateway_auth_session_validation_rejects_insecure_combinations() {
        let mut config = load_config_from_sources(DEFAULT_CONFIG_TOML, Vec::new())
            .expect("default config")
            .gateway
            .auth;
        config.validate_session_security().expect("secure defaults");

        config.refresh_token_ttl_seconds = config.access_token_ttl_seconds;
        assert!(config.validate_session_security().is_err());
        config.refresh_token_ttl_seconds = 366 * 24 * 60 * 60;
        assert!(config.validate_session_security().is_err());
        config.refresh_token_ttl_seconds = 7_776_000;
        config.token_refresh_leeway_seconds = config.access_token_ttl_seconds;
        assert!(config.validate_session_security().is_err());
        config.token_refresh_leeway_seconds = 300;
        config.secret_size_bytes = 31;
        assert!(config.validate_session_security().is_err());
        config.secret_size_bytes = 64;
        config.device_activation_code_ttl_seconds = 0;
        assert!(config.validate_session_security().is_err());
        config.device_activation_code_ttl_seconds = 600;
        config.database_acquire_timeout_ms = 49;
        assert!(config.validate_session_security().is_err());
        config.database_acquire_timeout_ms = 5_001;
        assert!(config.validate_session_security().is_err());
        config.database_acquire_timeout_ms = 500;
        config.jwt_issuer.clear();
        assert!(config.validate_session_security().is_err());
    }

    fn write_file(path: &PathBuf, content: &str) {
        fs::write(path, content).expect("failed to write test file");
    }

    fn unique_temp_file_path(prefix: &str) -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{prefix}-{nanos}-{id}.toml"))
    }

    fn expected_executable_name(stem: &str) -> String {
        if cfg!(windows) {
            format!("{stem}.exe")
        } else {
            stem.to_owned()
        }
    }

    fn expected_install_root_directory_name() -> String {
        if cfg!(any(windows, target_os = "macos")) {
            "Pioneer".to_owned()
        } else {
            "pioneer".to_owned()
        }
    }
}
