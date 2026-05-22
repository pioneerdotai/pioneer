use anyhow::{Context, Result, bail};
use config::{Config, ConfigError, File, FileFormat};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
    #[serde(default = "default_gateway_keepawake")]
    pub keepawake: bool,
    #[serde(default)]
    pub preflight_model: GatewayMemoryModelSelectionConfig,
    pub thread: GatewayThreadConfig,
    #[serde(default)]
    pub tools: GatewayToolsConfig,
    #[serde(default)]
    pub skills: GatewaySkillsConfig,
    pub provider: GatewayProviderConfig,
    pub database: GatewayDatabaseConfig,
    #[serde(default)]
    pub memory: GatewayMemoryConfig,
    #[serde(default)]
    pub hooks: GatewayHooksConfig,
    #[serde(default)]
    pub artifacts: GatewayArtifactsConfig,
    pub auth: GatewayAuthConfig,
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
    #[serde(default = "default_gateway_artifacts_download_session_ttl_secs")]
    pub download_session_ttl_secs: u64,
    #[serde(default = "default_gateway_artifacts_gc_grace_secs")]
    pub gc_grace_secs: u64,
    #[serde(default = "default_gateway_artifacts_output_dir_ttl_secs")]
    pub output_dir_ttl_secs: u64,
    #[serde(default = "default_gateway_artifacts_quota_warn_at_percent")]
    pub quota_warn_at_percent: u8,
}

impl Default for GatewayArtifactsConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: default_gateway_artifacts_max_file_bytes(),
            max_workspace_bytes: default_gateway_artifacts_max_workspace_bytes(),
            max_files_per_workspace: default_gateway_artifacts_max_files_per_workspace(),
            upload_session_ttl_secs: default_gateway_artifacts_upload_session_ttl_secs(),
            download_session_ttl_secs: default_gateway_artifacts_download_session_ttl_secs(),
            gc_grace_secs: default_gateway_artifacts_gc_grace_secs(),
            output_dir_ttl_secs: default_gateway_artifacts_output_dir_ttl_secs(),
            quota_warn_at_percent: default_gateway_artifacts_quota_warn_at_percent(),
        }
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
    pub default_timeout_secs: u64,
    #[serde(default)]
    pub attachments: GatewayProviderAttachmentsConfig,
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
    512
}

const fn default_tool_loop_max_tool_calls_per_turn() -> u32 {
    2048
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
    true
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

const fn default_gateway_artifacts_download_session_ttl_secs() -> u64 {
    15 * 60
}

const fn default_gateway_artifacts_gc_grace_secs() -> u64 {
    24 * 60 * 60
}

const fn default_gateway_artifacts_output_dir_ttl_secs() -> u64 {
    24 * 60 * 60
}

const fn default_gateway_artifacts_quota_warn_at_percent() -> u8 {
    80
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
    24_000
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
    pub run_migrations_on_startup: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayAuthConfig {
    pub jwt_issuer: String,
    pub jwt_audience: String,
    pub superuser_subject: String,
    pub superuser_role: String,
    pub secret_size_bytes: usize,
    pub token_ttl_seconds: u64,
    pub token_refresh_leeway_seconds: u64,
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
    pub registry_version: u32,
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
        DEFAULT_CONFIG_TOML, GatewayMemoryConfig, GatewayMemoryModelSelectionConfig,
        InstallManagedBy, InstallState, load_config_from_sources, load_install_state,
        save_install_state,
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
    fn migration_gateway_preflight_model_copies_legacy_memory_active_recall_model() {
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
    fn migration_gateway_preflight_model_preserves_explicit_new_setting() {
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
        assert_eq!(config.gateway.skills.runtime.read_skill_max_chars, 24_000);
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
        assert!(config.gateway.provider.attachments.allow_url_sources);
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
