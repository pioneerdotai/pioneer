use anyhow::{Context, Result};
use pioneer_cli_agent_runtime::claude::ClaudeAccountProbeConfig;
use pioneer_cli_agent_runtime::codex::CodexAccountProbeConfig;
use pioneer_config::{AppConfig, EffectiveGatewayCliAgentRuntimeInstanceConfig};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) fn load_effective_cli_runtime_instances(
    runtime_home: &Path,
) -> Result<Vec<EffectiveGatewayCliAgentRuntimeInstanceConfig>> {
    let config = AppConfig::load().context("failed to load app config for CLI runtime catalog")?;
    let settings_file_name =
        crate::settings::normalize_settings_file_name(config.gateway.settings_file_name.as_str())?;
    let settings_path = runtime_home.join(settings_file_name.as_str());
    let settings = crate::settings::load_or_create_gateway_settings(
        settings_path.as_path(),
        config.gateway.settings_version,
        settings_file_name.as_str(),
    )?;
    let config = settings.apply_to_app_config(config);
    Ok(config.gateway.effective_cli_agent_runtime_instances())
}

pub(crate) fn codex_account_probe_config_from_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
) -> CodexAccountProbeConfig {
    CodexAccountProbeConfig {
        executable: instance.binary_path.clone(),
        home_path: instance.home_path.clone(),
        shadow_home_path: instance.shadow_home_path.clone(),
        cwd: std::env::current_dir().ok(),
        home_dir: None,
        initialize_timeout: Duration::from_millis(instance.startup_probe_timeout_ms),
        request_timeout: Duration::from_millis(instance.request_timeout_ms),
        shutdown_grace: Duration::from_secs(2),
        stderr_ring_lines: instance.stderr_ring_lines,
    }
}

pub(crate) fn claude_account_probe_config_from_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
) -> ClaudeAccountProbeConfig {
    ClaudeAccountProbeConfig {
        executable: instance.binary_path.clone(),
        config_dir_path: instance
            .shadow_home_path
            .clone()
            .unwrap_or_else(|| instance.home_path.clone()),
        home_dir: None,
        request_timeout: Duration::from_millis(instance.startup_probe_timeout_ms),
    }
}

pub(crate) fn current_process_cwd() -> Result<String> {
    let cwd: PathBuf =
        std::env::current_dir().context("failed to resolve current working directory")?;
    Ok(cwd.to_string_lossy().into_owned())
}
