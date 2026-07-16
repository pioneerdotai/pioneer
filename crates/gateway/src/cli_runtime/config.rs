use anyhow::{Context, Result};
use pioneer_cli_agent_runtime::claude::ClaudeAccountProbeConfig;
use pioneer_cli_agent_runtime::codex::CodexAccountProbeConfig;
use pioneer_cli_agent_runtime::process::{SecretString, SensitiveEnvironment};
use pioneer_cli_agent_runtime::reserved_args::{
    validate_claude_custom_args, validate_codex_custom_args,
};
use pioneer_config::{
    AppConfig, EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
};
use std::fs;
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
    let instances = config.gateway.effective_cli_agent_runtime_instances();
    for instance in &instances {
        validate_instance_launch_args(instance).with_context(|| {
            format!(
                "invalid custom launch arguments for CLI runtime `{}`",
                instance.id
            )
        })?;
    }
    Ok(instances)
}

pub(crate) fn validate_instance_launch_args(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
) -> Result<()> {
    match instance.kind {
        GatewayCliAgentRuntimeKindConfig::Codex => {
            validate_codex_custom_args(&instance.app_server_args)?;
        }
        GatewayCliAgentRuntimeKindConfig::Claude => {
            validate_claude_custom_args(&instance.app_server_args)?;
        }
    }
    Ok(())
}

/// Resolve the MCP stdio helper to the exact Pioneer executable that is
/// already running this Gateway. Production never performs a PATH lookup or
/// selects a separately installed sidecar.
pub(crate) fn resolve_current_pioneer_cli_mcp_helper() -> Result<PathBuf> {
    let running = std::env::current_exe()
        .context("failed to resolve the running Pioneer executable for CLI MCP")?;
    if !running.is_absolute() {
        anyhow::bail!("running Pioneer executable path is not absolute");
    }
    let resolved = fs::canonicalize(running.as_path())
        .context("failed to canonicalize the running Pioneer executable for CLI MCP")?;
    let metadata = fs::metadata(resolved.as_path())
        .context("failed to inspect the running Pioneer executable for CLI MCP")?;
    if !resolved.is_absolute() || !metadata.is_file() {
        anyhow::bail!("running Pioneer executable is not an absolute regular file");
    }
    Ok(resolved)
}

pub(crate) fn codex_account_probe_config_from_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
) -> CodexAccountProbeConfig {
    codex_account_probe_config_from_instance_with_proxy(instance, None)
}

pub(crate) fn codex_account_probe_config_from_instance_with_proxy(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    proxy_url: Option<&str>,
) -> CodexAccountProbeConfig {
    CodexAccountProbeConfig {
        executable: instance.binary_path.clone(),
        home_path: instance.home_path.clone(),
        shadow_home_path: instance.shadow_home_path.clone(),
        cwd: std::env::current_dir().ok(),
        home_dir: None,
        env: proxy_env(proxy_url),
        initialize_timeout: Duration::from_millis(instance.startup_probe_timeout_ms),
        request_timeout: Duration::from_millis(instance.request_timeout_ms),
        shutdown_grace: Duration::from_secs(2),
        stderr_ring_lines: instance.stderr_ring_lines,
    }
}

pub(crate) fn claude_account_probe_config_from_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
) -> ClaudeAccountProbeConfig {
    claude_account_probe_config_from_instance_with_proxy(instance, None)
}

pub(crate) fn claude_account_probe_config_from_instance_with_proxy(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    proxy_url: Option<&str>,
) -> ClaudeAccountProbeConfig {
    ClaudeAccountProbeConfig {
        executable: instance.binary_path.clone(),
        config_dir_path: instance
            .shadow_home_path
            .clone()
            .unwrap_or_else(|| instance.home_path.clone()),
        home_dir: None,
        env: proxy_env(proxy_url),
        request_timeout: Duration::from_millis(instance.startup_probe_timeout_ms),
    }
}

pub(crate) fn proxy_env(proxy_url: Option<&str>) -> SensitiveEnvironment {
    let Some(proxy_url) = proxy_url
        .map(str::trim)
        .filter(|proxy_url| !proxy_url.is_empty())
    else {
        return SensitiveEnvironment::new();
    };
    let mut env = SensitiveEnvironment::new();
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        env.insert_secret(key, SecretString::new(proxy_url));
    }
    let no_proxy = "localhost,127.0.0.1,::1";
    env.insert_plain("NO_PROXY", no_proxy);
    env.insert_plain("no_proxy", no_proxy);
    env
}

#[cfg(test)]
mod tests {
    use super::{proxy_env, resolve_current_pioneer_cli_mcp_helper, validate_instance_launch_args};
    use pioneer_config::{
        EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
    };

    fn instance(
        kind: GatewayCliAgentRuntimeKindConfig,
        app_server_args: Vec<String>,
    ) -> EffectiveGatewayCliAgentRuntimeInstanceConfig {
        EffectiveGatewayCliAgentRuntimeInstanceConfig {
            id: "test-runtime".to_owned(),
            kind,
            display_name: "Test runtime".to_owned(),
            enabled: true,
            binary_path: "provider".to_owned(),
            home_path: "/tmp/provider-home".to_owned(),
            shadow_home_path: None,
            custom_models: Vec::new(),
            app_server_args,
            startup_probe_timeout_ms: 1_000,
            request_timeout_ms: 1_000,
            idle_session_ttl_secs: 60,
            event_channel_capacity: 16,
            stderr_ring_lines: 16,
            debug_native_events: false,
        }
    }

    #[test]
    fn proxy_env_sets_proxy_vars_and_loopback_bypass() {
        let env = proxy_env(Some(" socks5://proxy.example:1080 "));

        for key in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            assert_eq!(env.expose(key), Some("socks5://proxy.example:1080"));
        }
        assert_eq!(env.expose("NO_PROXY"), Some("localhost,127.0.0.1,::1"));
        assert_eq!(env.expose("no_proxy"), Some("localhost,127.0.0.1,::1"));
    }

    #[test]
    fn proxy_env_ignores_empty_values() {
        assert!(proxy_env(None).is_empty());
        assert!(proxy_env(Some("   ")).is_empty());
    }

    #[test]
    fn cli_runtime_config_rejects_reserved_args_and_keeps_safe_args() {
        let codex_reserved = instance(
            GatewayCliAgentRuntimeKindConfig::Codex,
            vec!["-c".to_owned(), "mcp_servers.pioneer={}".to_owned()],
        );
        assert!(validate_instance_launch_args(&codex_reserved).is_err());

        let claude_reserved = instance(
            GatewayCliAgentRuntimeKindConfig::Claude,
            vec!["--strict-mcp-config".to_owned()],
        );
        assert!(validate_instance_launch_args(&claude_reserved).is_err());

        let codex_safe = instance(
            GatewayCliAgentRuntimeKindConfig::Codex,
            vec!["-c".to_owned(), "model=\"gpt-5\"".to_owned()],
        );
        validate_instance_launch_args(&codex_safe).expect("safe Codex args should remain valid");

        let claude_safe = instance(
            GatewayCliAgentRuntimeKindConfig::Claude,
            vec!["--model".to_owned(), "sonnet".to_owned()],
        );
        validate_instance_launch_args(&claude_safe).expect("safe Claude args should remain valid");
    }

    #[test]
    fn proxy_credentials_are_redacted_from_debug_output() {
        let canary = "socks5://user:proposal53-proxy-canary@proxy.example:1080";
        let env = proxy_env(Some(canary));
        assert_eq!(env.expose("HTTPS_PROXY"), Some(canary));
        assert!(!format!("{env:?}").contains(canary));
    }

    #[test]
    fn codex_mcp_config_helper_is_the_absolute_running_executable() {
        let helper = resolve_current_pioneer_cli_mcp_helper().expect("running test executable");
        assert!(helper.is_absolute());
        assert!(helper.is_file());
        assert_eq!(
            helper,
            std::fs::canonicalize(std::env::current_exe().expect("current executable"))
                .expect("canonical current executable")
        );
    }
}
