use anyhow::{Context, Result};
use pioneer_cli_agent_runtime::claude::ClaudeAccountProbeConfig;
use pioneer_cli_agent_runtime::codex::CodexAccountProbeConfig;
use pioneer_config::{AppConfig, EffectiveGatewayCliAgentRuntimeInstanceConfig};
use std::collections::BTreeMap;
use std::path::Path;
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

pub(crate) fn proxy_env(proxy_url: Option<&str>) -> BTreeMap<String, String> {
    let Some(proxy_url) = proxy_url
        .map(str::trim)
        .filter(|proxy_url| !proxy_url.is_empty())
    else {
        return BTreeMap::new();
    };
    let mut env = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ]
    .into_iter()
    .map(|key| (key.to_owned(), proxy_url.to_owned()))
    .collect::<BTreeMap<_, _>>();
    let no_proxy = "localhost,127.0.0.1,::1";
    env.insert("NO_PROXY".to_owned(), no_proxy.to_owned());
    env.insert("no_proxy".to_owned(), no_proxy.to_owned());
    env
}

#[cfg(test)]
mod tests {
    use super::proxy_env;

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
            assert_eq!(
                env.get(key).map(String::as_str),
                Some("socks5://proxy.example:1080")
            );
        }
        assert_eq!(
            env.get("NO_PROXY").map(String::as_str),
            Some("localhost,127.0.0.1,::1")
        );
        assert_eq!(
            env.get("no_proxy").map(String::as_str),
            Some("localhost,127.0.0.1,::1")
        );
    }

    #[test]
    fn proxy_env_ignores_empty_values() {
        assert!(proxy_env(None).is_empty());
        assert!(proxy_env(Some("   ")).is_empty());
    }
}
