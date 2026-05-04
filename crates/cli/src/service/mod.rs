use anyhow::{Context, Result, bail};
use pioneer_config::{
    AppConfig, InstallManagedBy, InstallState, load_install_state, save_install_state,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const SERVICE_MODE_ARG: &str = "gateway-service";

pub(crate) struct ServiceSettings {
    pub service_name: String,
    pub legacy_service_names: Vec<String>,
    pub runtime_home_dir: PathBuf,
    pub macos_background_item_name: String,
    pub macos_associated_bundle_identifier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayServiceStatus {
    pub service_name: String,
    pub listen_addr: String,
    pub service_active: bool,
    pub gateway_reachable: bool,
    pub runtime_home: String,
    pub install_state: Option<InstallState>,
}

pub fn run_gateway_service() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    runtime.block_on(pioneer_gateway::run_gateway_until_shutdown())
}

pub fn start_gateway_service() -> Result<()> {
    let config = AppConfig::load().context("failed to load app config")?;
    let settings = load_service_settings_from_config(&config)?;
    platform::start_gateway_service(&settings)?;
    save_install_state_from_current_context(&config)
}

pub fn stop_gateway_service() -> Result<()> {
    let config = AppConfig::load().context("failed to load app config")?;
    let settings = load_service_settings_from_config(&config)?;
    platform::stop_gateway_service(&settings)?;
    save_install_state_from_current_context(&config)
}

pub fn issue_superuser_token() -> Result<()> {
    let config = AppConfig::load().context("failed to load app config")?;
    let runtime_home = config
        .ensure_runtime_home_dir()
        .context("failed to prepare runtime home directory")?;
    let token = pioneer_gateway::issue_superuser_token(&config, &runtime_home)
        .context("failed to issue superuser token")?;
    println!("{token}");
    Ok(())
}

pub fn secrets_status() -> Result<pioneer_gateway::SecretsStatusReport> {
    let (config, runtime_home) = load_config_and_runtime_home()?;
    block_on_gateway_operation(pioneer_gateway::secrets_status(&config, &runtime_home))
}

pub fn secrets_garbage_collection(
    dry_run: bool,
) -> Result<pioneer_gateway::McpSecretGarbageCollectionReport> {
    let (config, runtime_home) = load_config_and_runtime_home()?;
    block_on_gateway_operation(pioneer_gateway::secrets_garbage_collection(
        &config,
        &runtime_home,
        dry_run,
    ))
}

pub fn rotate_superuser_jwt_token() -> Result<pioneer_gateway::SuperuserJwtRotationReport> {
    let (config, runtime_home) = load_config_and_runtime_home()?;
    pioneer_gateway::rotate_superuser_jwt_token(&config, &runtime_home)
}

pub fn gateway_service_status() -> Result<GatewayServiceStatus> {
    let config = AppConfig::load().context("failed to load app config")?;
    let settings = load_service_settings_from_config(&config)?;
    let runtime_home = config
        .ensure_runtime_home_dir()
        .context("failed to prepare runtime home directory")?;
    let service_active = platform::is_gateway_service_active(&settings)?;
    let connect_timeout = Duration::from_millis(config.desktop.gateway.connect_timeout_ms.max(1));
    let gateway_reachable =
        is_gateway_reachable(config.gateway.listen_addr.as_str(), connect_timeout)?;
    let install_state = load_install_state(&config.install_state_path()?)?;

    Ok(GatewayServiceStatus {
        service_name: settings.service_name,
        listen_addr: config.gateway.listen_addr.trim().to_owned(),
        service_active,
        gateway_reachable,
        runtime_home: runtime_home.display().to_string(),
        install_state,
    })
}

fn load_config_and_runtime_home() -> Result<(AppConfig, PathBuf)> {
    let config = AppConfig::load().context("failed to load app config")?;
    let runtime_home = config
        .ensure_runtime_home_dir()
        .context("failed to prepare runtime home directory")?;
    Ok((config, runtime_home))
}

fn block_on_gateway_operation<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    runtime.block_on(future)
}

fn load_service_settings_from_config(config: &AppConfig) -> Result<ServiceSettings> {
    let service_name = config.gateway.service_name.trim().to_owned();

    if service_name.is_empty() {
        bail!("gateway.service_name in config must not be empty");
    }

    if service_name.contains('/') || service_name.contains('\\') {
        bail!("gateway.service_name must not contain path separators");
    }

    let mut legacy_service_names = Vec::new();
    for legacy_service_name in &config.gateway.legacy_service_names {
        let legacy_service_name = legacy_service_name.trim();
        if legacy_service_name.is_empty() {
            continue;
        }
        if legacy_service_name.contains('/') || legacy_service_name.contains('\\') {
            bail!("gateway.legacy_service_names entries must not contain path separators");
        }
        if legacy_service_name != service_name {
            legacy_service_names.push(legacy_service_name.to_owned());
        }
    }
    legacy_service_names.sort();
    legacy_service_names.dedup();

    Ok(ServiceSettings {
        service_name,
        legacy_service_names,
        runtime_home_dir: config.runtime_home_dir()?,
        macos_background_item_name: config.install.macos_background_item_name.trim().to_owned(),
        macos_associated_bundle_identifier: config
            .install
            .macos_associated_bundle_identifier
            .trim()
            .to_owned(),
    })
}

fn save_install_state_from_current_context(config: &AppConfig) -> Result<()> {
    let install_state_path = config.install_state_path()?;
    let current_exe = std::env::current_exe().context("failed to determine current executable")?;
    let install_root = current_exe.parent().map(PathBuf::from);
    let managed_by = install_managed_by_from_env().unwrap_or(InstallManagedBy::Manual);

    let state = InstallState {
        version: InstallState::CURRENT_VERSION,
        managed_by,
        installed_version: env!("CARGO_PKG_VERSION").to_owned(),
        install_root,
        binary_path: current_exe,
        updated_at_unix: unix_timestamp_secs()?,
    };

    save_install_state(&install_state_path, &state)
}

fn install_managed_by_from_env() -> Option<InstallManagedBy> {
    let value = std::env::var("PIONEER_MANAGED_BY").ok()?;
    let normalized = value.trim().to_ascii_lowercase();

    match normalized.as_str() {
        "script" => Some(InstallManagedBy::Script),
        "desktop" => Some(InstallManagedBy::Desktop),
        "manual" => Some(InstallManagedBy::Manual),
        "unknown" => Some(InstallManagedBy::Unknown),
        _ => None,
    }
}

fn unix_timestamp_secs() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs())
}

pub(super) fn resolve_service_path() -> Option<String> {
    resolve_login_shell_path()
        .or_else(|| std::env::var("PATH").ok())
        .map(|raw| raw.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn resolve_login_shell_path() -> Option<String> {
    let marker_begin = "__PIONEER_PATH_BEGIN__";
    let marker_end = "__PIONEER_PATH_END__";
    let probe = format!("printf '{marker_begin}%s{marker_end}' \"$PATH\"");

    for shell in shell_candidates() {
        let output = match Command::new(shell.as_str())
            .arg("-ilc")
            .arg(probe.as_str())
            .output()
        {
            Ok(output) => output,
            Err(_) => continue,
        };
        if !output.status.success() {
            continue;
        }

        let stdout = String::from_utf8_lossy(output.stdout.as_slice());
        let Some(begin) = stdout.find(marker_begin) else {
            continue;
        };
        let after_begin = begin + marker_begin.len();
        let Some(end_rel) = stdout[after_begin..].find(marker_end) else {
            continue;
        };
        let end = after_begin + end_rel;
        let path = stdout[after_begin..end].trim();
        if path.is_empty() {
            continue;
        }
        return Some(path.to_owned());
    }

    None
}

fn shell_candidates() -> Vec<String> {
    let mut candidates = Vec::new();
    if let Ok(shell) = std::env::var("SHELL") {
        let shell = shell.trim();
        if !shell.is_empty() {
            candidates.push(shell.to_owned());
        }
    }
    candidates.extend(
        ["/bin/zsh", "/bin/bash", "/bin/sh"]
            .iter()
            .map(|value| (*value).to_owned()),
    );
    candidates.sort();
    candidates.dedup();
    candidates
}

fn is_gateway_reachable(address: &str, connect_timeout: Duration) -> Result<bool> {
    let addrs: Vec<SocketAddr> = address
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve gateway address `{address}`"))?
        .collect();

    if addrs.is_empty() {
        bail!("gateway address `{address}` resolved to no socket addresses");
    }

    for addr in addrs {
        let addr = normalize_unspecified_addr(addr);
        if TcpStream::connect_timeout(&addr, connect_timeout).is_ok() {
            return Ok(true);
        }
    }

    Ok(false)
}

fn normalize_unspecified_addr(addr: SocketAddr) -> SocketAddr {
    match addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4.port())
        }
        SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6.port())
        }
        _ => addr,
    }
}

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
mod unsupported;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
use self::linux as platform;
#[cfg(target_os = "macos")]
use self::macos as platform;
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
use self::unsupported as platform;
#[cfg(target_os = "windows")]
use self::windows as platform;
