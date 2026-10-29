use anyhow::{Context, Result, bail};
use pioneer_config::{
    AppConfig, InstallManagedBy, InstallState, load_install_state, save_install_state,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const SERVICE_MODE_ARG: &str = "gateway-service";

pub(crate) struct ServiceSettings {
    pub service_name: String,
    pub legacy_service_names: Vec<String>,
    pub macos_associated_bundle_identifier: String,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub managed_by: InstallManagedBy,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayServiceWarning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayServiceStageTiming {
    pub stage: String,
    pub started_after_ms: u64,
    pub duration_ms: u64,
    pub outcome: String,
}

#[derive(Debug)]
pub struct GatewayServiceStartReport {
    pub warnings: Vec<GatewayServiceWarning>,
    pub stage_timings: Vec<GatewayServiceStageTiming>,
    started_at: Instant,
}

impl GatewayServiceStartReport {
    fn new() -> Self {
        Self {
            warnings: Vec::new(),
            stage_timings: Vec::new(),
            started_at: Instant::now(),
        }
    }

    pub(crate) fn observe<T>(
        &mut self,
        stage: &'static str,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let stage_started = Instant::now();
        let result = operation();
        self.stage_timings.push(GatewayServiceStageTiming {
            stage: stage.to_owned(),
            started_after_ms: duration_millis(stage_started.duration_since(self.started_at)),
            duration_ms: duration_millis(stage_started.elapsed()),
            outcome: if result.is_ok() { "ok" } else { "error" }.to_owned(),
        });
        result
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl GatewayServiceWarning {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
        }
    }
}

// An operation-local snapshot of this service's registration. Restoring it
// never activates the Gateway; the installer separately restores prior activity.
pub(crate) struct GatewayServiceRegistration {
    settings: ServiceSettings,
    snapshot: platform::RegistrationSnapshot,
}

impl GatewayServiceRegistration {
    pub(crate) fn capture(config: &AppConfig) -> Result<Self> {
        let settings = load_service_settings_from_config(config)?;
        let snapshot = platform::capture_registration(&settings)?;
        Ok(Self { settings, snapshot })
    }

    pub(crate) fn is_registered(&self) -> bool {
        self.snapshot.is_registered()
    }

    pub(crate) fn restore(&self) -> Result<()> {
        platform::restore_registration(&self.settings, &self.snapshot)
    }

    pub(crate) fn is_active(&self) -> Result<bool> {
        platform::is_gateway_service_active(&self.settings)
    }

    pub(crate) fn stop(&self) -> Result<()> {
        platform::stop_gateway_service(&self.settings)
    }
}

// LaunchAgent/systemd definitions are restored as bytes at their original path.
// Keep the absent case explicit so recovery never invents a registration.
pub(super) struct ServiceDefinitionBackup {
    path: PathBuf,
    contents: Option<(Vec<u8>, std::fs::Permissions)>,
}

impl ServiceDefinitionBackup {
    pub(super) fn capture(path: PathBuf) -> Result<Self> {
        let contents = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => {
                anyhow::ensure!(
                    metadata.is_file() && metadata.len() <= 1024 * 1024,
                    "invalid Gateway service definition"
                );
                Some((std::fs::read(&path)?, metadata.permissions()))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("service definition metadata unavailable"),
        };
        Ok(Self { path, contents })
    }

    pub(super) fn is_registered(&self) -> bool {
        self.contents.is_some()
    }

    pub(super) fn restore(&self) -> Result<()> {
        if let Some((bytes, permissions)) = &self.contents {
            let parent = self
                .path
                .parent()
                .context("service definition has no parent")?;
            std::fs::create_dir_all(parent)?;
            let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
            use std::io::Write;
            temporary.write_all(bytes)?;
            temporary.as_file().set_permissions(permissions.clone())?;
            temporary.persist(&self.path).map_err(|error| error.error)?;
        } else {
            match std::fs::remove_file(&self.path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).context("failed to restore absent service definition");
                }
            }
        }
        Ok(())
    }
}

pub fn run_gateway_service() -> Result<()> {
    let startup = pioneer_observability::GatewayStartupTrace::start();
    let runtime_stage = startup.stage(pioneer_observability::GatewayStartupStage::RuntimeBuild);
    let runtime = pioneer_gateway::build_gateway_runtime()
        .context("failed to build Gateway Tokio runtime")?;
    runtime_stage.succeed();

    runtime.block_on(pioneer_gateway::run_gateway_until_shutdown_with_startup(
        startup,
    ))
}

pub fn start_gateway_service() -> Result<GatewayServiceStartReport> {
    let mut report = GatewayServiceStartReport::new();
    let config = report.observe("service.config.load", || {
        AppConfig::load().context("failed to load app config")
    })?;
    let settings = report.observe("service.settings.load", || {
        load_service_settings_from_config(&config)
    })?;
    // Validate ownership before changing the service registration. In particular,
    // an extracted installer must never become the persistent service executable.
    let state = install_state_from_current_context(&config)?;
    report.warnings = platform::start_gateway_service(&settings, &mut report)?;
    report.observe("service.state.persist", || {
        save_install_state(&config.install_state_path()?, &state)
    })?;
    Ok(report)
}

pub fn stop_gateway_service() -> Result<()> {
    let config = AppConfig::load().context("failed to load app config")?;
    let settings = load_service_settings_from_config(&config)?;
    // Stopping changes service state, not the installed package identity. This
    // command may also be issued by a temporary installer during replacement.
    platform::stop_gateway_service(&settings)
}

pub fn create_device() -> Result<pioneer_protocol::AuthDeviceCreateResponse> {
    let (config, runtime_home) = load_config_and_runtime_home()?;
    block_on_gateway_operation(pioneer_gateway::create_device(&config, &runtime_home))
        .context("failed to create pending device session")
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
    let _runtime_observer = pioneer_observability::runtime::register(
        runtime.handle(),
        pioneer_observability::runtime::RuntimeRole::CliHelper,
    );

    _runtime_observer.block_on(&runtime, future)
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

    config.runtime_home_dir()?;
    Ok(ServiceSettings {
        service_name,
        legacy_service_names,
        macos_associated_bundle_identifier: config
            .install
            .macos_associated_bundle_identifier
            .trim()
            .to_owned(),
        managed_by: install_managed_by_from_env()
            .or(load_install_state(&config.install_state_path()?)?.map(|state| state.managed_by))
            .unwrap_or(InstallManagedBy::Manual),
    })
}

pub(super) fn current_executable_path() -> Result<PathBuf> {
    let path = std::env::current_exe().context("failed to determine current executable path")?;
    crate::installer::canonical_install_path(&path)
        .context("failed to resolve current executable path")
}

fn install_state_from_current_context(config: &AppConfig) -> Result<InstallState> {
    let current_exe = current_executable_path()?;
    let previous = load_install_state(&config.install_state_path()?)?;
    let configured_root = crate::installer::install_root_path(config)?;
    let matches_managed_root = |root: &std::path::Path| -> Result<bool> {
        let expected = crate::installer::installed_binary_path(config, root)?;
        Ok(crate::installer::canonical_install_path(&expected)
            .is_ok_and(|path| path == current_exe))
    };
    let previous_root = previous
        .as_ref()
        .and_then(|state| state.install_root.as_ref());
    let install_root = if let Some(root) = previous_root
        && matches_managed_root(root)?
    {
        Some(
            crate::installer::canonical_install_path(root)
                .context("failed to resolve install root")?,
        )
    } else if matches_managed_root(&configured_root)? {
        Some(
            crate::installer::canonical_install_path(&configured_root)
                .context("failed to resolve install root")?,
        )
    } else {
        let same_manual_binary = previous.as_ref().is_some_and(|state| {
            matches!(
                state.managed_by,
                InstallManagedBy::Manual | InstallManagedBy::Unknown
            ) && crate::installer::canonical_install_path(&state.binary_path)
                .is_ok_and(|path| path == current_exe)
        });
        anyhow::ensure!(
            install_managed_by_from_env().is_none_or(|owner| {
                matches!(owner, InstallManagedBy::Manual | InstallManagedBy::Unknown)
            }) && (previous.is_none() || same_manual_binary),
            "start must run from the installed Gateway executable; installation unchanged"
        );
        // Preserve standalone developer/manual use without borrowing a managed
        // package's identity or overwriting its install-state.
        previous_root
            .cloned()
            .or_else(|| current_exe.parent().map(PathBuf::from))
    };
    let managed_by = install_managed_by_from_env()
        .or_else(|| previous.as_ref().map(|state| state.managed_by.clone()))
        .unwrap_or(InstallManagedBy::Manual);

    Ok(InstallState {
        version: InstallState::CURRENT_VERSION,
        managed_by,
        installed_version: env!("CARGO_PKG_VERSION").to_owned(),
        install_root,
        binary_path: current_exe,
        updated_at_unix: unix_timestamp_secs()?,
    })
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
    shell_candidates_from_env(std::env::var("SHELL").ok().as_deref())
}

fn shell_candidates_from_env(shell: Option<&str>) -> Vec<String> {
    let mut candidates = Vec::new();
    if let Some(shell) = shell.map(str::trim).filter(|shell| !shell.is_empty()) {
        push_shell_candidate(&mut candidates, shell);
    }
    for fallback in ["/bin/zsh", "/bin/bash", "/bin/sh"] {
        push_shell_candidate(&mut candidates, fallback);
    }
    candidates
}

fn push_shell_candidate(candidates: &mut Vec<String>, shell: &str) {
    if !candidates.iter().any(|candidate| candidate == shell) {
        candidates.push(shell.to_owned());
    }
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

#[cfg(test)]
mod tests {
    use super::{GatewayServiceStartReport, shell_candidates_from_env};

    #[test]
    fn service_timing_report_uses_static_stage_and_outcome() {
        let mut report = GatewayServiceStartReport::new();

        report
            .observe("service.manager.activate", || Ok::<_, anyhow::Error>(()))
            .expect("stage should succeed");

        assert_eq!(report.stage_timings.len(), 1);
        assert_eq!(report.stage_timings[0].stage, "service.manager.activate");
        assert_eq!(report.stage_timings[0].outcome, "ok");
    }

    #[test]
    fn shell_candidates_preserve_user_priority_and_fallback_order() {
        for (shell, expected) in [
            (Some("/bin/zsh"), vec!["/bin/zsh", "/bin/bash", "/bin/sh"]),
            (Some("/bin/bash"), vec!["/bin/bash", "/bin/zsh", "/bin/sh"]),
            (Some("  "), vec!["/bin/zsh", "/bin/bash", "/bin/sh"]),
        ] {
            assert_eq!(shell_candidates_from_env(shell), expected, "{shell:?}");
        }
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

// Linux and Windows adapters only compose external commands, so test builds can
// type-check them on another host without requiring that platform's SDK.
#[cfg(all(test, not(target_os = "linux")))]
#[path = "linux.rs"]
#[allow(dead_code)]
mod linux_cross_platform;
#[cfg(all(test, not(target_os = "windows")))]
#[path = "windows.rs"]
#[allow(dead_code)]
mod windows_cross_platform;

#[cfg(target_os = "linux")]
use self::linux as platform;
#[cfg(target_os = "macos")]
use self::macos as platform;
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
use self::unsupported as platform;
#[cfg(target_os = "windows")]
use self::windows as platform;
