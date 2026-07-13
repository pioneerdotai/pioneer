use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use pioneer_config::{AppConfig, InstallManagedBy, InstallState, save_install_state};
use reqwest::{StatusCode, blocking::Client, header::RETRY_AFTER};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

const INSTALLER_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const INSTALLER_HTTP_IO_TIMEOUT: Duration = Duration::from_secs(120);
const INSTALLER_DOWNLOAD_MAX_ATTEMPTS: u32 = 4;
const INSTALLER_DOWNLOAD_RETRY_BASE_DELAY: Duration = Duration::from_secs(1);
const INSTALLER_DOWNLOAD_RETRY_MAX_DELAY: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallCommand {
    Install,
    Update,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseChannel {
    Stable,
    Beta,
    Canary,
}

impl ReleaseChannel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Beta => "beta",
            Self::Canary => "canary",
        }
    }
}

#[derive(Debug, Clone)]
pub enum InstallSourceSpec {
    Local {
        asset_path: PathBuf,
        checksums_path: PathBuf,
    },
    Release {
        channel: ReleaseChannel,
        version: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct InstallOptions {
    pub command: InstallCommand,
    pub source: InstallSourceSpec,
    pub managed_by: InstallManagedBy,
    pub no_start: bool,
    pub force_start: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstallReport {
    pub phase: &'static str,
    pub command: &'static str,
    pub installed_version: String,
    pub installed_binary: String,
    pub install_root: String,
    pub service_active: bool,
    pub gateway_reachable: bool,
    pub was_active: bool,
    pub started: bool,
    pub command_link_created: bool,
    pub path_updated: bool,
    pub rollback_performed: bool,
    pub error_code: Option<String>,
    pub warnings: Vec<InstallWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallWarning {
    pub code: String,
    pub message: String,
}

impl InstallWarning {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone)]
struct CommandLinkResult {
    link_created: bool,
    path_updated: bool,
    warnings: Vec<InstallWarning>,
}

#[cfg(not(windows))]
#[derive(Debug, Clone)]
struct PathUpdateResult {
    path_updated: bool,
    warnings: Vec<InstallWarning>,
}

#[derive(Debug, Deserialize)]
struct StatusOutput {
    service_active: bool,
    gateway_reachable: bool,
    #[serde(default)]
    runtime_home: String,
}

#[derive(Debug, Deserialize)]
struct StartOutput {
    #[serde(default)]
    warnings: Vec<InstallWarning>,
}

#[derive(Debug, Deserialize)]
struct VersionOutput {
    version: String,
}

#[derive(Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
}

#[derive(Debug, Clone)]
struct ServiceSnapshot {
    service_active: bool,
    gateway_reachable: bool,
    runtime_home: Option<PathBuf>,
}

#[derive(Debug)]
struct ResolvedInstallSource {
    asset_path: PathBuf,
    checksums_path: PathBuf,
    _temp_dir: Option<TempDir>,
}

#[derive(Debug)]
pub(crate) struct InstallerTransientDownloadError {
    url: String,
    attempts: u32,
    last_error: String,
}

impl fmt::Display for InstallerTransientDownloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "download of `{}` failed after {} attempts due to a transient network failure: {}; check network access and retry the command",
            self.url, self.attempts, self.last_error
        )
    }
}

impl Error for InstallerTransientDownloadError {}

pub(crate) fn is_transient_download_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<InstallerTransientDownloadError>()
        .is_some()
}

#[derive(Debug, Clone, Copy)]
struct DownloadRetryPolicy {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
}

impl DownloadRetryPolicy {
    const fn installer_default() -> Self {
        Self {
            max_attempts: INSTALLER_DOWNLOAD_MAX_ATTEMPTS,
            base_delay: INSTALLER_DOWNLOAD_RETRY_BASE_DELAY,
            max_delay: INSTALLER_DOWNLOAD_RETRY_MAX_DELAY,
        }
    }

    fn delay_after_attempt(self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        if let Some(delay) = retry_after {
            return delay.min(self.max_delay);
        }

        let exponent = attempt.saturating_sub(1).min(10);
        self.base_delay
            .saturating_mul(1_u32 << exponent)
            .min(self.max_delay)
    }
}

enum DownloadAttemptFailure {
    Retryable {
        message: String,
        retry_after: Option<Duration>,
    },
    Fatal(anyhow::Error),
}

pub fn run_install(options: InstallOptions) -> Result<InstallReport> {
    let config = AppConfig::load().context("failed to load app config for install/update")?;

    let source = resolve_install_source(&options)
        .context("failed to resolve installer source for install/update")?;

    let install_root = install_root_path(&config)?;
    let bin_dir = install_root.join("bin");
    let target_binary = bin_dir.join(config.install_binary_file_name()?);
    let staged_binary = bin_dir.join(config.install_staged_binary_file_name()?);
    let rollback_binary = bin_dir.join(config.install_rollback_binary_file_name()?);
    fs::create_dir_all(&bin_dir)
        .with_context(|| format!("failed to create install directory `{}`", bin_dir.display()))?;

    let asset_name = source
        .asset_path
        .file_name()
        .and_then(OsStr::to_str)
        .context("asset path has no file name")?;
    let expected_sha = expected_checksum_for_asset(&source.checksums_path, asset_name)?;
    let actual_sha = sha256_file(&source.asset_path)?;
    if expected_sha != actual_sha {
        bail!(
            "checksum mismatch for {}: expected {}, got {}",
            asset_name,
            expected_sha,
            actual_sha
        );
    }

    if staged_binary.exists() {
        let _ = fs::remove_file(&staged_binary);
    }
    unpack_asset_to_binary(&source.asset_path, &staged_binary)?;
    make_binary_executable(&staged_binary)?;

    let existing = target_binary.is_file();
    let mut was_active = false;
    let mut runtime_home: Option<PathBuf> = None;
    let mut config_backup_dir: Option<PathBuf> = None;
    if existing {
        if let Some(snapshot) = query_gateway_status(&target_binary)? {
            was_active = snapshot.service_active;
            runtime_home = snapshot.runtime_home;
        }

        if let Some(home) = runtime_home.as_ref() {
            let backup_dir = bin_dir.join(format!("config.rollback.{}", unix_timestamp_secs()?));
            if backup_runtime_toml(home, &backup_dir)? {
                config_backup_dir = Some(backup_dir);
            }
        }
    }

    let should_start = !options.no_start && (options.force_start || !existing || was_active);
    let mut warnings: Vec<InstallWarning> = Vec::new();
    if was_active {
        run_gateway_command(
            &target_binary,
            &["stop"],
            &options.managed_by,
            false,
            "stop",
        )?;
    }

    if rollback_binary.exists() {
        let _ = fs::remove_file(&rollback_binary);
    }

    if existing {
        fs::rename(&target_binary, &rollback_binary).with_context(|| {
            format!(
                "failed to move existing binary `{}` to rollback `{}`",
                target_binary.display(),
                rollback_binary.display()
            )
        })?;
    }

    if let Err(error) = fs::rename(&staged_binary, &target_binary) {
        restore_binary_and_config(
            &target_binary,
            &rollback_binary,
            runtime_home.as_deref(),
            config_backup_dir.as_deref(),
        );
        return Err(error).with_context(|| {
            format!(
                "failed to replace binary with staged file `{}`",
                target_binary.display()
            )
        });
    }

    let installed_version = match query_binary_version(&target_binary) {
        Ok(version) => version,
        Err(error) => {
            restore_binary_and_config(
                &target_binary,
                &rollback_binary,
                runtime_home.as_deref(),
                config_backup_dir.as_deref(),
            );
            maybe_restore_service_after_rollback(
                &target_binary,
                &rollback_binary,
                was_active,
                &options.managed_by,
            );
            bail!("installed binary failed version probe: {error:#}; update rolled back");
        }
    };

    let link_result = match ensure_global_command_link(&config, &target_binary) {
        Ok(result) => result,
        Err(error) => {
            restore_binary_and_config(
                &target_binary,
                &rollback_binary,
                runtime_home.as_deref(),
                config_backup_dir.as_deref(),
            );
            maybe_restore_service_after_rollback(
                &target_binary,
                &rollback_binary,
                was_active,
                &options.managed_by,
            );
            bail!("failed to expose command globally: {error:#}; update rolled back");
        }
    };
    let command_link_created = link_result.link_created;
    let path_updated = link_result.path_updated;
    warnings.extend(link_result.warnings);

    if should_start {
        match run_gateway_start_command(&target_binary, &options.managed_by) {
            Ok(start_warnings) => warnings.extend(start_warnings),
            Err(error) => {
                rollback_and_restore(
                    &target_binary,
                    &rollback_binary,
                    runtime_home.as_deref(),
                    config_backup_dir.as_deref(),
                    was_active,
                    &options.managed_by,
                );
                bail!("failed to start gateway after update: {error:#}; update rolled back");
            }
        };

        if !wait_for_gateway_health(&target_binary, Duration::from_secs(30)) {
            let _ = run_gateway_command(
                &target_binary,
                &["stop"],
                &options.managed_by,
                true,
                "stop during rollback",
            );
            rollback_and_restore(
                &target_binary,
                &rollback_binary,
                runtime_home.as_deref(),
                config_backup_dir.as_deref(),
                was_active,
                &options.managed_by,
            );
            bail!("gateway failed health check after update; update rolled back");
        }
    }

    if let Err(error) = persist_install_state(
        &options.managed_by,
        &target_binary,
        &install_root,
        installed_version.as_str(),
    ) {
        rollback_and_restore(
            &target_binary,
            &rollback_binary,
            runtime_home.as_deref(),
            config_backup_dir.as_deref(),
            was_active,
            &options.managed_by,
        );
        bail!("failed to persist install-state: {error:#}; update rolled back");
    }

    if rollback_binary.exists() {
        let _ = fs::remove_file(&rollback_binary);
    }
    if let Some(backup_dir) = config_backup_dir.as_ref() {
        let _ = fs::remove_dir_all(backup_dir);
    }

    let post_status = query_gateway_status(&target_binary)?.unwrap_or(ServiceSnapshot {
        service_active: false,
        gateway_reachable: false,
        runtime_home: None,
    });

    Ok(InstallReport {
        phase: match options.command {
            InstallCommand::Install => "installed",
            InstallCommand::Update => "updated",
        },
        command: match options.command {
            InstallCommand::Install => "install",
            InstallCommand::Update => "update",
        },
        installed_version,
        installed_binary: target_binary.display().to_string(),
        install_root: install_root.display().to_string(),
        service_active: post_status.service_active,
        gateway_reachable: post_status.gateway_reachable,
        was_active,
        started: should_start,
        command_link_created,
        path_updated,
        rollback_performed: false,
        error_code: None,
        warnings,
    })
}

fn resolve_install_source(options: &InstallOptions) -> Result<ResolvedInstallSource> {
    match &options.source {
        InstallSourceSpec::Local {
            asset_path,
            checksums_path,
        } => {
            if !asset_path.is_file() {
                bail!("asset file does not exist: {}", asset_path.display());
            }
            if !checksums_path.is_file() {
                bail!(
                    "checksums file does not exist: {}",
                    checksums_path.display()
                );
            }
            Ok(ResolvedInstallSource {
                asset_path: asset_path.clone(),
                checksums_path: checksums_path.clone(),
                _temp_dir: None,
            })
        }
        InstallSourceSpec::Release { channel, version } => {
            resolve_release_install_source(*channel, version.as_deref())
        }
    }
}

fn resolve_release_install_source(
    channel: ReleaseChannel,
    version: Option<&str>,
) -> Result<ResolvedInstallSource> {
    let repo =
        std::env::var("PIONEER_RELEASE_REPO").unwrap_or_else(|_| "pioneerdotai/pioneer".into());
    let api_base = std::env::var("PIONEER_RELEASE_API_BASE")
        .unwrap_or_else(|_| format!("https://api.github.com/repos/{repo}/releases"));
    let download_base = std::env::var("PIONEER_RELEASE_DOWNLOAD_BASE")
        .unwrap_or_else(|_| format!("https://github.com/{repo}/releases/download"));

    let client = Client::builder()
        .user_agent("pioneer-installer/1.0")
        .connect_timeout(INSTALLER_HTTP_CONNECT_TIMEOUT)
        .timeout(INSTALLER_HTTP_IO_TIMEOUT)
        .build()
        .context("failed to initialize HTTP client")?;

    let tag = resolve_release_tag(&client, &api_base, channel, version)?;
    let asset_name = gateway_asset_file_name()?;
    let temp_dir = TempDir::new().context("failed to allocate temporary download directory")?;
    let asset_path = temp_dir.path().join(asset_name.as_str());
    let checksums_path = temp_dir.path().join("SHA256SUMS");

    let asset_url = format!("{download_base}/{tag}/{asset_name}");
    let checksums_url = format!("{download_base}/{tag}/SHA256SUMS");

    download_release_asset(&client, asset_url.as_str(), asset_path.as_path())
        .with_context(|| format!("failed to download gateway asset from release `{tag}`"))?;
    download_release_asset(&client, checksums_url.as_str(), checksums_path.as_path())
        .with_context(|| format!("failed to download SHA256SUMS from release `{tag}`"))?;

    Ok(ResolvedInstallSource {
        asset_path,
        checksums_path,
        _temp_dir: Some(temp_dir),
    })
}

fn resolve_release_tag(
    client: &Client,
    api_base: &str,
    channel: ReleaseChannel,
    version: Option<&str>,
) -> Result<String> {
    if let Some(pinned_version) = version {
        return Ok(normalize_version_tag(pinned_version));
    }

    if matches!(channel, ReleaseChannel::Stable) {
        let url = format!("{api_base}/latest");
        let release = client
            .get(url.as_str())
            .send()
            .with_context(|| format!("failed to fetch latest release metadata from `{url}`"))?
            .error_for_status()
            .with_context(|| format!("release API request failed for `{url}`"))?
            .json::<GithubRelease>()
            .context("failed to parse latest release metadata")?;
        let tag = release.tag_name.trim().to_owned();
        if tag.is_empty() {
            bail!("latest release payload does not include `tag_name`");
        }
        return Ok(tag);
    }

    let url = format!("{api_base}?per_page=100");
    let releases = client
        .get(url.as_str())
        .send()
        .with_context(|| format!("failed to fetch release list from `{url}`"))?
        .error_for_status()
        .with_context(|| format!("release API request failed for `{url}`"))?
        .json::<Vec<GithubRelease>>()
        .context("failed to parse release list response")?;

    let needle = format!("-{}", channel.as_str());
    if let Some(tag) = releases.into_iter().find_map(|release| {
        let tag = release.tag_name.trim().to_owned();
        if !tag.is_empty() && tag.contains(needle.as_str()) {
            Some(tag)
        } else {
            None
        }
    }) {
        return Ok(tag);
    }

    bail!(
        "failed to find release tag for channel `{}` from release list",
        channel.as_str()
    )
}

fn normalize_version_tag(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with('v') || trimmed.starts_with('V') {
        trimmed.to_owned()
    } else {
        format!("v{trimmed}")
    }
}

fn download_release_asset(client: &Client, url: &str, destination: &Path) -> Result<()> {
    download_release_asset_with_policy(
        client,
        url,
        destination,
        DownloadRetryPolicy::installer_default(),
    )
}

fn download_release_asset_with_policy(
    client: &Client,
    url: &str,
    destination: &Path,
    policy: DownloadRetryPolicy,
) -> Result<()> {
    let partial_path = download_partial_path(destination);
    let max_attempts = policy.max_attempts.max(1);

    for attempt in 1..=max_attempts {
        match download_release_asset_once(client, url, destination, partial_path.as_path()) {
            Ok(()) => return Ok(()),
            Err(DownloadAttemptFailure::Fatal(error)) => {
                let _ = fs::remove_file(partial_path.as_path());
                return Err(error);
            }
            Err(DownloadAttemptFailure::Retryable {
                message,
                retry_after,
            }) if attempt < max_attempts => {
                let _ = fs::remove_file(partial_path.as_path());
                let delay = policy.delay_after_attempt(attempt, retry_after);
                tracing::warn!(
                    url,
                    attempt,
                    max_attempts,
                    retry_delay_ms = delay.as_millis(),
                    error = message,
                    "release asset download attempt failed; retrying"
                );
                thread::sleep(delay);
            }
            Err(DownloadAttemptFailure::Retryable { message, .. }) => {
                let _ = fs::remove_file(partial_path.as_path());
                return Err(InstallerTransientDownloadError {
                    url: url.to_owned(),
                    attempts: max_attempts,
                    last_error: message,
                }
                .into());
            }
        }
    }

    unreachable!("download retry loop always returns")
}

fn download_release_asset_once(
    client: &Client,
    url: &str,
    destination: &Path,
    partial_path: &Path,
) -> std::result::Result<(), DownloadAttemptFailure> {
    let mut response = match client.get(url).send() {
        Ok(response) => response,
        Err(error) if is_retryable_reqwest_error(&error) => {
            return Err(DownloadAttemptFailure::Retryable {
                message: error.to_string(),
                retry_after: None,
            });
        }
        Err(error) => {
            return Err(DownloadAttemptFailure::Fatal(
                anyhow::Error::new(error).context(format!("failed to download `{url}`")),
            ));
        }
    };

    let status = response.status();
    if !status.is_success() {
        let retry_after = retry_after_delay(&response);
        let error = response
            .error_for_status()
            .expect_err("non-success response must fail status validation");
        if is_retryable_download_status(status) {
            return Err(DownloadAttemptFailure::Retryable {
                message: error.to_string(),
                retry_after,
            });
        }
        return Err(DownloadAttemptFailure::Fatal(
            anyhow::Error::new(error).context(format!("release asset request failed for `{url}`")),
        ));
    }

    let mut file = File::create(partial_path).map_err(|error| {
        DownloadAttemptFailure::Fatal(anyhow::Error::new(error).context(format!(
            "failed to create partial download `{}`",
            partial_path.display()
        )))
    })?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read =
            response
                .read(&mut buffer)
                .map_err(|error| DownloadAttemptFailure::Retryable {
                    message: format!("failed to read response body from `{url}`: {error}"),
                    retry_after: None,
                })?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read]).map_err(|error| {
            DownloadAttemptFailure::Fatal(anyhow::Error::new(error).context(format!(
                "failed to write downloaded payload into `{}`",
                partial_path.display()
            )))
        })?;
    }
    file.flush().map_err(|error| {
        DownloadAttemptFailure::Fatal(anyhow::Error::new(error).context(format!(
            "failed to flush downloaded payload `{}`",
            partial_path.display()
        )))
    })?;
    drop(file);

    if destination.exists() {
        fs::remove_file(destination).map_err(|error| {
            DownloadAttemptFailure::Fatal(anyhow::Error::new(error).context(format!(
                "failed to remove previous download `{}`",
                destination.display()
            )))
        })?;
    }
    fs::rename(partial_path, destination).map_err(|error| {
        DownloadAttemptFailure::Fatal(anyhow::Error::new(error).context(format!(
            "failed to finalize download `{}`",
            destination.display()
        )))
    })?;
    Ok(())
}

fn download_partial_path(destination: &Path) -> PathBuf {
    let mut path = destination.as_os_str().to_os_string();
    path.push(".part");
    PathBuf::from(path)
}

fn is_retryable_reqwest_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
}

fn is_retryable_download_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

fn retry_after_delay(response: &reqwest::blocking::Response) -> Option<Duration> {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn gateway_asset_file_name() -> Result<String> {
    let arch = match std::env::consts::ARCH {
        "x86_64" | "amd64" => "x86_64",
        "aarch64" | "arm64" => "aarch64",
        other => bail!("unsupported architecture `{other}` for release installer source"),
    };

    let os = match std::env::consts::OS {
        "linux" => "linux",
        "macos" => "macos",
        "windows" => "windows",
        other => bail!("unsupported OS `{other}` for release installer source"),
    };

    let variant = if cfg!(feature = "computer-use") {
        "-computer-use"
    } else {
        ""
    };
    let ext = if os == "windows" || (os == "macos" && arch == "x86_64") {
        "zip"
    } else {
        "gz"
    };
    Ok(format!("pioneer-gateway-{os}-{arch}{variant}.{ext}"))
}

fn persist_install_state(
    managed_by: &InstallManagedBy,
    target_binary: &Path,
    install_root: &Path,
    installed_version: &str,
) -> Result<()> {
    let config = AppConfig::load().context("failed to load app config for install-state update")?;
    let install_state_path = config
        .install_state_path()
        .context("failed to resolve install-state path")?;
    let state = InstallState {
        version: InstallState::CURRENT_VERSION,
        managed_by: managed_by.clone(),
        installed_version: installed_version.to_owned(),
        install_root: Some(install_root.to_path_buf()),
        binary_path: target_binary.to_path_buf(),
        updated_at_unix: unix_timestamp_secs()?,
    };
    save_install_state(&install_state_path, &state).with_context(|| {
        format!(
            "failed to save install-state at `{}`",
            install_state_path.display()
        )
    })
}

fn rollback_and_restore(
    target_binary: &Path,
    rollback_binary: &Path,
    runtime_home: Option<&Path>,
    config_backup_dir: Option<&Path>,
    was_active: bool,
    managed_by: &InstallManagedBy,
) {
    restore_binary_and_config(
        target_binary,
        rollback_binary,
        runtime_home,
        config_backup_dir,
    );
    maybe_restore_service_after_rollback(target_binary, rollback_binary, was_active, managed_by);
}

fn restore_binary_and_config(
    target_binary: &Path,
    rollback_binary: &Path,
    runtime_home: Option<&Path>,
    config_backup_dir: Option<&Path>,
) {
    if rollback_binary.is_file() {
        let _ = fs::rename(rollback_binary, target_binary);
    }
    if let (Some(home), Some(backup_dir)) = (runtime_home, config_backup_dir) {
        let _ = restore_runtime_toml(home, backup_dir);
    }
}

fn maybe_restore_service_after_rollback(
    target_binary: &Path,
    rollback_binary: &Path,
    was_active: bool,
    managed_by: &InstallManagedBy,
) {
    if !was_active || !target_binary.is_file() || rollback_binary.is_file() {
        return;
    }

    let _ = run_gateway_command(
        target_binary,
        &["start"],
        managed_by,
        true,
        "rollback start",
    );
}

fn query_gateway_status(binary: &Path) -> Result<Option<ServiceSnapshot>> {
    let output = match Command::new(binary).arg("status").arg("--json").output() {
        Ok(output) => output,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to run `{}` status --json", binary.display()));
        }
    };

    if !output.status.success() {
        return Ok(None);
    }

    let parsed: StatusOutput = match serde_json::from_slice(&output.stdout) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(None),
    };

    let runtime_home = {
        let trimmed = parsed.runtime_home.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(PathBuf::from(trimmed))
        }
    };

    Ok(Some(ServiceSnapshot {
        service_active: parsed.service_active,
        gateway_reachable: parsed.gateway_reachable,
        runtime_home,
    }))
}

fn query_binary_version(binary: &Path) -> Result<String> {
    let output = Command::new(binary)
        .arg("version")
        .arg("--json")
        .output()
        .with_context(|| format!("failed to run `{}` version --json", binary.display()))?;

    if !output.status.success() {
        bail!(
            "command `{}` failed during version probe: {}",
            render_binary_command(binary, &["version", "--json"]),
            command_output_details(&output)
        );
    }

    let parsed: VersionOutput =
        serde_json::from_slice(&output.stdout).context("failed to parse version probe output")?;
    let version = parsed.version.trim();
    if version.is_empty() {
        bail!("version probe output did not include a version");
    }

    Ok(version.to_owned())
}

fn wait_for_gateway_health(binary: &Path, timeout: Duration) -> bool {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if let Ok(Some(status)) = query_gateway_status(binary)
            && status.service_active
        {
            let reachable = Command::new(binary)
                .arg("status")
                .arg("--json")
                .output()
                .ok()
                .and_then(|output| {
                    if !output.status.success() {
                        return None;
                    }
                    serde_json::from_slice::<StatusOutput>(&output.stdout).ok()
                })
                .is_some_and(|status| status.service_active && status.gateway_reachable);

            if reachable {
                return true;
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
    false
}

fn run_gateway_start_command(
    binary: &Path,
    managed_by: &InstallManagedBy,
) -> Result<Vec<InstallWarning>> {
    let args = ["start", "--json"];
    let mut command = Command::new(binary);
    command.args(args.as_slice());
    command.env("PIONEER_MANAGED_BY", managed_by_label(managed_by));

    let output = command.output().with_context(|| {
        format!(
            "failed to run `{}`",
            render_binary_command(binary, args.as_slice())
        )
    })?;

    if output.status.success() {
        return Ok(parse_start_warnings(&output.stdout));
    }

    bail!(
        "command `{}` failed during start: {}",
        render_binary_command(binary, args.as_slice()),
        command_output_details(&output)
    )
}

fn parse_start_warnings(stdout: &[u8]) -> Vec<InstallWarning> {
    serde_json::from_slice::<StartOutput>(stdout)
        .map(|output| output.warnings)
        .unwrap_or_default()
}

fn run_gateway_command(
    binary: &Path,
    args: &[&str],
    managed_by: &InstallManagedBy,
    ignore_failure: bool,
    label: &str,
) -> Result<()> {
    let mut command = Command::new(binary);
    command.args(args);
    command.env("PIONEER_MANAGED_BY", managed_by_label(managed_by));

    let output = match command.output() {
        Ok(output) => output,
        Err(error) if ignore_failure => {
            let _ = error;
            return Ok(());
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to run `{}` {}", binary.display(), args.join(" "))
            });
        }
    };

    if output.status.success() || ignore_failure {
        return Ok(());
    }

    bail!(
        "command `{}` failed during {}: {}",
        render_binary_command(binary, args),
        label,
        command_output_details(&output)
    )
}

fn render_binary_command(binary: &Path, args: &[&str]) -> String {
    if args.is_empty() {
        binary.display().to_string()
    } else {
        format!("{} {}", binary.display(), args.join(" "))
    }
}

fn command_output_details(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        format!("exit status {}", output.status)
    }
}

fn expected_checksum_for_asset(checksums_path: &Path, asset_name: &str) -> Result<String> {
    let content = fs::read_to_string(checksums_path).with_context(|| {
        format!(
            "failed to read checksums file `{}`",
            checksums_path.display()
        )
    })?;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(prefix_hash) = parts.next() else {
            continue;
        };
        let Some(name) = parts.next() else {
            continue;
        };
        if name == asset_name {
            return Ok(prefix_hash
                .trim_start_matches("sha256:")
                .to_ascii_lowercase());
        }
    }

    bail!(
        "checksum for `{}` not found in `{}`",
        asset_name,
        checksums_path.display()
    )
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open file `{}`", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read `{}`", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn unpack_asset_to_binary(asset_path: &Path, target_binary: &Path) -> Result<()> {
    let is_zip = asset_path
        .extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"));

    if is_zip {
        unpack_zip_asset(asset_path, target_binary)
    } else {
        unpack_gzip_asset(asset_path, target_binary)
    }
}

fn unpack_gzip_asset(asset_path: &Path, target_binary: &Path) -> Result<()> {
    let input = File::open(asset_path)
        .with_context(|| format!("failed to open asset `{}`", asset_path.display()))?;
    let mut decoder = GzDecoder::new(input);
    let mut output = File::create(target_binary).with_context(|| {
        format!(
            "failed to create staged binary `{}`",
            target_binary.display()
        )
    })?;
    std::io::copy(&mut decoder, &mut output).with_context(|| {
        format!(
            "failed to unpack gateway archive into `{}`",
            target_binary.display()
        )
    })?;
    Ok(())
}

fn unpack_zip_asset(asset_path: &Path, target_binary: &Path) -> Result<()> {
    let input = File::open(asset_path)
        .with_context(|| format!("failed to open asset `{}`", asset_path.display()))?;
    let mut archive = zip::ZipArchive::new(input)
        .with_context(|| format!("failed to parse zip archive `{}`", asset_path.display()))?;

    let expected_binary_name = if cfg!(windows) {
        "pioneer.exe"
    } else {
        "pioneer"
    };
    #[cfg(target_os = "macos")]
    let target_dir = target_binary
        .parent()
        .context("staged binary path has no parent directory")?;
    let mut found = false;
    #[cfg(target_os = "macos")]
    let mut companion_dylibs = 0usize;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).with_context(|| {
            format!(
                "failed to read entry #{index} in `{}`",
                asset_path.display()
            )
        })?;
        let file_name = Path::new(entry.name())
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or_default()
            .to_ascii_lowercase();

        if file_name == expected_binary_name {
            let mut output = File::create(target_binary).with_context(|| {
                format!(
                    "failed to create staged binary `{}`",
                    target_binary.display()
                )
            })?;
            std::io::copy(&mut entry, &mut output).with_context(|| {
                format!(
                    "failed to unpack {expected_binary_name} to `{}`",
                    target_binary.display()
                )
            })?;
            found = true;
            continue;
        }

        #[cfg(target_os = "macos")]
        {
            if file_name.starts_with("libonnxruntime") && file_name.ends_with(".dylib") {
                let dylib_path = target_dir.join(file_name.as_str());
                let mut output = File::create(&dylib_path).with_context(|| {
                    format!(
                        "failed to create companion dylib `{}`",
                        dylib_path.display()
                    )
                })?;
                std::io::copy(&mut entry, &mut output).with_context(|| {
                    format!(
                        "failed to unpack companion dylib `{}`",
                        dylib_path.display()
                    )
                })?;
                companion_dylibs = companion_dylibs.saturating_add(1);
            }
        }
    }

    if !found {
        bail!(
            "zip archive `{}` does not contain {expected_binary_name}",
            asset_path.display(),
        );
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    if companion_dylibs == 0 {
        bail!(
            "zip archive `{}` does not contain libonnxruntime dylibs",
            asset_path.display(),
        );
    }

    Ok(())
}

fn make_binary_executable(path: &Path) -> Result<()> {
    #[cfg(not(windows))]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)
            .with_context(|| format!("failed to stat `{}`", path.display()))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).with_context(|| {
            format!(
                "failed to set executable permissions on `{}`",
                path.display()
            )
        })?;
    }

    #[cfg(windows)]
    {
        let _ = path;
    }

    Ok(())
}

fn backup_runtime_toml(runtime_home: &Path, backup_dir: &Path) -> Result<bool> {
    if !runtime_home.is_dir() {
        return Ok(false);
    }

    fs::create_dir_all(backup_dir)
        .with_context(|| format!("failed to create backup dir `{}`", backup_dir.display()))?;
    let mut copied = 0usize;

    for entry in fs::read_dir(runtime_home)
        .with_context(|| format!("failed to list `{}`", runtime_home.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let is_toml = path
            .extension()
            .and_then(OsStr::to_str)
            .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"));
        if !is_toml {
            continue;
        }

        let target = backup_dir.join(entry.file_name());
        fs::copy(&path, &target).with_context(|| {
            format!(
                "failed to backup config `{}` to `{}`",
                path.display(),
                target.display()
            )
        })?;
        copied += 1;
    }

    if copied == 0 {
        let _ = fs::remove_dir(backup_dir);
        Ok(false)
    } else {
        Ok(true)
    }
}

fn restore_runtime_toml(runtime_home: &Path, backup_dir: &Path) -> Result<()> {
    if !backup_dir.is_dir() {
        return Ok(());
    }
    fs::create_dir_all(runtime_home)
        .with_context(|| format!("failed to create runtime home `{}`", runtime_home.display()))?;

    for entry in fs::read_dir(backup_dir)
        .with_context(|| format!("failed to list `{}`", backup_dir.display()))?
    {
        let entry = entry?;
        let src = entry.path();
        if !src.is_file() {
            continue;
        }
        let dst = runtime_home.join(entry.file_name());
        fs::copy(&src, &dst).with_context(|| {
            format!(
                "failed to restore config `{}` to `{}`",
                src.display(),
                dst.display()
            )
        })?;
    }

    Ok(())
}

fn install_root_path(config: &AppConfig) -> Result<PathBuf> {
    if let Some(value) = std::env::var_os("PIONEER_INSTALL_ROOT") {
        return Ok(PathBuf::from(value));
    }

    let base = dirs::data_local_dir().or_else(|| {
        #[cfg(windows)]
        {
            std::env::var_os("LOCALAPPDATA").map(PathBuf::from)
        }
        #[cfg(not(windows))]
        {
            dirs::home_dir().map(|home| home.join(".local").join("share"))
        }
    });

    let base = base.context("failed to resolve local data directory for current user")?;

    #[cfg(windows)]
    {
        return Ok(base
            .join(config.install_root_directory_name()?)
            .join(config.install_managed_directory_name()?));
    }

    #[cfg(not(windows))]
    {
        Ok(base
            .join(config.install_root_directory_name()?)
            .join(config.install_managed_directory_name()?))
    }
}

fn ensure_global_command_link(
    config: &AppConfig,
    target_binary: &Path,
) -> Result<CommandLinkResult> {
    #[cfg(windows)]
    {
        ensure_windows_path(config, target_binary)
    }

    #[cfg(not(windows))]
    {
        ensure_unix_symlink(config, target_binary)
    }
}

fn force_path_update_warning() -> bool {
    std::env::var("PIONEER_FORCE_PATH_WARNING")
        .ok()
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

#[cfg(not(windows))]
fn ensure_unix_symlink(config: &AppConfig, target_binary: &Path) -> Result<CommandLinkResult> {
    use std::os::unix::fs::symlink;

    let link_path_overridden = std::env::var_os("PIONEER_LINK_PATH").is_some();
    let link_path = if let Some(path) = std::env::var_os("PIONEER_LINK_PATH") {
        PathBuf::from(path)
    } else {
        let home = dirs::home_dir()
            .context("failed to resolve current user home directory for command link")?;
        home.join(".local")
            .join("bin")
            .join(config.install_command_file_name()?)
    };
    let mut warnings = Vec::new();
    let mut path_updated = true;

    if let Some(parent) = link_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create link dir `{}`", parent.display()))?;
    }

    if link_path.exists() {
        let metadata = fs::symlink_metadata(&link_path).with_context(|| {
            format!(
                "failed to read existing link metadata `{}`",
                link_path.display()
            )
        })?;
        if metadata.file_type().is_dir() {
            bail!(
                "cannot replace directory at command link path `{}`",
                link_path.display()
            );
        }
        fs::remove_file(&link_path).with_context(|| {
            format!(
                "failed to remove existing command link `{}`",
                link_path.display()
            )
        })?;
    }

    symlink(target_binary, &link_path).with_context(|| {
        format!(
            "failed to create command symlink `{}` -> `{}`",
            link_path.display(),
            target_binary.display()
        )
    })?;

    if !link_path_overridden && let Some(bin_dir) = link_path.parent() {
        if force_path_update_warning() {
            path_updated = false;
            warnings.push(InstallWarning::new(
                "path_update_skipped",
                "automatic PATH update skipped because PIONEER_FORCE_PATH_WARNING is set",
            ));
        } else {
            let path_setup = ensure_unix_user_path_configured(bin_dir)?;
            path_updated = path_setup.path_updated;
            warnings.extend(path_setup.warnings);
        }
    }

    Ok(CommandLinkResult {
        link_created: true,
        path_updated,
        warnings,
    })
}

#[cfg(windows)]
fn ensure_windows_path(config: &AppConfig, target_binary: &Path) -> Result<CommandLinkResult> {
    let mut warnings = Vec::new();
    let command_binary = windows_command_binary_path(config)?;
    let command_binary_parent = command_binary.parent().with_context(|| {
        format!(
            "command binary path has no parent directory: `{}`",
            command_binary.display()
        )
    })?;

    fs::create_dir_all(command_binary_parent).with_context(|| {
        format!(
            "failed to create Windows command directory `{}`",
            command_binary_parent.display()
        )
    })?;

    if command_binary_parent.is_file() {
        bail!(
            "Windows command path parent is unexpectedly a file: `{}`",
            command_binary_parent.display()
        );
    }

    if command_binary != target_binary {
        fs::copy(target_binary, &command_binary).with_context(|| {
            format!(
                "failed to copy installed binary `{}` to user command path `{}`",
                target_binary.display(),
                command_binary.display()
            )
        })?;
    }

    if force_path_update_warning() {
        return Ok(CommandLinkResult {
            link_created: true,
            path_updated: false,
            warnings: vec![InstallWarning::new(
                "path_update_skipped",
                "automatic PATH update skipped because PIONEER_FORCE_PATH_WARNING is set",
            )],
        });
    }

    let bin_dir = command_binary
        .parent()
        .context("command binary has no parent directory")?;
    let bin_dir_escaped = powershell_single_quote(&bin_dir.display().to_string());

    let script = format!(
        r#"$bin = '{bin_dir_escaped}'
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        if ($null -eq $userPath) {{ $userPath = '' }}
        $parts = $userPath -split ';' | ForEach-Object {{ $_.Trim() }} | Where-Object {{ $_ -ne '' }}
        if ($parts -contains $bin) {{ exit 0 }}
        $newPath = if ($userPath.TrimEnd(';').Length -eq 0) {{ $bin }} else {{ $userPath.TrimEnd(';') + ';' + $bin }}
        [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')"#
    );

    let output = Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &script,
        ])
        .output()
        .context("failed to update user PATH")?;

    let mut path_updated = true;
    if !output.status.success() {
        path_updated = false;
        warnings.push(InstallWarning::new(
            "path_update_skipped",
            format!(
                "failed to update user PATH automatically: {}",
                command_output_details(&output)
            ),
        ));
    }

    Ok(CommandLinkResult {
        link_created: true,
        path_updated,
        warnings,
    })
}

#[cfg(windows)]
fn powershell_single_quote(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(windows)]
fn windows_command_binary_path(config: &AppConfig) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("PIONEER_LINK_PATH") {
        return Ok(PathBuf::from(path));
    }

    let base =
        dirs::data_local_dir().or_else(|| std::env::var_os("LOCALAPPDATA").map(PathBuf::from));
    let base = base.context("failed to resolve LOCALAPPDATA for user command path")?;
    Ok(base
        .join(config.install_root_directory_name()?)
        .join("bin")
        .join(config.install_command_file_name()?))
}

#[cfg(not(windows))]
fn ensure_unix_user_path_configured(bin_dir: &Path) -> Result<PathUpdateResult> {
    let path_env = std::env::var("PATH").unwrap_or_default();
    if unix_path_contains_dir(path_env.as_str(), bin_dir) {
        return Ok(PathUpdateResult {
            path_updated: true,
            warnings: Vec::new(),
        });
    }

    let home =
        dirs::home_dir().context("failed to resolve current user home directory for PATH setup")?;
    let export_line = unix_path_export_line(bin_dir, home.as_path());
    let profile_files = unix_path_profile_update_candidates(home.as_path());
    let mut warnings = Vec::new();
    let mut any_profile_updated = false;

    for profile_path in profile_files {
        match append_path_export_if_missing(
            profile_path.as_path(),
            export_line.as_str(),
            bin_dir,
            home.as_path(),
        ) {
            Ok(()) => {
                any_profile_updated = true;
            }
            Err(error) => {
                warnings.push(InstallWarning::new(
                    "path_update_skipped",
                    format!(
                        "failed to update PATH profile `{}`: {error:#}",
                        profile_path.display()
                    ),
                ));
            }
        }
    }

    Ok(PathUpdateResult {
        path_updated: any_profile_updated,
        warnings,
    })
}

#[cfg(not(windows))]
fn unix_path_export_line(bin_dir: &Path, home: &Path) -> String {
    let bin_dir_expr = if bin_dir == home.join(".local").join("bin") {
        "$HOME/.local/bin".to_owned()
    } else {
        unix_shell_double_quote_escape(&bin_dir.display().to_string())
    };
    format!("export PATH=\"{bin_dir_expr}:$PATH\"")
}

#[cfg(not(windows))]
fn unix_path_profile_update_candidates(home: &Path) -> Vec<PathBuf> {
    let shell_name = std::env::var("SHELL")
        .ok()
        .and_then(|shell| {
            Path::new(shell.trim())
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
        })
        .unwrap_or_default();

    let mut candidates = Vec::new();
    match shell_name.as_str() {
        "bash" => {
            if home.join(".bash_profile").exists() {
                push_profile_candidate(&mut candidates, home.join(".bash_profile"));
            } else if home.join(".bash_login").exists() {
                push_profile_candidate(&mut candidates, home.join(".bash_login"));
            } else {
                push_profile_candidate(&mut candidates, home.join(".profile"));
            }
        }
        "zsh" => {
            push_profile_candidate(&mut candidates, home.join(".zprofile"));
        }
        "sh" | "dash" => {
            push_profile_candidate(&mut candidates, home.join(".profile"));
        }
        _ => {
            for file_name in [
                ".profile",
                ".bash_profile",
                ".bash_login",
                ".zprofile",
                ".zshrc",
            ] {
                let profile_path = home.join(file_name);
                if profile_path.exists() {
                    push_profile_candidate(&mut candidates, profile_path);
                }
            }
            if candidates.is_empty() {
                push_profile_candidate(&mut candidates, home.join(".profile"));
            }
        }
    }

    candidates
}

#[cfg(not(windows))]
fn push_profile_candidate(candidates: &mut Vec<PathBuf>, profile_path: PathBuf) {
    if !candidates
        .iter()
        .any(|candidate| candidate == &profile_path)
    {
        candidates.push(profile_path);
    }
}

#[cfg(not(windows))]
fn unix_path_contains_dir(path_env: &str, expected: &Path) -> bool {
    path_env
        .split(':')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(PathBuf::from)
        .any(|entry| entry == expected)
}

#[cfg(not(windows))]
fn append_path_export_if_missing(
    profile_path: &Path,
    export_line: &str,
    marker_dir: &Path,
    home: &Path,
) -> Result<()> {
    let existing = match fs::read_to_string(profile_path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => String::new(),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to read profile file `{}`", profile_path.display())
            });
        }
    };

    if profile_configures_path_dir(existing.as_str(), marker_dir, home)
        || existing.contains(export_line)
    {
        return Ok(());
    }

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(profile_path)
        .with_context(|| {
            format!(
                "failed to open profile file `{}` for PATH update",
                profile_path.display()
            )
        })?;

    if !existing.is_empty() && !existing.ends_with('\n') {
        writeln!(file).with_context(|| {
            format!(
                "failed to write trailing newline into `{}` before PATH update",
                profile_path.display()
            )
        })?;
    }
    writeln!(file, "{export_line}").with_context(|| {
        format!(
            "failed to append PATH export into `{}`",
            profile_path.display()
        )
    })?;

    Ok(())
}

#[cfg(not(windows))]
fn profile_configures_path_dir(existing: &str, marker_dir: &Path, home: &Path) -> bool {
    let markers = unix_path_dir_markers(marker_dir, home);
    existing.lines().any(|line| {
        let trimmed = line.trim_start();
        !trimmed.starts_with('#')
            && line.contains("PATH")
            && markers.iter().any(|marker| line.contains(marker.as_str()))
    })
}

#[cfg(not(windows))]
fn unix_path_dir_markers(marker_dir: &Path, home: &Path) -> Vec<String> {
    let mut markers = vec![marker_dir.display().to_string()];

    if let Ok(relative) = marker_dir.strip_prefix(home) {
        let relative = relative.display().to_string();
        if !relative.is_empty() {
            markers.push(format!("$HOME/{relative}"));
            markers.push(format!("${{HOME}}/{relative}"));
            markers.push(format!("~/{relative}"));
        }
    }

    markers
}

#[cfg(not(windows))]
fn unix_shell_double_quote_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('$', "\\$")
}

fn managed_by_label(value: &InstallManagedBy) -> &'static str {
    match value {
        InstallManagedBy::Script => "script",
        InstallManagedBy::Desktop => "desktop",
        InstallManagedBy::Manual => "manual",
        InstallManagedBy::Unknown => "unknown",
    }
}

fn unix_timestamp_secs() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs())
}

#[cfg(test)]
mod tests {
    use super::{
        DownloadRetryPolicy, download_partial_path, download_release_asset_with_policy,
        ensure_unix_user_path_configured, expected_checksum_for_asset, force_path_update_warning,
        is_transient_download_error, parse_start_warnings,
    };
    use anyhow::Context as _;
    use reqwest::blocking::Client;
    use std::fs;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    #[cfg(not(windows))]
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    #[cfg(not(windows))]
    use std::sync::{Mutex, OnceLock};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn parses_prefixed_checksum_line() {
        let path = unique_temp_path("checksum-prefixed");
        fs::write(
            &path,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa pioneer-gateway-linux-x86_64.gz\n",
        )
        .expect("write checksums file");

        let parsed = expected_checksum_for_asset(&path, "pioneer-gateway-linux-x86_64.gz")
            .expect("parse checksum");

        assert_eq!(
            parsed,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn returns_error_when_asset_is_missing() {
        let path = unique_temp_path("checksum-missing");
        fs::write(
            &path,
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb pioneer-gateway-linux-aarch64.gz\n",
        )
        .expect("write checksums file");

        let error = expected_checksum_for_asset(&path, "pioneer-gateway-linux-x86_64.gz")
            .expect_err("missing asset should fail");

        assert!(format!("{error:#}").contains("checksum for"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn release_asset_name_matches_build_variant() {
        let asset_name = super::gateway_asset_file_name().expect("asset name");

        let expected_os = match std::env::consts::OS {
            "linux" => "linux",
            "macos" => "macos",
            "windows" => "windows",
            other => panic!("unexpected test OS: {other}"),
        };
        let expected_arch = match std::env::consts::ARCH {
            "x86_64" | "amd64" => "x86_64",
            "aarch64" | "arm64" => "aarch64",
            other => panic!("unexpected test arch: {other}"),
        };
        let expected_variant = if cfg!(feature = "computer-use") {
            "-computer-use"
        } else {
            ""
        };
        let expected_ext =
            if cfg!(windows) || (std::env::consts::OS == "macos" && expected_arch == "x86_64") {
                "zip"
            } else {
                "gz"
            };

        assert_eq!(
            asset_name,
            format!(
                "pioneer-gateway-{expected_os}-{expected_arch}{expected_variant}.{expected_ext}"
            )
        );
    }

    #[test]
    fn release_asset_download_retries_transient_status_and_finalizes_atomically() {
        let body = b"gateway-asset";
        let (url, server) = spawn_http_responses(vec![
            http_response(503, "Service Unavailable", b""),
            http_response(200, "OK", body),
        ]);
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let destination = temp_dir.path().join("gateway.gz");

        download_release_asset_with_policy(
            &test_http_client(),
            url.as_str(),
            destination.as_path(),
            immediate_retry_policy(2),
        )
        .expect("transient response should be retried");
        server.join().expect("HTTP server should finish");

        assert_eq!(fs::read(&destination).expect("downloaded asset"), body);
        assert!(!download_partial_path(&destination).exists());
    }

    #[test]
    fn release_asset_download_retries_request_timeout() {
        let body = b"gateway-asset-after-timeout";
        let (url, server) = spawn_timeout_then_success(body);
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let destination = temp_dir.path().join("gateway.gz");

        download_release_asset_with_policy(
            &test_http_client_with_timeout(Duration::from_millis(50)),
            url.as_str(),
            destination.as_path(),
            immediate_retry_policy(2),
        )
        .expect("timed out request should be retried");
        server.join().expect("HTTP server should finish");

        assert_eq!(fs::read(&destination).expect("downloaded asset"), body);
        assert!(!download_partial_path(&destination).exists());
    }

    #[test]
    fn exhausted_transient_download_remains_classified_through_context() {
        let (url, server) = spawn_http_responses(vec![
            http_response(503, "Service Unavailable", b""),
            http_response(503, "Service Unavailable", b""),
        ]);
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let destination = temp_dir.path().join("gateway.gz");

        let error = download_release_asset_with_policy(
            &test_http_client(),
            url.as_str(),
            destination.as_path(),
            immediate_retry_policy(2),
        )
        .context("failed to download gateway asset from release `v-test`")
        .expect_err("exhausted transient responses should fail");
        server.join().expect("HTTP server should finish");

        assert!(is_transient_download_error(&error));
        assert!(format!("{error:#}").contains("failed after 2 attempts"));
        assert!(!destination.exists());
        assert!(!download_partial_path(&destination).exists());
    }

    #[test]
    fn release_asset_download_does_not_retry_permanent_status() {
        let (url, server) = spawn_http_responses(vec![http_response(404, "Not Found", b"missing")]);
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let destination = temp_dir.path().join("gateway.gz");

        let error = download_release_asset_with_policy(
            &test_http_client(),
            url.as_str(),
            destination.as_path(),
            immediate_retry_policy(4),
        )
        .expect_err("permanent response should fail immediately");
        server.join().expect("HTTP server should finish");

        assert!(!is_transient_download_error(&error));
        assert!(format!("{error:#}").contains("404 Not Found"));
        assert!(!destination.exists());
    }

    #[test]
    fn parses_start_warnings_from_json() {
        let warnings = parse_start_warnings(
            br#"{
                "phase":"started",
                "warnings":[
                    {"code":"linux_linger_enable_failed","message":"run loginctl"}
                ]
            }"#,
        );

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "linux_linger_enable_failed");
        assert_eq!(warnings[0].message, "run loginctl");
    }

    #[test]
    fn ignores_missing_or_invalid_start_warnings() {
        assert!(parse_start_warnings(br#"{"phase":"started"}"#).is_empty());
        assert!(parse_start_warnings(b"not json").is_empty());
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_path_setup_reuses_existing_home_local_bin_profile_block() {
        let _guard = env_lock().lock().expect("env lock poisoned");

        let home = unique_temp_path("home-existing-profile-path");
        fs::create_dir_all(&home).expect("create temp home dir");
        let bin_dir = home.join(".local").join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir");

        let profile = home.join(".profile");
        let profile_content = r#"# test profile
if [ -d "$HOME/.local/bin" ] ; then
    PATH="$HOME/.local/bin:$PATH"
fi
"#;
        fs::write(&profile, profile_content).expect("write .profile");

        let old_home = std::env::var_os("HOME");
        let old_path = std::env::var_os("PATH");
        let old_shell = std::env::var_os("SHELL");
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("PATH", "/usr/bin:/bin");
            std::env::set_var("SHELL", "/bin/bash");
        }

        let result =
            ensure_unix_user_path_configured(&bin_dir).expect("path setup should not fail");

        restore_env_var("HOME", old_home);
        restore_env_var("PATH", old_path);
        restore_env_var("SHELL", old_shell);

        assert!(result.path_updated);
        assert!(
            result.warnings.is_empty(),
            "warnings: {:#?}",
            result.warnings
        );
        assert_eq!(
            fs::read_to_string(&profile).expect("read .profile"),
            profile_content
        );
        assert!(
            !home.join(".bash_profile").exists(),
            "installer should not create .bash_profile when .profile is the active bash startup file"
        );

        let _ = fs::remove_dir_all(&home);
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_path_setup_updates_profile_without_creating_bash_profile() {
        let _guard = env_lock().lock().expect("env lock poisoned");

        let home = unique_temp_path("home-profile-no-bash-profile");
        fs::create_dir_all(&home).expect("create temp home dir");
        let bin_dir = home.join(".local").join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        let profile = home.join(".profile");
        fs::write(&profile, "# test profile\n").expect("write .profile");

        let old_home = std::env::var_os("HOME");
        let old_path = std::env::var_os("PATH");
        let old_shell = std::env::var_os("SHELL");
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("PATH", "/usr/bin:/bin");
            std::env::set_var("SHELL", "/bin/bash");
        }

        let result =
            ensure_unix_user_path_configured(&bin_dir).expect("path setup should not fail");

        restore_env_var("HOME", old_home);
        restore_env_var("PATH", old_path);
        restore_env_var("SHELL", old_shell);

        let profile_content = fs::read_to_string(&profile).expect("read .profile");
        assert!(result.path_updated);
        assert!(
            result.warnings.is_empty(),
            "warnings: {:#?}",
            result.warnings
        );
        assert!(profile_content.contains("export PATH=\"$HOME/.local/bin:$PATH\""));
        assert!(
            !home.join(".bash_profile").exists(),
            "installer should not create .bash_profile and shadow .profile"
        );

        let _ = fs::remove_dir_all(&home);
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_path_setup_updates_existing_bash_profile_for_bash() {
        let _guard = env_lock().lock().expect("env lock poisoned");

        let home = unique_temp_path("home-existing-bash-profile");
        fs::create_dir_all(&home).expect("create temp home dir");
        let bin_dir = home.join(".local").join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        let profile = home.join(".profile");
        let bash_profile = home.join(".bash_profile");
        fs::write(&profile, "# profile\n").expect("write .profile");
        fs::write(&bash_profile, "# bash profile\n").expect("write .bash_profile");

        let old_home = std::env::var_os("HOME");
        let old_path = std::env::var_os("PATH");
        let old_shell = std::env::var_os("SHELL");
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("PATH", "/usr/bin:/bin");
            std::env::set_var("SHELL", "/bin/bash");
        }

        let result =
            ensure_unix_user_path_configured(&bin_dir).expect("path setup should not fail");

        restore_env_var("HOME", old_home);
        restore_env_var("PATH", old_path);
        restore_env_var("SHELL", old_shell);

        let bash_profile_content = fs::read_to_string(&bash_profile).expect("read .bash_profile");
        assert!(result.path_updated);
        assert!(
            result.warnings.is_empty(),
            "warnings: {:#?}",
            result.warnings
        );
        assert!(bash_profile_content.contains("export PATH=\"$HOME/.local/bin:$PATH\""));
        assert_eq!(
            fs::read_to_string(&profile).expect("read .profile"),
            "# profile\n"
        );

        let _ = fs::remove_dir_all(&home);
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_path_setup_updates_zprofile_for_zsh_without_creating_zshrc() {
        let _guard = env_lock().lock().expect("env lock poisoned");

        let home = unique_temp_path("home-zsh-zprofile");
        fs::create_dir_all(&home).expect("create temp home dir");
        let bin_dir = home.join(".local").join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir");

        let old_home = std::env::var_os("HOME");
        let old_path = std::env::var_os("PATH");
        let old_shell = std::env::var_os("SHELL");
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("PATH", "/usr/bin:/bin");
            std::env::set_var("SHELL", "/bin/zsh");
        }

        let result =
            ensure_unix_user_path_configured(&bin_dir).expect("path setup should not fail");

        restore_env_var("HOME", old_home);
        restore_env_var("PATH", old_path);
        restore_env_var("SHELL", old_shell);

        let zprofile = home.join(".zprofile");
        let zprofile_content = fs::read_to_string(&zprofile).expect("read .zprofile");
        assert!(result.path_updated);
        assert!(
            result.warnings.is_empty(),
            "warnings: {:#?}",
            result.warnings
        );
        assert!(zprofile_content.contains("export PATH=\"$HOME/.local/bin:$PATH\""));
        assert!(
            !home.join(".zshrc").exists(),
            "installer should not create .zshrc as a side effect of PATH setup"
        );

        let _ = fs::remove_dir_all(&home);
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_path_setup_warns_when_profile_is_unwritable() {
        let _guard = env_lock().lock().expect("env lock poisoned");

        let home = unique_temp_path("home-readonly-profile");
        fs::create_dir_all(&home).expect("create temp home dir");
        let bin_dir = home.join(".local").join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir");

        let bash_profile = home.join(".bash_profile");
        fs::write(&bash_profile, "# test profile\n").expect("write .bash_profile");
        let mut perms = fs::metadata(&bash_profile)
            .expect("stat .bash_profile")
            .permissions();
        perms.set_mode(0o444);
        fs::set_permissions(&bash_profile, perms).expect("set readonly permissions");

        let old_home = std::env::var_os("HOME");
        let old_path = std::env::var_os("PATH");
        let old_shell = std::env::var_os("SHELL");
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("PATH", "/usr/bin:/bin");
            std::env::set_var("SHELL", "/bin/bash");
        }

        let warnings = ensure_unix_user_path_configured(&bin_dir)
            .expect("path setup should not fail")
            .warnings;

        restore_env_var("HOME", old_home);
        restore_env_var("PATH", old_path);
        restore_env_var("SHELL", old_shell);

        let _ = fs::remove_dir_all(&home);

        assert!(
            warnings
                .iter()
                .any(|warning| warning.code == "path_update_skipped"),
            "expected path_update_skipped warning, got: {warnings:#?}"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn force_path_warning_env_toggle_is_respected() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let old_value = std::env::var_os("PIONEER_FORCE_PATH_WARNING");

        unsafe { std::env::set_var("PIONEER_FORCE_PATH_WARNING", "1") };
        assert!(force_path_update_warning());

        unsafe { std::env::set_var("PIONEER_FORCE_PATH_WARNING", "true") };
        assert!(force_path_update_warning());

        unsafe { std::env::set_var("PIONEER_FORCE_PATH_WARNING", "0") };
        assert!(!force_path_update_warning());

        unsafe { std::env::remove_var("PIONEER_FORCE_PATH_WARNING") };
        assert!(!force_path_update_warning());

        match old_value {
            Some(value) => unsafe { std::env::set_var("PIONEER_FORCE_PATH_WARNING", value) },
            None => unsafe { std::env::remove_var("PIONEER_FORCE_PATH_WARNING") },
        }
    }

    #[cfg(not(windows))]
    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[cfg(not(windows))]
    fn restore_env_var(name: &str, value: Option<std::ffi::OsString>) {
        match value {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
    }

    fn unique_temp_path(prefix: &str) -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{prefix}-{nanos}-{id}.txt"))
    }

    fn immediate_retry_policy(max_attempts: u32) -> DownloadRetryPolicy {
        DownloadRetryPolicy {
            max_attempts,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    fn test_http_client() -> Client {
        test_http_client_with_timeout(Duration::from_secs(2))
    }

    fn test_http_client_with_timeout(timeout: Duration) -> Client {
        Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(timeout)
            .build()
            .expect("test HTTP client")
    }

    fn spawn_timeout_then_success(body: &'static [u8]) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test HTTP server");
        let address = listener.local_addr().expect("test HTTP server address");
        let delayed_response = http_response(200, "OK", b"late");
        let success_response = http_response(200, "OK", body);
        let server = thread::spawn(move || {
            let (mut first_stream, _) = listener.accept().expect("accept first HTTP request");
            read_http_request(&mut first_stream);
            let delayed_writer = thread::spawn(move || {
                thread::sleep(Duration::from_millis(200));
                let _ = first_stream.write_all(&delayed_response);
            });

            let (mut second_stream, _) = listener.accept().expect("accept retry HTTP request");
            read_http_request(&mut second_stream);
            second_stream
                .write_all(&success_response)
                .expect("write successful retry response");
            delayed_writer.join().expect("delayed response writer");
        });
        (format!("http://{address}/asset"), server)
    }

    fn spawn_http_responses(responses: Vec<Vec<u8>>) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test HTTP server");
        let address = listener.local_addr().expect("test HTTP server address");
        let server = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept HTTP request");
                read_http_request(&mut stream);
                stream.write_all(&response).expect("write HTTP response");
            }
        });
        (format!("http://{address}/asset"), server)
    }

    fn read_http_request(stream: &mut std::net::TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set request read timeout");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).expect("read HTTP request");
        assert!(
            request[..read]
                .windows(4)
                .any(|window| window == b"\r\n\r\n"),
            "request headers should be complete"
        );
    }

    fn http_response(status: u16, reason: &str, body: &[u8]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }
}
