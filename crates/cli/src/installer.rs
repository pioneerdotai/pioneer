use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use pioneer_config::{AppConfig, InstallManagedBy, InstallState, save_install_state};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

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
        if let Err(error) = run_gateway_command(
            &target_binary,
            &["start"],
            &options.managed_by,
            false,
            "start",
        ) {
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

    if let Err(error) = persist_install_state(&options.managed_by, &target_binary, &install_root) {
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
        installed_version: env!("CARGO_PKG_VERSION").to_owned(),
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
    let mut response = client
        .get(url)
        .send()
        .with_context(|| format!("failed to download `{url}`"))?
        .error_for_status()
        .with_context(|| format!("release asset request failed for `{url}`"))?;

    let mut file = File::create(destination)
        .with_context(|| format!("failed to create `{}`", destination.display()))?;
    response.copy_to(&mut file).with_context(|| {
        format!(
            "failed to write downloaded payload into `{}`",
            destination.display()
        )
    })?;
    Ok(())
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

    let ext = if os == "windows" { "zip" } else { "gz" };
    Ok(format!("pioneer-gateway-{os}-{arch}.{ext}"))
}

fn persist_install_state(
    managed_by: &InstallManagedBy,
    target_binary: &Path,
    install_root: &Path,
) -> Result<()> {
    let config = AppConfig::load().context("failed to load app config for install-state update")?;
    let install_state_path = config
        .install_state_path()
        .context("failed to resolve install-state path")?;
    let state = InstallState {
        version: InstallState::CURRENT_VERSION,
        managed_by: managed_by.clone(),
        installed_version: env!("CARGO_PKG_VERSION").to_owned(),
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
    Ok(format!("{:x}", hasher.finalize()))
}

fn unpack_asset_to_binary(asset_path: &Path, target_binary: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        unpack_zip_asset(asset_path, target_binary)
    }

    #[cfg(not(windows))]
    {
        unpack_gzip_asset(asset_path, target_binary)
    }
}

#[cfg(not(windows))]
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

#[cfg(windows)]
fn unpack_zip_asset(asset_path: &Path, target_binary: &Path) -> Result<()> {
    let input = File::open(asset_path)
        .with_context(|| format!("failed to open asset `{}`", asset_path.display()))?;
    let mut archive = zip::ZipArchive::new(input)
        .with_context(|| format!("failed to parse zip archive `{}`", asset_path.display()))?;

    let mut found = false;
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
        if file_name != "pioneer.exe" {
            continue;
        }

        let mut output = File::create(target_binary).with_context(|| {
            format!(
                "failed to create staged binary `{}`",
                target_binary.display()
            )
        })?;
        std::io::copy(&mut entry, &mut output).with_context(|| {
            format!(
                "failed to unpack pioneer.exe to `{}`",
                target_binary.display()
            )
        })?;
        found = true;
        break;
    }

    if !found {
        bail!(
            "zip archive `{}` does not contain pioneer.exe",
            asset_path.display()
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
    let escaped_bin_dir = unix_shell_double_quote_escape(&bin_dir.display().to_string());
    let export_line = format!("export PATH=\"{escaped_bin_dir}:$PATH\"");
    let profile_files = [".profile", ".bash_profile", ".zprofile", ".zshrc"];
    let mut warnings = Vec::new();
    let mut any_profile_updated = false;

    for file_name in profile_files {
        let profile_path = home.join(file_name);
        match append_path_export_if_missing(profile_path.as_path(), export_line.as_str(), bin_dir) {
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
) -> Result<()> {
    let marker = marker_dir.display().to_string();
    let existing = match fs::read_to_string(profile_path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => String::new(),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to read profile file `{}`", profile_path.display())
            });
        }
    };

    if existing.contains(marker.as_str()) || existing.contains(export_line) {
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
        ensure_unix_user_path_configured, expected_checksum_for_asset, force_path_update_warning,
    };
    use std::fs;
    #[cfg(not(windows))]
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    #[cfg(not(windows))]
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

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
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("PATH", "/usr/bin:/bin");
        }

        let warnings = ensure_unix_user_path_configured(&bin_dir)
            .expect("path setup should not fail")
            .warnings;

        match old_home {
            Some(value) => unsafe { std::env::set_var("HOME", value) },
            None => unsafe { std::env::remove_var("HOME") },
        }
        match old_path {
            Some(value) => unsafe { std::env::set_var("PATH", value) },
            None => unsafe { std::env::remove_var("PATH") },
        }

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

    fn unique_temp_path(prefix: &str) -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{prefix}-{nanos}-{id}.txt"))
    }
}
