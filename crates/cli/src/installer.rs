use anyhow::{Context, Result, bail};
use pioneer_config::{AppConfig, InstallManagedBy, InstallState, save_install_state};
use reqwest::{StatusCode, blocking::Client, header::RETRY_AFTER};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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
    #[serde(default)]
    pub stage_timings: Vec<InstallStageTiming>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallStageTiming {
    pub stage: String,
    pub started_after_ms: u64,
    pub duration_ms: u64,
    pub outcome: String,
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
    #[serde(default)]
    stage_timings: Vec<InstallStageTiming>,
}

#[derive(Debug, Deserialize)]
struct VersionOutput {
    version: String,
    #[serde(default)]
    os: Option<String>,
    #[serde(default)]
    arch: Option<String>,
    #[serde(default)]
    computer_use: Option<bool>,
    #[serde(default)]
    compiled_database_migrations: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    prerelease: bool,
    #[serde(default)]
    assets: Vec<GithubReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct GithubReleaseAsset {
    name: String,
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
    let install_started = Instant::now();
    let mut stage_timings = Vec::new();
    let config = observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.config.load",
        || AppConfig::load().context("failed to load app config for install/update"),
    )?;

    let source = observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.source.resolve",
        || {
            resolve_install_source(&options)
                .context("failed to resolve installer source for install/update")
        },
    )?;

    let (install_root, bin_dir, target_binary, staged_binary, rollback_binary) =
        observe_install_stage(
            &mut stage_timings,
            install_started,
            "installer.paths.prepare",
            || {
                let install_root = install_root_path(&config)?;
                fs::create_dir_all(install_root.join("bin"))
                    .context("failed to create managed install directory")?;
                let install_root = canonical_install_path(&install_root)
                    .context("failed to resolve managed install root")?;
                let bin_dir = install_root.join("bin");
                let target_binary = installed_binary_path(&config, &install_root)?;
                let staged_binary = bin_dir.join(config.install_staged_binary_file_name()?);
                let rollback_binary = bin_dir.join(config.install_rollback_binary_file_name()?);
                fs::create_dir_all(&bin_dir).with_context(|| {
                    format!("failed to create install directory `{}`", bin_dir.display())
                })?;
                Ok((
                    install_root,
                    bin_dir,
                    target_binary,
                    staged_binary,
                    rollback_binary,
                ))
            },
        )?;

    observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.asset.verify",
        || {
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
            Ok(())
        },
    )?;

    observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.asset.unpack",
        || {
            if rollback_binary.exists() {
                bail!("binary rollback exists; explicit recovery is required before updating");
            }
            match fs::symlink_metadata(&staged_binary) {
                Ok(metadata) if metadata.is_file() => fs::remove_file(&staged_binary)?,
                Ok(_) => bail!("staged binary has unexpected file type"),
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error).context("staged binary metadata unavailable"),
            }
            if bin_dir.join("cua.rollback").exists() {
                bail!("CUA rollback payload exists; explicit recovery is required before updating");
            }
            unpack_asset_to_binary(&source.asset_path, &staged_binary)?;
            if !macos_cu_layout() {
                make_binary_executable(&staged_binary)?;
            }
            Ok(())
        },
    )?;

    // Keep the exact candidate that will be activated beside its own libraries
    // during schema admission; live libraries must remain untouched until then.
    let staged_binary = if macos_cu_layout() {
        bin_dir.join("cua.next").join(GATEWAY_APP_EXECUTABLE)
    } else {
        staged_binary
    };
    #[cfg(all(target_os = "macos", not(feature = "computer-use")))]
    let (_candidate_layout, staged_binary) = prepare_candidate_probe_layout(&staged_binary)?;

    let previous_binary = previous_installed_binary(&config, &install_root, &target_binary)?;
    let existing = previous_binary.is_some();
    let current_binary = previous_binary.as_deref().unwrap_or(&target_binary);
    #[cfg(windows)]
    let installer_owns_rollback_image = std::env::current_exe()
        .and_then(fs::canonicalize)
        .is_ok_and(|path| {
            fs::canonicalize(current_binary).is_ok_and(|installed| path == installed)
        });
    let runtime_home = observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.current.inspect",
        || {
            if !existing {
                return Ok(None);
            }
            Ok(query_gateway_status(current_binary)?.and_then(|snapshot| snapshot.runtime_home))
        },
    )?;
    observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.schema.preflight",
        || {
            // A failed status probe must not silently inspect another home.
            if existing && runtime_home.is_none() {
                bail!(
                    "schema compatibility unknown: installed runtime home unavailable; installation unchanged"
                );
            }
            let home = match runtime_home.clone() {
                Some(home) => home,
                None => config.runtime_home_dir()?,
            };
            let candidate = query_binary_version_output(&staged_binary)?;
            anyhow::ensure!(
                candidate.os.as_deref() == Some(std::env::consts::OS)
                    && candidate.arch.as_deref() == Some(std::env::consts::ARCH)
                    && candidate.computer_use == Some(cfg!(feature = "computer-use")),
                "candidate target or release variant mismatch; installation unchanged"
            );
            let config = config.clone();
            let applied = thread::spawn(move || {
                let runtime = pioneer_observability::runtime::ObservedRuntime::new(
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?,
                    pioneer_observability::runtime::RuntimeRole::CliHelper,
                );
                runtime.block_on(pioneer_gateway::inspect_applied_database_migrations(
                    &home, &config,
                ))
            })
            .join()
            .map_err(|_| anyhow::anyhow!("schema inspection worker failed"))??;
            let applied = match applied {
                Some(applied) => applied,
                None if !existing => Vec::new(),
                None => bail!(
                    "schema compatibility unknown: installed database missing; installation unchanged"
                ),
            };
            ensure_candidate_schema_compatible(
                &applied,
                candidate.compiled_database_migrations.as_deref(),
            )
        },
    )?;
    let config_backup_dir = observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.config.backup",
        || {
            let Some(home) = runtime_home.as_ref() else {
                return Ok(None);
            };
            let backup_dir = bin_dir.join(format!("config.rollback.{}", unix_timestamp_secs()?));
            backup_runtime_toml(home, &backup_dir)
        },
    )?;

    let install_state_path = config.install_state_path()?;
    let prior_install_state = pioneer_config::load_install_state(&install_state_path)?;
    let prior_command_link = CommandLinkBackup::capture(&config)?;
    let service_registration = crate::service::GatewayServiceRegistration::capture(&config)?;
    // Probe through this installer's adapter, not an older CLI whose native
    // activity/stop semantics may only describe the registration.
    let was_active = existing && service_registration.is_active()?;
    anyhow::ensure!(
        !was_active || service_registration.is_registered(),
        "running Gateway registration cannot be retained; installation unchanged"
    );
    let retired_binary = RetiredBinary::new(
        if existing && !is_bundle_executable(current_binary) {
            Some(current_binary.to_path_buf())
        } else if macos_cu_layout() {
            obsolete_macos_gateway_binary(&config, &bin_dir)?
        } else {
            None
        },
        rollback_binary.clone(),
    );
    let recover = |safety| {
        let mut outcome = rollback_installation(
            &target_binary,
            &rollback_binary,
            previous_binary.as_deref(),
            runtime_home.as_deref(),
            config_backup_dir.as_ref(),
            &options.managed_by,
            safety,
        );
        if safety == RollbackSafety::BeforeGatewayStart {
            // The old external macOS artifact can differ from the executable
            // selected for the running service. Restore what this attempt moved.
            if let Err(error) = retired_binary.restore_obsolete(current_binary) {
                outcome.binary = RestoreOutcome::Failed {
                    kind: restore_error_kind(&error),
                };
            }
            if outcome.performed() {
                let context_restore = restore_install_context(
                    &install_state_path,
                    prior_install_state.as_ref(),
                    &prior_command_link,
                    &target_binary,
                    || service_registration.restore(),
                );
                if let Err(error) = context_restore {
                    outcome.config = RestoreOutcome::Failed {
                        kind: restore_error_kind(&error),
                    };
                }
            }
            if was_active && outcome.performed() {
                let owner = prior_install_state
                    .as_ref()
                    .map(|state| &state.managed_by)
                    .unwrap_or(&options.managed_by);
                outcome.service_start = restore_outcome((|| -> Result<()> {
                    // A refused stop may have left the original process alive.
                    // Do not re-register/restart it, or treat unknown as stopped.
                    if !service_registration.is_active()? {
                        run_gateway_command(
                            current_binary,
                            &["start"],
                            owner,
                            false,
                            "rollback start",
                        )?;
                    }
                    Ok(())
                })());
                if outcome.service_start == RestoreOutcome::Restored {
                    // Prior releases may rewrite state or enable registration
                    // when starting. Reapply the captured passive identity and
                    // autostart policy without requesting another activation.
                    if let Err(error) = restore_install_context(
                        &install_state_path,
                        prior_install_state.as_ref(),
                        &prior_command_link,
                        &target_binary,
                        || service_registration.restore(),
                    ) {
                        outcome.config = RestoreOutcome::Failed {
                            kind: restore_error_kind(&error),
                        };
                    }
                    outcome.health =
                        wait_for_gateway_health(current_binary, Duration::from_secs(30)).err();
                }
            }
        }
        outcome
    };

    let should_start = !options.no_start && (options.force_start || !existing || was_active);
    let mut warnings: Vec<InstallWarning> = Vec::new();
    if existing {
        // Remove even an inactive registration, so an obsolete command cannot
        // be launched at the next login after a --no-start update.
        if let Err(error) =
            observe_install_stage(&mut stage_timings, install_started, "service.stop", || {
                service_registration.stop()
            })
        {
            return Err(InstallRecoveryFailure::error(
                error.context("failed to remove the prior service registration"),
                recover(RollbackSafety::BeforeGatewayStart),
            ));
        }
    }

    observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.binary.replace",
        || {
            let activation = (|| -> Result<()> {
                retired_binary.retain()?;
                activate_install_companions(&bin_dir)?;
                if macos_cu_layout() {
                    Ok(())
                } else {
                    fs::rename(&staged_binary, &target_binary).map_err(Into::into)
                }
            })();
            if let Err(error) = activation {
                let rollback = recover(RollbackSafety::BeforeGatewayStart);
                return Err(InstallRecoveryFailure::error(
                    error.context("failed to replace staged Gateway payload"),
                    rollback,
                ));
            }
            Ok(())
        },
    )?;

    let installed_version = match observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.binary.version_probe",
        || query_binary_version(&target_binary),
    ) {
        Ok(version) => version,
        Err(error) => {
            let rollback = recover(RollbackSafety::BeforeGatewayStart);
            return Err(InstallRecoveryFailure::error(
                error.context("installed binary failed version probe"),
                rollback,
            ));
        }
    };

    let link_result = match observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.command_link.ensure",
        || ensure_global_command_link(&config, &target_binary),
    ) {
        Ok(result) => result,
        Err(error) => {
            let rollback = recover(RollbackSafety::BeforeGatewayStart);
            return Err(InstallRecoveryFailure::error(
                error.context("failed to expose command globally"),
                rollback,
            ));
        }
    };
    let command_link_created = link_result.link_created;
    let path_updated = link_result.path_updated;
    warnings.extend(link_result.warnings);

    if let Err(error) = observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.state.persist",
        || {
            persist_install_state(
                &options.managed_by,
                &target_binary,
                &install_root,
                installed_version.as_str(),
            )
        },
    ) {
        return Err(InstallRecoveryFailure::error(
            error.context("failed to persist install-state"),
            recover(RollbackSafety::BeforeGatewayStart),
        ));
    }

    if should_start {
        let stage_started = Instant::now();
        match run_gateway_start_command(&target_binary, &options.managed_by) {
            Ok(start_output) => {
                let nested_offset = duration_millis(stage_started.duration_since(install_started));
                warnings.extend(start_output.warnings);
                stage_timings.push(successful_stage_timing(
                    "service.start",
                    install_started,
                    stage_started,
                ));
                stage_timings.extend(start_output.stage_timings.into_iter().map(|mut timing| {
                    timing.started_after_ms = nested_offset.saturating_add(timing.started_after_ms);
                    timing
                }));
            }
            Err(error) => {
                let rollback = recover(RollbackSafety::SchemaMayHaveAdvanced);
                return Err(InstallRecoveryFailure::error(
                    error.context("failed to start gateway after update"),
                    rollback,
                ));
            }
        };

        let stage_started = Instant::now();
        if let Err(health) = wait_for_gateway_health(&target_binary, Duration::from_secs(30)) {
            let stop = run_gateway_command(
                &target_binary,
                &["stop"],
                &options.managed_by,
                false,
                "stop during rollback",
            );
            let mut rollback = recover(RollbackSafety::SchemaMayHaveAdvanced);
            rollback.candidate_stop = Some(restore_outcome(stop));
            return Err(InstallRecoveryFailure::error(health.into(), rollback));
        }
        stage_timings.push(successful_stage_timing(
            "health.wait",
            install_started,
            stage_started,
        ));
    }

    observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.cleanup",
        || {
            #[cfg(windows)]
            remove_obsolete_windows_command(
                &legacy_windows_command_binary_path(&config)?,
                &target_binary,
            )?;
            if rollback_binary.exists() {
                if let Err(error) = fs::remove_file(&rollback_binary) {
                    #[cfg(windows)]
                    if installer_owns_rollback_image {
                        if let Err(cleanup) = defer_windows_rollback_cleanup(&rollback_binary) {
                            warnings.push(InstallWarning::new("rollback_cleanup_failed",
                                format!("installed Gateway is current; retain rollback for explicit cleanup: {cleanup:#}")));
                        }
                    } else {
                        warnings.push(InstallWarning::new(
                            "rollback_cleanup_failed",
                            error.to_string(),
                        ));
                    }
                    #[cfg(not(windows))]
                    warnings.push(InstallWarning::new(
                        "rollback_cleanup_failed",
                        error.to_string(),
                    ));
                }
            }
            let _ = fs::remove_dir_all(bin_dir.join("cua.rollback"));
            if macos_cu_layout() {
                cleanup_macos_cu_artifacts(&config, &bin_dir)?;
            }
            if let Some(backup_dir) = config_backup_dir.as_ref() {
                let _ = fs::remove_dir_all(&backup_dir.directory);
            }
            Ok(())
        },
    )?;

    let post_status = observe_install_stage(
        &mut stage_timings,
        install_started,
        "installer.post_status.probe",
        || query_gateway_status(&target_binary),
    )?
    .unwrap_or(ServiceSnapshot {
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
        stage_timings,
    })
}

fn observe_install_stage<T>(
    stage_timings: &mut Vec<InstallStageTiming>,
    install_started: Instant,
    stage: &'static str,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let stage_started = Instant::now();
    let result = operation();
    stage_timings.push(InstallStageTiming {
        stage: stage.to_owned(),
        started_after_ms: duration_millis(stage_started.duration_since(install_started)),
        duration_ms: duration_millis(stage_started.elapsed()),
        outcome: if result.is_ok() { "ok" } else { "error" }.to_owned(),
    });
    result
}

fn successful_stage_timing(
    stage: &'static str,
    install_started: Instant,
    stage_started: Instant,
) -> InstallStageTiming {
    InstallStageTiming {
        stage: stage.to_owned(),
        started_after_ms: duration_millis(stage_started.duration_since(install_started)),
        duration_ms: duration_millis(stage_started.elapsed()),
        outcome: "ok".to_owned(),
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
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

    let asset_name = gateway_asset_file_name()?;
    let tag = resolve_release_tag(&client, &api_base, channel, version, asset_name.as_str())?;
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
    asset_name: &str,
) -> Result<String> {
    if let Some(pinned_version) = version {
        let tag = normalize_version_tag(pinned_version);
        let url = format!("{}/tags/{tag}", api_base.trim_end_matches('/'));
        let release = client
            .get(url.as_str())
            .send()
            .with_context(|| format!("failed to fetch pinned release metadata from `{url}`"))?
            .error_for_status()
            .with_context(|| format!("pinned release `{tag}` does not exist"))?
            .json::<GithubRelease>()
            .context("failed to parse pinned release metadata")?;
        if !release_has_gateway_assets(&release, asset_name) {
            bail!(
                "pinned release `{tag}` is not ready: required assets `{asset_name}` and `SHA256SUMS` are not both published"
            );
        }
        return normalized_release_tag(&release);
    }

    let url = format!("{}?per_page=100", api_base.trim_end_matches('/'));
    let releases = client
        .get(url.as_str())
        .send()
        .with_context(|| format!("failed to fetch release list from `{url}`"))?
        .error_for_status()
        .with_context(|| format!("release API request failed for `{url}`"))?
        .json::<Vec<GithubRelease>>()
        .context("failed to parse release list response")?;
    select_ready_release_tag(channel, releases.iter(), asset_name)
}

fn normalize_version_tag(raw: &str) -> String {
    raw.trim().to_owned()
}

fn normalized_release_tag(release: &GithubRelease) -> Result<String> {
    let tag = release.tag_name.trim();
    if tag.is_empty() {
        bail!("release payload does not include `tag_name`");
    }
    Ok(tag.to_owned())
}

fn release_has_gateway_assets(release: &GithubRelease, asset_name: &str) -> bool {
    let has_gateway = release.assets.iter().any(|asset| asset.name == asset_name);
    let has_checksums = release
        .assets
        .iter()
        .any(|asset| asset.name == "SHA256SUMS");
    has_gateway && has_checksums
}

fn release_matches_channel(release: &GithubRelease, channel: ReleaseChannel) -> bool {
    if release.draft {
        return false;
    }
    match channel {
        ReleaseChannel::Stable => !release.prerelease,
        ReleaseChannel::Beta | ReleaseChannel::Canary => {
            let needle = format!("-{}", channel.as_str());
            release.tag_name.trim().contains(needle.as_str())
        }
    }
}

fn select_ready_release_tag<'a>(
    channel: ReleaseChannel,
    releases: impl IntoIterator<Item = &'a GithubRelease>,
    asset_name: &str,
) -> Result<String> {
    let mut matching_release_found = false;
    for release in releases {
        if !release_matches_channel(release, channel) {
            continue;
        }
        matching_release_found = true;
        if release_has_gateway_assets(release, asset_name) {
            return normalized_release_tag(release);
        }
    }

    if matching_release_found {
        bail!(
            "no fully published `{}` release contains required assets `{asset_name}` and `SHA256SUMS`",
            channel.as_str()
        );
    }
    bail!(
        "failed to find release tag for channel `{}` from release list",
        channel.as_str()
    )
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
    gateway_asset_file_name_for(
        std::env::consts::OS,
        std::env::consts::ARCH,
        cfg!(feature = "computer-use"),
    )
}

fn gateway_asset_file_name_for(os: &str, arch: &str, computer_use: bool) -> Result<String> {
    let arch = match arch {
        "x86_64" | "amd64" => "x86_64",
        "aarch64" | "arm64" => "aarch64",
        other => bail!("unsupported architecture `{other}` for release installer source"),
    };
    let os = match os {
        "linux" => "linux",
        "macos" => "macos",
        "windows" => "windows",
        other => bail!("unsupported OS `{other}` for release installer source"),
    };
    let suffix = if computer_use {
        "-computer-use-package"
    } else {
        ""
    };
    let extension = if computer_use || os == "windows" || (os == "macos" && arch == "x86_64") {
        "zip"
    } else {
        "gz"
    };
    Ok(format!("pioneer-gateway-{os}-{arch}{suffix}.{extension}"))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RollbackSafety {
    BeforeGatewayStart,
    SchemaMayHaveAdvanced,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RestoreOutcome {
    NotRequired,
    Restored,
    Failed { kind: &'static str },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GatewayHealthFailure {
    NotObserved,
    MissingBinary,
    StatusCommand,
    CommandRejected,
    InvalidStatus,
    ServiceInactive,
    GatewayUnreachable,
}
impl fmt::Display for GatewayHealthFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gateway health check failed ({self:?})")
    }
}
impl Error for GatewayHealthFailure {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct RollbackOutcome {
    pub blocked_compatibility_unknown: bool,
    pub binary: RestoreOutcome,
    pub config: RestoreOutcome,
    pub service_start: RestoreOutcome,
    pub health: Option<GatewayHealthFailure>,
    pub candidate_stop: Option<RestoreOutcome>,
}
impl RollbackOutcome {
    fn blocked() -> Self {
        Self {
            blocked_compatibility_unknown: true,
            binary: RestoreOutcome::NotRequired,
            config: RestoreOutcome::NotRequired,
            service_start: RestoreOutcome::NotRequired,
            health: None,
            candidate_stop: None,
        }
    }
    pub(crate) fn performed(&self) -> bool {
        !self.blocked_compatibility_unknown
            && self.binary == RestoreOutcome::Restored
            && !matches!(self.config, RestoreOutcome::Failed { .. })
    }
    pub(crate) fn healthy(&self) -> bool {
        self.performed() && self.service_start == RestoreOutcome::Restored && self.health.is_none()
    }
}
impl fmt::Display for RollbackOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rollback outcome: {self:?}")
    }
}
/// Carries original failure and actual recovery result to text/JSON reporting.
#[derive(Debug)]
pub(crate) struct InstallRecoveryFailure {
    pub rollback: RollbackOutcome,
    source: anyhow::Error,
}
impl InstallRecoveryFailure {
    fn error(source: anyhow::Error, rollback: RollbackOutcome) -> anyhow::Error {
        Self { source, rollback }.into()
    }
}
impl fmt::Display for InstallRecoveryFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}; {}", self.source, self.rollback)
    }
}
impl Error for InstallRecoveryFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

fn restore_error_kind(error: &anyhow::Error) -> &'static str {
    match error
        .downcast_ref::<std::io::Error>()
        .map(std::io::Error::kind)
    {
        Some(ErrorKind::NotFound) => "not_found",
        Some(ErrorKind::PermissionDenied) => "permission_denied",
        Some(ErrorKind::AlreadyExists) => "already_exists",
        Some(ErrorKind::StorageFull) => "storage_full",
        _ => "other",
    }
}
fn restore_outcome(result: Result<()>) -> RestoreOutcome {
    match result {
        Ok(()) => RestoreOutcome::Restored,
        Err(error) => RestoreOutcome::Failed {
            kind: restore_error_kind(&error),
        },
    }
}
fn rollback_installation(
    target_binary: &Path,
    rollback_binary: &Path,
    previous_binary: Option<&Path>,
    runtime_home: Option<&Path>,
    config_backup_dir: Option<&RuntimeConfigBackup>,
    managed_by: &InstallManagedBy,
    safety: RollbackSafety,
) -> RollbackOutcome {
    if safety == RollbackSafety::SchemaMayHaveAdvanced {
        return RollbackOutcome::blocked();
    }
    let directory = if is_bundle_executable(target_binary) {
        installed_payload_directory(target_binary)
    } else {
        target_binary
            .parent()
            .map(Path::to_path_buf)
            .context("installed binary has no parent")
    };
    let directory = match directory {
        Ok(directory) => directory,
        Err(error) => {
            return RollbackOutcome {
                blocked_compatibility_unknown: false,
                binary: RestoreOutcome::Failed {
                    kind: restore_error_kind(&error),
                },
                config: RestoreOutcome::NotRequired,
                service_start: RestoreOutcome::NotRequired,
                health: None,
                candidate_stop: None,
            };
        }
    };
    if previous_binary.is_none() {
        let binary = (|| -> Result<()> {
            restore_cua_payload(&directory)?;
            if !is_bundle_executable(target_binary) && target_binary.is_file() {
                fs::remove_file(target_binary)?;
            }
            Ok(())
        })();
        return RollbackOutcome {
            blocked_compatibility_unknown: false,
            binary: restore_outcome(binary),
            config: restore_config_backup(runtime_home, config_backup_dir),
            service_start: RestoreOutcome::NotRequired,
            health: None,
            candidate_stop: None,
        };
    }
    // A failed rename may leave the prior installation untouched after stop.
    if !rollback_binary.exists()
        && !directory.join("cua.rollback").exists()
        && previous_binary.is_some_and(Path::is_file)
    {
        return RollbackOutcome {
            blocked_compatibility_unknown: false,
            binary: RestoreOutcome::Restored,
            config: RestoreOutcome::NotRequired,
            service_start: RestoreOutcome::NotRequired,
            health: None,
            candidate_stop: None,
        };
    }
    let previous = previous_binary.unwrap_or(target_binary);
    if previous == target_binary {
        return rollback_and_restore(
            target_binary,
            rollback_binary,
            runtime_home,
            config_backup_dir,
            false,
            managed_by,
            safety,
        );
    }
    // macOS variant changes cross the app/standalone boundary. Restore the
    // matching package first, then publish/restart its own executable.
    let binary = (|| -> Result<()> {
        restore_cua_payload(&directory)?;
        if !is_bundle_executable(previous) {
            restore_binary_preserving_backup(previous, rollback_binary)?;
        } else if target_binary.is_file() {
            fs::remove_file(target_binary)?;
        }
        Ok(())
    })();
    let config = restore_config_backup(runtime_home, config_backup_dir);
    RollbackOutcome {
        blocked_compatibility_unknown: false,
        binary: restore_outcome(binary),
        config,
        service_start: RestoreOutcome::NotRequired,
        health: None,
        candidate_stop: None,
    }
}

fn restore_config_backup(
    runtime_home: Option<&Path>,
    config_backup_dir: Option<&RuntimeConfigBackup>,
) -> RestoreOutcome {
    match (runtime_home, config_backup_dir) {
        (Some(home), Some(backup)) => restore_outcome(restore_runtime_toml(home, backup)),
        (None, Some(_)) => RestoreOutcome::Failed {
            kind: "runtime_home_missing",
        },
        _ => RestoreOutcome::NotRequired,
    }
}

fn rollback_and_restore(
    target_binary: &Path,
    rollback_binary: &Path,
    runtime_home: Option<&Path>,
    config_backup_dir: Option<&RuntimeConfigBackup>,
    was_active: bool,
    managed_by: &InstallManagedBy,
    safety: RollbackSafety,
) -> RollbackOutcome {
    // No filesystem replacement before actual schema compatibility is known.
    if safety == RollbackSafety::SchemaMayHaveAdvanced {
        return RollbackOutcome::blocked();
    }
    let mut outcome = restore_binary_and_config(
        target_binary,
        rollback_binary,
        runtime_home,
        config_backup_dir,
    );
    if !is_bundle_executable(target_binary)
        && let Some(directory) = target_binary.parent()
    {
        if let Err(error) = restore_cua_payload(directory) {
            outcome.binary = RestoreOutcome::Failed {
                kind: restore_error_kind(&error),
            };
        }
    }
    // A failed binary/config restore must not start the candidate as if it were
    // the old installation. No backup is deleted on partial recovery.
    if was_active && outcome.performed() {
        outcome.service_start = restore_outcome(run_gateway_command(
            target_binary,
            &["start"],
            managed_by,
            false,
            "rollback start",
        ));
        if outcome.service_start == RestoreOutcome::Restored {
            outcome.health = wait_for_gateway_health(target_binary, Duration::from_secs(30)).err();
        }
    }
    outcome
}
fn restore_binary_and_config(
    target_binary: &Path,
    rollback_binary: &Path,
    runtime_home: Option<&Path>,
    config_backup_dir: Option<&RuntimeConfigBackup>,
) -> RollbackOutcome {
    // Copy through a same-directory temporary file and atomically replace the
    // target. Keep the sole rollback artifact for partial recovery and retry.
    let binary = if is_bundle_executable(target_binary) {
        restore_outcome(
            installed_payload_directory(target_binary)
                .and_then(|directory| restore_cua_payload(&directory)),
        )
    } else {
        restore_outcome(restore_binary_preserving_backup(
            target_binary,
            rollback_binary,
        ))
    };
    let config = restore_config_backup(runtime_home, config_backup_dir);
    RollbackOutcome {
        blocked_compatibility_unknown: false,
        binary,
        config,
        service_start: RestoreOutcome::NotRequired,
        health: None,
        candidate_stop: None,
    }
}
fn probe_gateway_status(
    binary: &Path,
) -> std::result::Result<ServiceSnapshot, GatewayHealthFailure> {
    let output = pioneer_observability::resources::captured_output(
        Command::new(binary).arg("status").arg("--json"),
        pioneer_observability::resources::ChildRole::PlatformHelper,
    )
    .map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            GatewayHealthFailure::MissingBinary
        } else {
            GatewayHealthFailure::StatusCommand
        }
    })?;
    if !output.status.success() {
        return Err(GatewayHealthFailure::CommandRejected);
    }
    let parsed: StatusOutput =
        serde_json::from_slice(&output.stdout).map_err(|_| GatewayHealthFailure::InvalidStatus)?;
    let runtime_home =
        (!parsed.runtime_home.trim().is_empty()).then(|| PathBuf::from(parsed.runtime_home.trim()));
    Ok(ServiceSnapshot {
        service_active: parsed.service_active,
        gateway_reachable: parsed.gateway_reachable,
        runtime_home,
    })
}
fn query_gateway_status(binary: &Path) -> Result<Option<ServiceSnapshot>> {
    match probe_gateway_status(binary) {
        Ok(status) => Ok(Some(status)),
        Err(GatewayHealthFailure::StatusCommand) => Err(GatewayHealthFailure::StatusCommand.into()),
        Err(_) => Ok(None), // Preserve existing initial-install inspection semantics.
    }
}

#[cfg(all(target_os = "macos", not(feature = "computer-use")))]
fn prepare_candidate_probe_layout(binary: &Path) -> Result<(tempfile::TempDir, PathBuf)> {
    let directory = binary.parent().context("candidate binary has no parent")?;
    let layout = tempfile::Builder::new()
        .prefix(".pioneer-candidate-")
        .tempdir_in(directory)
        .context("failed to create isolated candidate probe layout")?;
    restore_native_libraries(&directory.join("cua.next"), layout.path())?;
    let candidate = layout
        .path()
        .join(binary.file_name().context("candidate binary has no name")?);
    // Move rather than choose another packaged executable: probe and activation
    // use this same file, with its existing executable permissions preserved.
    fs::rename(binary, &candidate).context("failed to stage candidate for version probe")?;
    Ok((layout, candidate))
}

fn query_binary_version_output(binary: &Path) -> Result<VersionOutput> {
    let output = pioneer_observability::resources::captured_output(
        Command::new(binary).arg("version").arg("--json"),
        pioneer_observability::resources::ChildRole::PlatformHelper,
    )
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

    Ok(parsed)
}

fn query_binary_version(binary: &Path) -> Result<String> {
    Ok(query_binary_version_output(binary)?
        .version
        .trim()
        .to_owned())
}

fn ensure_candidate_schema_compatible(
    applied: &[String],
    compiled: Option<&[String]>,
) -> Result<()> {
    let compiled = compiled.context("schema compatibility unknown: candidate does not expose compiled migrations; installation unchanged")?;
    anyhow::ensure!(
        !compiled.is_empty() && compiled.len() <= 4096,
        "schema compatibility unknown: invalid candidate migration set; installation unchanged"
    );
    let names: std::collections::HashSet<_> = compiled.iter().collect();
    anyhow::ensure!(
        names.len() == compiled.len() && compiled.iter().all(|name| !name.trim().is_empty()),
        "schema compatibility unknown: invalid candidate migration names; installation unchanged"
    );
    anyhow::ensure!(
        applied.iter().all(|name| names.contains(name)),
        "candidate is incompatible with applied migration history; installation unchanged"
    );
    Ok(())
}

fn wait_for_gateway_health(
    binary: &Path,
    timeout: Duration,
) -> std::result::Result<(), GatewayHealthFailure> {
    let started = Instant::now();
    let mut last = GatewayHealthFailure::NotObserved;
    while started.elapsed() < timeout {
        last = match probe_gateway_status(binary) {
            Ok(status) if status.service_active && status.gateway_reachable => return Ok(()),
            Ok(status) if !status.service_active => GatewayHealthFailure::ServiceInactive,
            Ok(_) => GatewayHealthFailure::GatewayUnreachable,
            Err(reason) => reason,
        };
        thread::sleep(Duration::from_secs(1).min(timeout.saturating_sub(started.elapsed())));
    }
    Err(last)
}

fn run_gateway_start_command(binary: &Path, managed_by: &InstallManagedBy) -> Result<StartOutput> {
    let args = ["start", "--json"];
    let mut command = Command::new(binary);
    command.args(args.as_slice());
    command.env("PIONEER_MANAGED_BY", managed_by_label(managed_by));

    let output = pioneer_observability::resources::captured_output(
        &mut command,
        pioneer_observability::resources::ChildRole::PlatformHelper,
    )
    .with_context(|| {
        format!(
            "failed to run `{}`",
            render_binary_command(binary, args.as_slice())
        )
    })?;

    if output.status.success() {
        return Ok(parse_start_output(&output.stdout));
    }

    bail!(
        "command `{}` failed during start: {}",
        render_binary_command(binary, args.as_slice()),
        command_output_details(&output)
    )
}

fn parse_start_output(stdout: &[u8]) -> StartOutput {
    serde_json::from_slice::<StartOutput>(stdout).unwrap_or_else(|_| StartOutput {
        warnings: vec![InstallWarning::new(
            "gateway_start_output_parse_failed",
            "gateway start command succeeded but returned invalid JSON; detailed service stage timings are unavailable",
        )],
        stage_timings: Vec::new(),
    })
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

    let output = match pioneer_observability::resources::captured_output(
        &mut command,
        pioneer_observability::resources::ChildRole::PlatformHelper,
    ) {
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
    } else if !cfg!(feature = "computer-use")
        && !cfg!(windows)
        && !cfg!(all(target_os = "macos", target_arch = "x86_64"))
        && asset_path.extension().and_then(OsStr::to_str) == Some("gz")
    {
        let payload = target_binary
            .parent()
            .context("candidate has no parent")?
            .join("cua.next");
        if payload.exists() {
            fs::remove_dir_all(&payload)?;
        }
        let input = File::open(asset_path)?;
        let mut decoder = flate2::read::GzDecoder::new(input).take(512 * 1024 * 1024 + 1);
        let mut output = File::create(target_binary)?;
        if std::io::copy(&mut decoder, &mut output)? > 512 * 1024 * 1024 {
            bail!("Gateway archive exceeds binary budget");
        }
        Ok(())
    } else {
        bail!("Gateway archive format does not match this build variant and target")
    }
}

fn safe_archive_component(component: &str) -> bool {
    if component.is_empty() || component.ends_with(['.', ' ']) {
        return false;
    }
    let stem = component
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    !matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        && !(stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
}

fn copy_zip_member<R: Read>(
    entry: &mut zip::read::ZipFile<'_, R>,
    output: &mut File,
) -> Result<()> {
    let size = entry.size();
    let copied = std::io::copy(&mut entry.take(size + 1), output)?;
    anyhow::ensure!(copied == size, "Gateway archive member length mismatch");
    Ok(())
}

fn unpack_zip_asset(asset_path: &Path, target_binary: &Path) -> Result<()> {
    let input = File::open(asset_path)
        .with_context(|| format!("failed to open asset `{}`", asset_path.display()))?;
    let mut archive = zip::ZipArchive::new(input)
        .with_context(|| format!("failed to parse zip archive `{}`", asset_path.display()))?;

    let expected_binary_name = if macos_cu_layout() {
        "cua/Pioneer Gateway.app/Contents/MacOS/pioneer"
    } else if cfg!(windows) {
        "pioneer.exe"
    } else {
        "pioneer"
    };
    let target_dir = target_binary
        .parent()
        .context("staged binary path has no parent directory")?;
    let payload = target_dir.join("cua.next");
    if payload.exists() {
        fs::remove_dir_all(&payload).context("failed to clear staged CUA payload")?;
    }
    fs::create_dir(&payload).context("failed to create staged CUA payload")?;
    anyhow::ensure!(
        archive.len() <= 32768,
        "Gateway archive has too many members"
    );
    let mut total_bytes = 0u64;
    let mut seen = std::collections::BTreeSet::new();
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
        let name = entry.name();
        anyhow::ensure!(
            !name.contains('\\')
                && !name.contains(':')
                && !name.starts_with('/')
                && !name.chars().any(char::is_control)
                && !name
                    .trim_end_matches('/')
                    .split('/')
                    .any(|part| !safe_archive_component(part)),
            "invalid Gateway archive path"
        );
        let mode = entry.unix_mode().unwrap_or(0);
        let kind = mode & 0o170000;
        anyhow::ensure!(
            (kind == 0
                || if entry.is_dir() {
                    kind == 0o040000
                } else {
                    kind == 0o100000
                })
                && mode & 0o7000 == 0
                && (!cfg!(unix) || mode & 0o022 == 0),
            "Gateway archive links, special files and unsafe permissions are not permitted"
        );
        let relative = entry
            .enclosed_name()
            .context("invalid Gateway archive path")?;
        if !seen.insert(relative.to_string_lossy().to_lowercase()) {
            bail!("duplicate Gateway archive member");
        }
        if entry.is_dir() {
            anyhow::ensure!(entry.size() == 0, "invalid Gateway archive directory");
            continue;
        }
        if entry
            .unix_mode()
            .is_some_and(|mode| mode & 0o170000 == 0o120000)
        {
            bail!("Gateway archive symlinks are not permitted");
        }
        total_bytes = total_bytes.saturating_add(entry.size());
        if entry.size() > 512 * 1024 * 1024 || total_bytes > 2 * 1024 * 1024 * 1024 {
            bail!("Gateway archive exceeds payload budget");
        }
        if macos_cu_layout() && relative == Path::new(expected_binary_name) {
            found = true;
            #[cfg(unix)]
            anyhow::ensure!(
                entry.unix_mode().unwrap_or(0) & 0o111 != 0,
                "Gateway bundle executable is not executable"
            );
        }
        if let Ok(companion) = relative.strip_prefix("cua") {
            if !cfg!(feature = "computer-use") {
                bail!("server archive must not contain a CUA payload");
            }
            if companion.as_os_str().is_empty() {
                bail!("invalid CUA payload member");
            }
            let destination = payload.join(companion);
            fs::create_dir_all(destination.parent().context("CUA member has no parent")?)?;
            let mut output = File::create(&destination)?;
            copy_zip_member(&mut entry, &mut output)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = entry.unix_mode().unwrap_or(0o644) & 0o777;
                fs::set_permissions(&destination, fs::Permissions::from_mode(mode))?;
            }
            continue;
        }
        if relative.components().count() != 1 {
            bail!("unexpected Gateway archive member outside CUA payload");
        }
        let file_name = Path::new(entry.name())
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or_default()
            .to_ascii_lowercase();

        if macos_cu_layout() {
            bail!("macOS CU archive must contain only its CUA package and signed Gateway bundle");
        }
        if file_name == expected_binary_name {
            let mut output = File::create(target_binary).with_context(|| {
                format!(
                    "failed to create staged binary `{}`",
                    target_binary.display()
                )
            })?;
            copy_zip_member(&mut entry, &mut output).with_context(|| {
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
                let libraries = payload.join("native-libs");
                fs::create_dir_all(&libraries)?;
                let dylib_path = libraries.join(file_name.as_str());
                let mut output = File::create(&dylib_path).with_context(|| {
                    format!(
                        "failed to create companion dylib `{}`",
                        dylib_path.display()
                    )
                })?;
                copy_zip_member(&mut entry, &mut output).with_context(|| {
                    format!(
                        "failed to unpack companion dylib `{}`",
                        dylib_path.display()
                    )
                })?;
                companion_dylibs = companion_dylibs.saturating_add(1);
                continue;
            }
        }
        bail!("unexpected Gateway archive member");
    }

    if !found {
        bail!(
            "zip archive `{}` does not contain {expected_binary_name}",
            asset_path.display(),
        );
    }
    #[cfg(all(
        target_os = "macos",
        target_arch = "x86_64",
        not(feature = "computer-use")
    ))]
    if companion_dylibs == 0 {
        bail!(
            "zip archive `{}` does not contain libonnxruntime dylibs",
            asset_path.display(),
        );
    }

    if cfg!(feature = "computer-use") {
        validate_cua_payload(&payload)?;
    }
    Ok(())
}

// Server installs do not require a CUA package. A transition from CU still
// retires its managed payload in the same recovery unit as the executable.
fn activate_install_companions(directory: &Path) -> Result<()> {
    if cfg!(feature = "computer-use") {
        return activate_cua_payload(directory);
    }
    let current = directory.join("cua");
    let next = directory.join("cua.next");
    if !current.exists() && !next.join("native-libs").exists() {
        if next.exists() {
            fs::remove_dir_all(next)?;
        }
        return Ok(());
    }
    if !next.exists() {
        fs::create_dir(&next)?;
    }
    replace_install_companions(directory)
}

fn validate_cua_payload(payload: &Path) -> Result<()> {
    anyhow::ensure!(
        fs::metadata(payload.join("manifest.json"))?.len() <= 16 * 1024,
        "CUA manifest exceeds size budget"
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(payload.join("manifest.json"))?)?;
    let daemon = if cfg!(target_os = "macos") {
        "Pioneer Gateway.app/Contents/Helpers/cua-driver"
    } else if cfg!(windows) {
        "cua-driver.exe"
    } else {
        "cua-driver"
    };
    let os = match std::env::consts::OS {
        "macos" => "apple-darwin",
        "linux" => "unknown-linux-gnu",
        "windows" => "pc-windows-msvc",
        _ => bail!("unsupported Gateway CUA platform"),
    };
    let target = format!("{}-{os}", std::env::consts::ARCH);
    if manifest["schema_version"] != 1
        || manifest["repository"] != "https://github.com/trycua/cua"
        || manifest["target"].as_str() != Some(target.as_str())
        || manifest["daemon"] != daemon
        || manifest["permission_owner"] != "gateway"
        || manifest["sha256"].as_str() != Some(sha256_file(&payload.join(daemon))?.as_str())
        || !payload.join("LICENSE.cua-driver.md").is_file()
        || !payload.join("THIRD_PARTY_NOTICES.md").is_file()
    {
        bail!("Gateway CUA package identity or contents mismatch");
    }
    #[cfg(windows)]
    if !payload.join("cua-driver-uia.exe").is_file() {
        bail!("Gateway CUA Windows UIA companion is missing");
    }
    #[cfg(target_os = "macos")]
    if !payload
        .join("Pioneer Gateway.app/Contents/MacOS/pioneer")
        .is_file()
        || !payload
            .join("Pioneer Gateway.app/Contents/Info.plist")
            .is_file()
    {
        bail!("Gateway responsible application is missing");
    }
    Ok(())
}

fn activate_cua_payload(directory: &Path) -> Result<()> {
    let next = directory.join("cua.next");
    validate_cua_payload(&next)?;
    replace_install_companions(directory)
}

fn replace_install_companions(directory: &Path) -> Result<()> {
    let current = directory.join("cua");
    let backup = directory.join("cua.rollback");
    let next = directory.join("cua.next");
    if backup.exists() {
        bail!("CUA rollback payload exists; retain it for explicit recovery before updating");
    }
    if current.exists() {
        fs::rename(&current, &backup)?;
    } else {
        // An empty backup represents an installation that previously had no CUA.
        fs::create_dir(&backup)?;
    }
    #[cfg(all(target_os = "macos", not(feature = "computer-use")))]
    {
        // Root native libraries belong to the same recovery unit. Do not
        // overwrite a running installation while merely unpacking an archive.
        let libraries = backup.join("native-libs");
        fs::create_dir_all(&libraries)?;
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("libonnxruntime") && name.ends_with(".dylib") {
                anyhow::ensure!(
                    entry.file_type()?.is_file(),
                    "invalid installed native library"
                );
                fs::rename(entry.path(), libraries.join(name.as_ref()))?;
            }
        }
    }
    fs::rename(next, &current).context("failed to activate staged CUA payload")?;
    #[cfg(all(target_os = "macos", not(feature = "computer-use")))]
    restore_native_libraries(&current, directory)?;
    Ok(())
}

fn restore_cua_payload(directory: &Path) -> Result<()> {
    let backup = directory.join("cua.rollback");
    if backup.exists() {
        let current = directory.join("cua");
        if current.exists() {
            #[cfg(all(target_os = "macos", not(feature = "computer-use")))]
            if current.join("native-libs").is_dir() {
                for entry in fs::read_dir(current.join("native-libs"))? {
                    let path = directory.join(entry?.file_name());
                    if path.is_file() {
                        fs::remove_file(path)?;
                    }
                }
            }
            fs::remove_dir_all(&current)?;
        }
        // Preserve the sole known-good payload if a later config/service restore
        // fails, just as restore_binary_preserving_backup retains the executable.
        copy_cua_payload(&backup, &current).context("failed to restore CUA rollback payload")?;
        #[cfg(all(target_os = "macos", not(feature = "computer-use")))]
        restore_native_libraries(&backup, directory)?;
    }
    Ok(())
}

#[cfg(all(target_os = "macos", not(feature = "computer-use")))]
fn restore_native_libraries(payload: &Path, directory: &Path) -> Result<()> {
    let libraries = payload.join("native-libs");
    if libraries.is_dir() {
        for entry in fs::read_dir(libraries)? {
            let entry = entry?;
            fs::copy(entry.path(), directory.join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn copy_cua_payload(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_cua_payload(&entry.path(), &target)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), target)?;
        } else {
            bail!("invalid CUA rollback payload member");
        }
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

#[derive(Debug)]
struct RuntimeConfigBackup {
    directory: PathBuf,
    // The actual files selected by the existing TOML backup owner. Keep their
    // content digests so missing/truncated/replaced backups cannot report success.
    files: Vec<(OsString, String)>,
}

fn restore_binary_preserving_backup(target: &Path, backup: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(backup).context("binary backup metadata unavailable")?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() > 0,
        "invalid binary backup"
    );
    let parent = target.parent().context("binary target has no parent")?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    fs::copy(backup, temporary.path()).context("failed to copy binary backup")?;
    temporary
        .as_file()
        .sync_all()
        .context("failed to sync restored binary")?;
    temporary
        .persist(target)
        .map_err(|error| error.error)
        .context("failed to replace binary from retained backup")?;
    Ok(())
}

fn backup_runtime_toml(
    runtime_home: &Path,
    backup_dir: &Path,
) -> Result<Option<RuntimeConfigBackup>> {
    match fs::symlink_metadata(runtime_home) {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("runtime config metadata unavailable"),
        Ok(metadata) => anyhow::ensure!(metadata.is_dir(), "runtime home is not a directory"),
    }
    let mut selected = Vec::new();
    for entry in fs::read_dir(runtime_home).context("failed to list runtime config")? {
        let entry = entry?;
        let path = entry.path();
        if !path
            .extension()
            .and_then(OsStr::to_str)
            .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
        {
            continue;
        }
        let metadata = fs::symlink_metadata(&path).context("config entry metadata unavailable")?;
        anyhow::ensure!(metadata.is_file(), "config entry is not a regular file");
        selected.push(entry.file_name());
    }
    if selected.is_empty() {
        // Proven absence, unlike an unexpectedly empty expected backup.
        return Ok(None);
    }
    fs::create_dir(backup_dir).context("failed to create unique config backup directory")?;
    let mut backup = RuntimeConfigBackup {
        directory: backup_dir.to_owned(),
        files: Vec::new(),
    };
    for name in selected {
        let src = runtime_home.join(&name);
        let expected_digest = sha256_file(&src)?;
        let dst = backup_dir.join(&name);
        fs::copy(&src, &dst).context("failed to backup config")?;
        anyhow::ensure!(
            sha256_file(&dst)? == expected_digest,
            "config changed during backup"
        );
        backup.files.push((name, expected_digest));
    }
    Ok(Some(backup))
}

fn restore_runtime_toml(runtime_home: &Path, backup: &RuntimeConfigBackup) -> Result<()> {
    let metadata =
        fs::symlink_metadata(&backup.directory).context("configuration backup unavailable")?;
    anyhow::ensure!(metadata.is_dir(), "configuration backup is not a directory");
    anyhow::ensure!(
        !backup.files.is_empty(),
        "expected configuration backup is empty"
    );
    // Validate the complete expected set before the first config mutation.
    // Metadata errors are propagated; no is_file() branch silently skips them.
    for (name, digest) in &backup.files {
        let src = backup.directory.join(name);
        let metadata =
            fs::symlink_metadata(&src).context("config backup entry metadata unavailable")?;
        anyhow::ensure!(
            metadata.is_file(),
            "config backup entry is not a regular file"
        );
        anyhow::ensure!(
            sha256_file(&src)? == *digest,
            "config backup content mismatch"
        );
    }
    fs::create_dir_all(runtime_home).context("failed to create runtime home for config restore")?;
    for (name, digest) in &backup.files {
        // Atomic per-file restore keeps retries safe after any partial copy.
        let temporary = tempfile::NamedTempFile::new_in(runtime_home)?;
        fs::copy(backup.directory.join(name), temporary.path())
            .context("failed to restore config")?;
        anyhow::ensure!(
            sha256_file(temporary.path())? == *digest,
            "config backup changed during restore"
        );
        temporary.as_file().sync_all()?;
        temporary
            .persist(runtime_home.join(name))
            .map_err(|error| error.error)
            .context("failed to replace restored config")?;
    }
    Ok(())
}

const GATEWAY_APP_EXECUTABLE: &str = "Pioneer Gateway.app/Contents/MacOS/pioneer";

fn macos_cu_layout() -> bool {
    cfg!(all(target_os = "macos", feature = "computer-use"))
}

pub(crate) fn canonical_install_path(path: &Path) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};
        let mut components = canonical.components();
        if let Some(Component::Prefix(prefix)) = components.next() {
            // Windows canonicalize adds a verbatim prefix. Normal user paths
            // must remain usable by PATH lookup, PowerShell and Task Scheduler.
            let mut normal = match prefix.kind() {
                Prefix::VerbatimDisk(drive) => PathBuf::from(format!("{}:\\", drive as char)),
                Prefix::VerbatimUNC(server, share) => {
                    let mut unc = OsString::from(r"\\");
                    unc.push(server);
                    unc.push(r"\");
                    unc.push(share);
                    PathBuf::from(unc)
                }
                _ => return Ok(canonical),
            };
            for component in components {
                if let Component::Normal(part) = component {
                    if part.to_str().is_none_or(|name| name.ends_with(['.', ' '])) {
                        return Ok(canonical);
                    }
                    normal.push(part);
                }
            }
            return Ok(normal);
        }
    }
    Ok(canonical)
}

pub(crate) fn installed_binary_path(config: &AppConfig, install_root: &Path) -> Result<PathBuf> {
    let bin = install_root.join("bin");
    Ok(if macos_cu_layout() {
        bin.join("cua").join(GATEWAY_APP_EXECUTABLE)
    } else {
        bin.join(config.install_binary_file_name()?)
    })
}

fn is_installed_layout_path(config: &AppConfig, root: &Path, binary: &Path) -> Result<bool> {
    let bin = root.join("bin");
    Ok(binary == bin.join(config.install_binary_file_name()?)
        || (cfg!(target_os = "macos") && binary == bin.join("cua").join(GATEWAY_APP_EXECUTABLE)))
}

fn previous_installed_binary(
    config: &AppConfig,
    root: &Path,
    selected: &Path,
) -> Result<Option<PathBuf>> {
    let previous = pioneer_config::load_install_state(&config.install_state_path()?)?;
    // Install-state chooses a prior variant explicitly; never search for another
    // executable. The selected runtime remains authoritative when it exists.
    let path = match fs::symlink_metadata(selected) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_file(),
                "installed binary has unexpected file type"
            );
            return Ok(Some(selected.to_path_buf()));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => match previous {
            Some(state)
                if state.install_root.as_deref() == Some(root)
                    && is_installed_layout_path(config, root, &state.binary_path)? =>
            {
                state.binary_path
            }
            _ => return Ok(None),
        },
        Err(error) => return Err(error).context("installed binary metadata unavailable"),
    };
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            anyhow::ensure!(metadata.is_file(), "prior binary has unexpected file type");
            Ok(Some(path))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("prior binary metadata unavailable"),
    }
}

fn is_bundle_executable(binary: &Path) -> bool {
    cfg!(target_os = "macos") && binary.ends_with(Path::new(GATEWAY_APP_EXECUTABLE))
}

fn installed_payload_directory(binary: &Path) -> Result<PathBuf> {
    // bin/cua/Pioneer Gateway.app/Contents/MacOS/pioneer -> bin
    binary
        .ancestors()
        .nth(5)
        .map(Path::to_path_buf)
        .context("invalid installed Gateway bundle path")
}

fn obsolete_macos_gateway_binary(config: &AppConfig, directory: &Path) -> Result<Option<PathBuf>> {
    let external = directory.join(config.install_binary_file_name()?);
    // A configured CLI link may legitimately occupy this exact path. It is
    // owned by CommandLinkBackup, never by regular-executable recovery.
    match fs::symlink_metadata(&external) {
        Ok(metadata) if metadata.is_file() => Ok(Some(external)),
        Ok(_) => Ok(None),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("obsolete Gateway metadata unavailable"),
    }
}

fn cleanup_macos_cu_artifacts(config: &AppConfig, directory: &Path) -> Result<()> {
    if let Some(external) = obsolete_macos_gateway_binary(config, directory)? {
        fs::remove_file(external)?;
    }
    remove_root_native_libraries(directory)
}

#[cfg(target_os = "macos")]
fn remove_root_native_libraries(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("libonnxruntime")
            && name.ends_with(".dylib")
            && entry.file_type()?.is_file()
        {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn remove_root_native_libraries(_directory: &Path) -> Result<()> {
    Ok(())
}

pub(crate) fn install_root_path(config: &AppConfig) -> Result<PathBuf> {
    if let Some(value) = std::env::var_os("PIONEER_INSTALL_ROOT") {
        let root = PathBuf::from(value);
        return if root.is_absolute() {
            Ok(root)
        } else {
            Ok(std::env::current_dir()?.join(root))
        };
    }

    if let Some(state) = pioneer_config::load_install_state(&config.install_state_path()?)?
        && let Some(root) = state.install_root
        && root.is_absolute()
        && is_installed_layout_path(config, &root, &state.binary_path)?
    {
        return Ok(root);
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

struct RetiredBinary {
    original: Option<PathBuf>,
    rollback: PathBuf,
    moved: std::cell::Cell<bool>,
}

impl RetiredBinary {
    fn new(original: Option<PathBuf>, rollback: PathBuf) -> Self {
        Self {
            original,
            rollback,
            moved: std::cell::Cell::new(false),
        }
    }
    fn retain(&self) -> Result<()> {
        if let Some(original) = &self.original {
            fs::rename(original, &self.rollback)
                .context("failed to retain previous Gateway executable")?;
            self.moved.set(true);
        }
        Ok(())
    }
    fn restore_obsolete(&self, service_binary: &Path) -> Result<()> {
        if self.moved.get()
            && let Some(original) = &self.original
            && original != service_binary
        {
            restore_binary_preserving_backup(original, &self.rollback)?;
        }
        Ok(())
    }
}

struct CommandLinkBackup {
    #[cfg(not(windows))]
    path: PathBuf,
    #[cfg(not(windows))]
    target: Option<PathBuf>,
}

impl CommandLinkBackup {
    fn capture(config: &AppConfig) -> Result<Self> {
        #[cfg(not(windows))]
        {
            Self::capture_path(unix_command_link_path(config)?)
        }
        #[cfg(windows)]
        {
            let _ = config;
            Ok(Self {})
        }
    }

    #[cfg(not(windows))]
    fn capture_path(path: PathBuf) -> Result<Self> {
        let target = match fs::read_link(&path) {
            Ok(target) => Some(target),
            Err(error) if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::InvalidInput) => {
                None
            }
            Err(error) => return Err(error).context("failed to retain CLI symlink identity"),
        };
        Ok(Self { path, target })
    }

    fn restore(&self, candidate: &Path) -> Result<()> {
        #[cfg(not(windows))]
        {
            if let Some(target) = &self.target {
                publish_unix_command_link(&self.path, target)?;
            } else if fs::read_link(&self.path).is_ok_and(|path| path == candidate) {
                fs::remove_file(&self.path)?;
            }
        }
        #[cfg(windows)]
        let _ = candidate;
        Ok(())
    }
}

fn restore_install_state_snapshot(
    state_path: &Path,
    prior_state: Option<&InstallState>,
) -> Result<()> {
    if let Some(state) = prior_state {
        save_install_state(state_path, state)
    } else {
        match fs::remove_file(state_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn restore_install_context(
    state_path: &Path,
    prior_state: Option<&InstallState>,
    command_link: &CommandLinkBackup,
    candidate: &Path,
    restore_registration: impl FnOnce() -> Result<()>,
) -> Result<()> {
    // Attempt every passive restore; any failure prevents restarting the service
    // and prevents reporting a complete rollback.
    let state = restore_install_state_snapshot(state_path, prior_state);
    let command = command_link.restore(candidate);
    let registration = restore_registration();
    state.context("failed to restore install-state")?;
    command.context("failed to restore CLI identity")?;
    registration.context("failed to restore Gateway service registration")
}

#[cfg(not(windows))]
fn unix_command_link_path(config: &AppConfig) -> Result<PathBuf> {
    match std::env::var_os("PIONEER_LINK_PATH") {
        Some(path) => Ok(PathBuf::from(path)),
        None => Ok(dirs::home_dir()
            .context("home unavailable for command link")?
            .join(".local/bin")
            .join(config.install_command_file_name()?)),
    }
}

#[cfg(not(windows))]
fn publish_unix_command_link(link: &Path, target: &Path) -> Result<()> {
    use std::os::unix::fs::symlink;
    let parent = link.parent().context("command link has no parent")?;
    fs::create_dir_all(parent)?;
    match fs::symlink_metadata(link) {
        Ok(metadata) => anyhow::ensure!(
            metadata.file_type().is_symlink(),
            "cannot replace an unmanaged file at command link path `{}`",
            link.display()
        ),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("command link metadata unavailable"),
    }
    // TempPath cleans the staged symlink on failure. Rename publishes it on the
    // same filesystem without first removing the existing working command.
    let temporary = tempfile::Builder::new()
        .prefix(".pioneer-link-")
        .tempfile_in(parent)?
        .into_temp_path();
    fs::remove_file(&temporary)?;
    symlink(target, &temporary)?;
    fs::rename(&temporary, link).context("failed to atomically publish command symlink")?;
    Ok(())
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
    let link_path_overridden = std::env::var_os("PIONEER_LINK_PATH").is_some();
    let link_path = unix_command_link_path(config)?;
    let mut warnings = Vec::new();
    let mut path_updated = true;

    publish_unix_command_link(&link_path, target_binary)?;

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
    publish_windows_command_path(
        target_binary,
        &legacy_windows_command_binary_path(config)?,
        force_path_update_warning(),
    )
}

#[cfg(windows)]
fn publish_windows_command_path(
    target_binary: &Path,
    legacy_command: &Path,
    skip_path_update: bool,
) -> Result<CommandLinkResult> {
    let mut warnings = Vec::new();
    let bin_dir = target_binary
        .parent()
        .context("installed binary has no parent directory")?;
    // Retain the old executable until commit: rollback may restore a Task/state
    // that still names it. Publishing PATH must not retire that identity early.
    if skip_path_update {
        return Ok(CommandLinkResult {
            link_created: true,
            path_updated: false,
            warnings: vec![InstallWarning::new(
                "path_update_skipped",
                "automatic PATH update skipped because PIONEER_FORCE_PATH_WARNING is set",
            )],
        });
    }

    let bin_dir_escaped = powershell_single_quote(&bin_dir.display().to_string());

    let legacy_dir_escaped = powershell_single_quote(
        &legacy_command
            .parent()
            .context("legacy command has no parent")?
            .display()
            .to_string(),
    );
    let script = format!(
        r#"$bin = '{bin_dir_escaped}'
        $legacy = '{legacy_dir_escaped}'
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        if ($null -eq $userPath) {{ $userPath = '' }}
        $parts = $userPath -split ';' | ForEach-Object {{ $_.Trim() }} | Where-Object {{ $_ -ne '' }}
        $parts = @($parts | Where-Object {{ $_ -ne $bin -and $_ -ne $legacy }})
        $newPath = (@($bin) + $parts) -join ';'
        [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')"#
    );

    let output = pioneer_observability::resources::captured_output(
        Command::new("powershell").args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &script,
        ]),
        pioneer_observability::resources::ChildRole::PlatformHelper,
    )
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
fn remove_obsolete_windows_command(legacy: &Path, target: &Path) -> Result<()> {
    match fs::symlink_metadata(legacy) {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("obsolete command metadata unavailable"),
        Ok(metadata) => anyhow::ensure!(metadata.is_file(), "invalid obsolete command artifact"),
    }
    // A custom root can resolve through the legacy directory's junction. Never
    // delete the installed executable merely because the spellings differ.
    let target =
        canonical_install_path(target).context("installed command identity unavailable")?;
    let legacy_identity = canonical_install_path(legacy)
        .context("obsolete command identity unavailable; artifact retained")?;
    if legacy_identity != target {
        fs::remove_file(legacy).context("failed to remove obsolete managed PATH executable")?;
    }
    Ok(())
}

#[cfg(windows)]
fn defer_windows_rollback_cleanup(rollback: &Path) -> Result<()> {
    // A self-updating Windows image can be renamed while mapped, but deletion
    // may be denied until this process exits. No helper binary is installed.
    // Spawn only after the installation committed, and bind cleanup to the
    // exact retained bytes so a later recovery artifact cannot be removed.
    let path = powershell_single_quote(&rollback.display().to_string());
    let digest = sha256_file(rollback)?;
    let pid = std::process::id();
    let script = format!(
        r#"$ErrorActionPreference = 'Stop'
        Wait-Process -Id {pid} -ErrorAction SilentlyContinue
        $path = '{path}'
        $item = Get-Item -LiteralPath $path -ErrorAction SilentlyContinue
        if ($null -ne $item -and -not $item.PSIsContainer -and
            -not ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -and
            (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash -eq '{digest}') {{
            Remove-Item -LiteralPath $path
        }}"#
    );
    use std::os::windows::process::CommandExt;
    use std::process::Stdio;
    Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &script,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(0x08000000) // CREATE_NO_WINDOW; keep cleanup out of the UI.
        .spawn()
        .context("failed to schedule cleanup of the running rollback image")?;
    Ok(())
}

#[cfg(windows)]
fn powershell_single_quote(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(windows)]
fn legacy_windows_command_binary_path(config: &AppConfig) -> Result<PathBuf> {
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
    #[cfg(not(windows))]
    use super::ensure_unix_user_path_configured;
    use super::{
        DownloadRetryPolicy, GithubRelease, GithubReleaseAsset, ReleaseChannel,
        download_partial_path, download_release_asset_with_policy, expected_checksum_for_asset,
        force_path_update_warning, is_transient_download_error, normalize_version_tag,
        parse_start_output, select_ready_release_tag,
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
    #[cfg(windows)]
    fn windows_alias_cleanup_retains_the_installed_executable() {
        let temporary = tempfile::tempdir().unwrap();
        let actual = temporary.path().join("runtime");
        let alias = temporary.path().join("Pioneer");
        fs::create_dir_all(actual.join("bin")).unwrap();
        let target = actual.join("bin/pioneer.exe");
        fs::write(&target, b"only installed Gateway").unwrap();
        let status = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&alias)
            .arg(&actual)
            .status()
            .unwrap();
        assert!(status.success(), "fixture junction creation failed");
        let legacy = alias.join("bin/pioneer.exe");
        let target = super::canonical_install_path(&target).unwrap();
        super::remove_obsolete_windows_command(&legacy, &target).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"only installed Gateway");
        assert_eq!(fs::read(&legacy).unwrap(), b"only installed Gateway");
        // Unknown target identity must retain the artifact, never guess.
        assert!(
            super::remove_obsolete_windows_command(&legacy, &actual.join("missing.exe")).is_err()
        );
        assert_eq!(fs::read(&legacy).unwrap(), b"only installed Gateway");
        fs::remove_dir(&alias).unwrap();
    }

    #[test]
    #[cfg(windows)]
    fn windows_path_copy_survives_pre_start_failure_and_is_retired_only_on_commit() {
        for commit in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let root = temporary.path();
            let binary = root.join("pioneer.exe");
            let rollback = root.join("pioneer.rollback.exe");
            let legacy = root.join("old-path/pioneer.exe");
            let state_path = root.join("install-state.toml");
            fs::create_dir(legacy.parent().unwrap()).unwrap();
            fs::write(&binary, b"old managed Gateway").unwrap();
            fs::write(&legacy, b"old PATH Gateway").unwrap();
            let prior = pioneer_config::InstallState {
                version: pioneer_config::InstallState::CURRENT_VERSION,
                managed_by: pioneer_config::InstallManagedBy::Script,
                installed_version: "prior".to_owned(),
                install_root: Some(root.to_path_buf()),
                binary_path: legacy.clone(),
                updated_at_unix: 42,
            };
            pioneer_config::save_install_state(&state_path, &prior).unwrap();
            fs::rename(&binary, &rollback).unwrap();
            fs::write(&binary, b"candidate Gateway").unwrap();
            // Exercise publication's supported warning path without touching
            // the host's user PATH or invoking PowerShell.
            let published = super::publish_windows_command_path(&binary, &legacy, true).unwrap();
            assert!(!published.path_updated);
            assert_eq!(fs::read(&legacy).unwrap(), b"old PATH Gateway");
            let mut candidate = prior.clone();
            candidate.binary_path = binary.clone();
            candidate.installed_version = "candidate".to_owned();
            if commit {
                pioneer_config::save_install_state(&state_path, &candidate).unwrap();
                super::remove_obsolete_windows_command(&legacy, &binary).unwrap();
                assert!(!legacy.exists());
                assert_eq!(fs::read(&binary).unwrap(), b"candidate Gateway");
                assert_eq!(
                    pioneer_config::load_install_state(&state_path).unwrap(),
                    Some(candidate)
                );
            } else {
                let blocked = root.join("blocked");
                fs::write(&blocked, b"not a state directory").unwrap();
                assert!(
                    pioneer_config::save_install_state(&blocked.join("state.toml"), &candidate)
                        .is_err()
                );
                let outcome = super::rollback_installation(
                    &binary,
                    &rollback,
                    Some(&binary),
                    None,
                    None,
                    &pioneer_config::InstallManagedBy::Script,
                    super::RollbackSafety::BeforeGatewayStart,
                );
                assert!(outcome.performed());
                super::restore_install_state_snapshot(&state_path, Some(&prior)).unwrap();
                assert_eq!(fs::read(&binary).unwrap(), b"old managed Gateway");
                assert_eq!(fs::read(&legacy).unwrap(), b"old PATH Gateway");
                let restored = pioneer_config::load_install_state(&state_path)
                    .unwrap()
                    .unwrap();
                assert_eq!(restored, prior);
                assert_eq!(
                    fs::read(&restored.binary_path).unwrap(),
                    b"old PATH Gateway"
                );
            }
        }
    }

    #[test]
    #[cfg(unix)]
    fn pre_start_failure_restores_inactive_definition_and_preserves_unregistered_state() {
        use std::os::unix::fs::symlink;
        for registered in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let directory = temporary.path();
            let binary = directory.join("pioneer");
            let rollback = directory.join("pioneer.rollback");
            let link = directory.join("command");
            let definition = directory.join("pioneer.service");
            let state_path = directory.join("install-state.toml");
            fs::write(&binary, b"prior Gateway").unwrap();
            symlink(&binary, &link).unwrap();
            let unit = format!(
                "[Service]\nExecStart={} gateway-service\n[Install]\nWantedBy=default.target\n",
                binary.display()
            );
            if registered {
                fs::write(&definition, &unit).unwrap();
            }
            let registration =
                crate::service::ServiceDefinitionBackup::capture(definition.clone()).unwrap();
            let command = super::CommandLinkBackup::capture_path(link.clone()).unwrap();
            let prior = pioneer_config::InstallState {
                version: pioneer_config::InstallState::CURRENT_VERSION,
                managed_by: pioneer_config::InstallManagedBy::Script,
                installed_version: "prior".to_owned(),
                install_root: Some(directory.to_path_buf()),
                binary_path: binary.clone(),
                updated_at_unix: 42,
            };
            pioneer_config::save_install_state(&state_path, &prior).unwrap();
            // Model the actual removed definition, then fail the version probe
            // on a non-executable candidate. No OS service is created or started.
            if registered {
                fs::remove_file(&definition).unwrap();
            }
            fs::rename(&binary, &rollback).unwrap();
            fs::write(&binary, b"invalid candidate executable").unwrap();
            assert!(super::query_binary_version(&binary).is_err());
            let outcome = super::rollback_installation(
                &binary,
                &rollback,
                Some(&binary),
                None,
                None,
                &pioneer_config::InstallManagedBy::Script,
                super::RollbackSafety::BeforeGatewayStart,
            );
            assert!(outcome.performed());
            assert_eq!(outcome.service_start, super::RestoreOutcome::NotRequired);
            super::restore_install_context(&state_path, Some(&prior), &command, &binary, || {
                registration.restore()
            })
            .unwrap();
            assert_eq!(fs::read(&binary).unwrap(), b"prior Gateway");
            assert_eq!(fs::read_link(&link).unwrap(), binary);
            assert_eq!(
                pioneer_config::load_install_state(&state_path).unwrap(),
                Some(prior)
            );
            assert_eq!(definition.exists(), registered);
            if registered {
                assert_eq!(fs::read_to_string(&definition).unwrap(), unit);
            }
            // The passive restore is retryable and does not invent a definition.
            super::restore_install_context(
                &state_path,
                pioneer_config::load_install_state(&state_path)
                    .unwrap()
                    .as_ref(),
                &command,
                &binary,
                || registration.restore(),
            )
            .unwrap();
            assert_eq!(definition.exists(), registered);
        }
    }

    #[test]
    #[cfg(unix)]
    fn failed_symlink_preparation_preserves_existing_cli_and_cleans_staging() {
        use std::os::unix::{ffi::OsStringExt, fs::symlink};
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path();
        let original = directory.join("pioneer");
        let link = directory.join("command");
        fs::write(&original, b"working Gateway").unwrap();
        symlink(&original, &link).unwrap();
        let invalid = PathBuf::from(std::ffi::OsString::from_vec(b"invalid\0target".to_vec()));
        assert!(super::publish_unix_command_link(&link, &invalid).is_err());
        assert_eq!(fs::read_link(&link).unwrap(), original);
        assert_eq!(fs::read(&link).unwrap(), b"working Gateway");
        assert_eq!(
            fs::read_dir(directory).unwrap().count(),
            2,
            "failed publication leaked its temporary link"
        );
        // The refusal to replace an unmanaged file/directory remains part of
        // the same publication boundary.
        fs::remove_file(&link).unwrap();
        fs::write(&link, b"user command").unwrap();
        assert!(super::publish_unix_command_link(&link, &original).is_err());
        assert_eq!(fs::read(&link).unwrap(), b"user command");
        fs::remove_file(&link).unwrap();
        fs::create_dir(&link).unwrap();
        assert!(super::publish_unix_command_link(&link, &original).is_err());
        assert!(link.is_dir());
    }

    #[test]
    #[cfg(all(target_os = "macos", feature = "computer-use"))]
    fn macos_cu_external_command_link_survives_cleanup_and_pre_start_recovery() {
        use std::os::unix::fs::symlink;
        let config: pioneer_config::AppConfig =
            toml::from_str(include_str!("../../../config/default.toml")).unwrap();
        for fail_before_start in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let root = temporary.path();
            let bin = root.join("bin");
            fs::create_dir(&bin).unwrap();
            let bundle = bin.join("cua").join(super::GATEWAY_APP_EXECUTABLE);
            // This is precisely the configured PIONEER_LINK_PATH=R/bin/pioneer
            // resolved by publication, not a command elsewhere in the fixture.
            let link = bin.join(config.install_binary_file_name().unwrap());
            if fail_before_start {
                write_cua_fixture(&bin.join("cua"), "prior");
                symlink(&bundle, &link).unwrap();
            }
            let command = super::CommandLinkBackup::capture_path(link.clone()).unwrap();
            let state_path = root.join("install-state.toml");
            let prior = pioneer_config::InstallState {
                version: pioneer_config::InstallState::CURRENT_VERSION,
                managed_by: pioneer_config::InstallManagedBy::Script,
                installed_version: "prior".to_owned(),
                install_root: Some(root.to_path_buf()),
                binary_path: bundle.clone(),
                updated_at_unix: 42,
            };
            if fail_before_start {
                pioneer_config::save_install_state(&state_path, &prior).unwrap();
            }
            let rollback = bin.join("pioneer.rollback");
            let obsolete = super::obsolete_macos_gateway_binary(&config, &bin).unwrap();
            assert!(
                obsolete.is_none(),
                "CLI link was misclassified as an executable copy"
            );
            let retired = super::RetiredBinary::new(obsolete, rollback.clone());
            retired.retain().unwrap();
            assert!(
                !rollback.exists(),
                "CLI link was moved into regular-executable rollback"
            );
            write_cua_fixture(&bin.join("cua.next"), "candidate");
            super::activate_install_companions(&bin).unwrap();
            super::publish_unix_command_link(&link, &bundle).unwrap();
            if fail_before_start {
                let blocker = root.join("blocked-state");
                fs::write(&blocker, b"not a directory").unwrap();
                assert!(
                    pioneer_config::save_install_state(&blocker.join("state.toml"), &prior)
                        .is_err()
                );
                let outcome = super::rollback_installation(
                    &bundle,
                    &rollback,
                    Some(&bundle),
                    None,
                    None,
                    &pioneer_config::InstallManagedBy::Script,
                    super::RollbackSafety::BeforeGatewayStart,
                );
                assert!(outcome.performed());
                retired.restore_obsolete(&bundle).unwrap();
                let registration =
                    crate::service::ServiceDefinitionBackup::capture(root.join("Gateway.plist"))
                        .unwrap();
                super::restore_install_context(
                    &state_path,
                    Some(&prior),
                    &command,
                    &bundle,
                    || registration.restore(),
                )
                .unwrap();
                assert_eq!(fs::read(&link).unwrap(), b"prior Gateway bytes");
                assert_eq!(fs::read(&bundle).unwrap(), b"prior Gateway bytes");
                let manifest: serde_json::Value =
                    serde_json::from_slice(&fs::read(bin.join("cua/manifest.json")).unwrap())
                        .unwrap();
                assert_eq!(manifest["version"], "prior");
                assert_eq!(
                    pioneer_config::load_install_state(&state_path).unwrap(),
                    Some(prior)
                );
            } else {
                // This is the same cleanup called by the commit path.
                super::cleanup_macos_cu_artifacts(&config, &bin).unwrap();
                assert_eq!(fs::read_link(&link).unwrap(), bundle);
                assert_eq!(fs::read(&link).unwrap(), b"candidate Gateway bytes");
                // A real external copy remains eligible for cleanup.
                fs::remove_file(&link).unwrap();
                fs::write(&link, b"obsolete full executable").unwrap();
                super::cleanup_macos_cu_artifacts(&config, &bin).unwrap();
                assert!(!link.exists());
                assert_eq!(fs::read(&bundle).unwrap(), b"candidate Gateway bytes");
                super::publish_unix_command_link(&link, &bundle).unwrap();
            }
            assert_eq!(fs::read_link(&link).unwrap(), bundle);
            assert!(!fs::symlink_metadata(&link).unwrap().is_file());
        }
    }

    #[test]
    #[cfg(all(target_os = "macos", feature = "computer-use"))]
    fn failed_manual_transition_restores_retired_external_cli_and_install_state() {
        use std::os::unix::{ffi::OsStringExt, fs::symlink};
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let bin = root.join("bin");
        fs::create_dir(&bin).unwrap();
        write_cua_fixture(&bin.join("cua"), "prior");
        write_cua_fixture(&bin.join("cua.next"), "candidate");
        let bundle = bin.join("cua").join(super::GATEWAY_APP_EXECUTABLE);
        let external = bin.join("pioneer");
        let rollback = bin.join("pioneer.rollback");
        let link = root.join("command");
        let state_path = root.join("install-state.toml");
        fs::write(&external, b"prior external executable").unwrap();
        symlink(&external, &link).unwrap();
        let command = super::CommandLinkBackup::capture_path(link.clone()).unwrap();
        let prior = pioneer_config::InstallState {
            version: pioneer_config::InstallState::CURRENT_VERSION,
            managed_by: pioneer_config::InstallManagedBy::Script,
            installed_version: "prior".to_owned(),
            install_root: Some(root.to_path_buf()),
            binary_path: external.clone(),
            updated_at_unix: 42,
        };
        pioneer_config::save_install_state(&state_path, &prior).unwrap();
        let registration_path = root.join("Gateway.plist");
        let registration =
            crate::service::ServiceDefinitionBackup::capture(registration_path.clone()).unwrap();
        let retained = super::RetiredBinary::new(Some(external.clone()), rollback.clone());
        retained.retain().unwrap();
        assert!(!external.exists());
        super::activate_install_companions(&bin).unwrap();
        let invalid = PathBuf::from(std::ffi::OsString::from_vec(b"invalid\0target".to_vec()));
        assert!(super::publish_unix_command_link(&link, &invalid).is_err());
        let outcome = super::rollback_installation(
            &bundle,
            &rollback,
            Some(&bundle),
            None,
            None,
            &pioneer_config::InstallManagedBy::Script,
            super::RollbackSafety::BeforeGatewayStart,
        );
        assert!(outcome.performed());
        // Bundle-only recovery is insufficient: this attempt independently
        // retired the executable still named by the prior CLI and state.
        assert!(!external.exists());
        retained.restore_obsolete(&bundle).unwrap();
        super::restore_install_context(&state_path, Some(&prior), &command, &bundle, || {
            registration.restore()
        })
        .unwrap();
        assert_eq!(fs::read(&bundle).unwrap(), b"prior Gateway bytes");
        assert_eq!(fs::read(&external).unwrap(), b"prior external executable");
        assert_eq!(fs::read_link(&link).unwrap(), external);
        assert_eq!(fs::read(&link).unwrap(), b"prior external executable");
        assert_eq!(
            pioneer_config::load_install_state(&state_path).unwrap(),
            Some(prior)
        );
        assert_eq!(fs::read(&rollback).unwrap(), b"prior external executable");
        assert!(!registration_path.exists());
    }

    #[test]
    fn schema_uncertainty_blocks_binary_only_rollback_before_any_restore() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("candidate");
        let rollback = dir.path().join("old");
        fs::write(&target, "candidate-with-new-schema").unwrap();
        fs::write(&rollback, "older-binary").unwrap();
        let outcome = super::rollback_and_restore(
            &target,
            &rollback,
            None,
            None,
            true,
            &pioneer_config::InstallManagedBy::Manual,
            super::RollbackSafety::SchemaMayHaveAdvanced,
        );
        assert!(outcome.blocked_compatibility_unknown);
        assert!(!outcome.performed());
        assert_eq!(
            fs::read_to_string(target).unwrap(),
            "candidate-with-new-schema"
        );
        assert_eq!(fs::read_to_string(rollback).unwrap(), "older-binary");
    }

    #[test]
    fn failed_restore_preserves_candidate_and_reports_missing_backup() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("candidate");
        fs::write(&target, "candidate").unwrap();
        let backup = super::RuntimeConfigBackup {
            directory: dir.path().join("missing-config"),
            files: vec![("config.toml".into(), "missing".into())],
        };
        let outcome = super::restore_binary_and_config(
            &target,
            &dir.path().join("missing"),
            Some(dir.path()),
            Some(&backup),
        );
        assert_eq!(
            outcome.binary,
            super::RestoreOutcome::Failed { kind: "not_found" }
        );
        assert_eq!(
            outcome.config,
            super::RestoreOutcome::Failed { kind: "not_found" }
        );
        assert!(!outcome.performed());
        assert!(!outcome.healthy());
        assert_eq!(fs::read_to_string(target).unwrap(), "candidate");
    }

    #[test]
    fn partial_config_restore_retains_binary_backup_and_can_continue_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let source_home = dir.path().join("source");
        fs::create_dir(&source_home).unwrap();
        fs::write(source_home.join("config.toml"), "setting = true\n").unwrap();
        let backup = super::backup_runtime_toml(&source_home, &dir.path().join("config-backup"))
            .unwrap()
            .unwrap();
        let target = dir.path().join("candidate");
        let old = dir.path().join("rollback");
        fs::write(&target, "candidate").unwrap();
        fs::write(&old, "old-binary").unwrap();
        let home = dir.path().join("restore-home");
        fs::write(&home, "blocks directory creation").unwrap();
        let partial = super::rollback_and_restore(
            &target,
            &old,
            Some(&home),
            Some(&backup),
            true,
            &pioneer_config::InstallManagedBy::Manual,
            super::RollbackSafety::BeforeGatewayStart,
        );
        assert_eq!(partial.binary, super::RestoreOutcome::Restored);
        assert!(matches!(
            partial.config,
            super::RestoreOutcome::Failed { .. }
        ));
        assert_eq!(
            partial.service_start,
            super::RestoreOutcome::NotRequired,
            "must not execute the restored service after incomplete config"
        );
        assert!(!partial.performed());
        assert_eq!(fs::read(&target).unwrap(), b"old-binary");
        assert_eq!(fs::read(&old).unwrap(), b"old-binary");
        assert!(backup.directory.join("config.toml").is_file());
        let original = super::InstallRecoveryFailure::error(
            anyhow::anyhow!("original replacement failure"),
            partial,
        );
        let mut bytes = Vec::new();
        crate::print_install_failure_json(super::InstallCommand::Update, &original, &mut bytes)
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(payload["rollback_performed"], false);
        assert_eq!(payload["rollback_outcome"]["binary"], "restored");
        assert!(
            original
                .chain()
                .any(|cause| cause.to_string() == "original replacement failure")
        );
        fs::remove_file(&home).unwrap();
        for _ in 0..2 {
            let restored = super::rollback_and_restore(
                &target,
                &old,
                Some(&home),
                Some(&backup),
                false,
                &pioneer_config::InstallManagedBy::Manual,
                super::RollbackSafety::BeforeGatewayStart,
            );
            assert!(restored.performed());
            assert!(
                !restored.healthy(),
                "restored artifacts alone are not health proof"
            );
            assert_eq!(
                fs::read(home.join("config.toml")).unwrap(),
                b"setting = true\n"
            );
            assert_eq!(fs::read(&old).unwrap(), b"old-binary");
        }
    }

    #[test]
    fn expected_config_backup_must_be_complete_and_match_captured_contents() {
        for damage in [
            "missing-directory",
            "empty-directory",
            "missing-file",
            "directory-entry",
            "empty-file",
            "corrupt-file",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            fs::create_dir(&source).unwrap();
            fs::write(source.join("a.toml"), "original = true\n").unwrap();
            fs::write(source.join("b.toml"), "second = true\n").unwrap();
            let backup = super::backup_runtime_toml(&source, &dir.path().join("backup"))
                .unwrap()
                .unwrap();
            let file = backup.directory.join("b.toml");
            match damage {
                "missing-directory" => fs::remove_dir_all(&backup.directory).unwrap(),
                "empty-directory" => {
                    fs::remove_file(backup.directory.join("a.toml")).unwrap();
                    fs::remove_file(&file).unwrap();
                }
                "missing-file" => fs::remove_file(&file).unwrap(),
                "directory-entry" => {
                    fs::remove_file(&file).unwrap();
                    fs::create_dir(&file).unwrap();
                }
                "empty-file" => fs::write(&file, "").unwrap(),
                "corrupt-file" => fs::write(&file, "tampered = true\n").unwrap(),
                _ => unreachable!(),
            }
            let home = dir.path().join("restored");
            fs::create_dir(&home).unwrap();
            fs::write(home.join("a.toml"), "candidate config").unwrap();
            let error = super::restore_runtime_toml(&home, &backup).unwrap_err();
            if matches!(
                damage,
                "missing-directory" | "empty-directory" | "missing-file"
            ) {
                assert!(
                    error.downcast_ref::<std::io::Error>().is_some(),
                    "metadata error source survives"
                );
            }
            assert_eq!(
                fs::read(home.join("a.toml")).unwrap(),
                b"candidate config",
                "validate full set before any restore"
            );
        }
    }

    #[test]
    fn absence_of_config_is_distinct_from_unexpected_empty_backup() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            super::backup_runtime_toml(dir.path(), &dir.path().join("unused"))
                .unwrap()
                .is_none()
        );
        let backup = super::RuntimeConfigBackup {
            directory: dir.path().to_owned(),
            files: Vec::new(),
        };
        assert!(super::restore_runtime_toml(&dir.path().join("restored"), &backup).is_err());
        // A deliberately empty source TOML can be restored only if captured as
        // such; corruption means disagreement with the captured source bytes.
        fs::write(dir.path().join("config.toml"), "").unwrap();
        let captured = super::backup_runtime_toml(dir.path(), &dir.path().join("captured"))
            .unwrap()
            .unwrap();
        let home = dir.path().join("restored");
        super::restore_runtime_toml(&home, &captured).unwrap();
        assert_eq!(fs::read(home.join("config.toml")).unwrap(), b"");
    }

    #[test]
    fn restored_files_require_separate_service_health_proof() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("candidate");
        let backup = dir.path().join("rollback");
        fs::write(&target, "candidate").unwrap();
        fs::write(&backup, "old-binary").unwrap();
        let outcome = super::rollback_and_restore(
            &target,
            &backup,
            None,
            None,
            false,
            &pioneer_config::InstallManagedBy::Manual,
            super::RollbackSafety::BeforeGatewayStart,
        );
        assert!(outcome.performed());
        assert!(!outcome.healthy());
        assert_eq!(fs::read_to_string(target).unwrap(), "old-binary");
    }

    #[test]
    fn install_failure_json_uses_typed_recovery_instead_of_error_text() {
        let mut bytes = Vec::new();
        crate::print_install_failure_json(
            super::InstallCommand::Update,
            &anyhow::anyhow!("untrusted claim: update rolled back"),
            &mut bytes,
        )
        .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(payload["rollback_performed"], false);
        bytes.clear();
        let error = super::InstallRecoveryFailure::error(
            anyhow::anyhow!("original start failure"),
            super::RollbackOutcome::blocked(),
        )
        .context("outer report context");
        crate::print_install_failure_json(super::InstallCommand::Update, &error, &mut bytes)
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(payload["rollback_performed"], false);
        assert_eq!(payload["rollback_healthy"], false);
        assert_eq!(
            payload["rollback_outcome"]["blocked_compatibility_unknown"],
            true
        );
        assert!(
            payload["error"]
                .as_str()
                .unwrap()
                .contains("original start failure")
        );
    }

    #[test]
    fn health_failure_distinguishes_missing_status_command_from_no_observation() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            super::probe_gateway_status(&dir.path().join("missing")),
            Err(super::GatewayHealthFailure::MissingBinary)
        ));
        assert_eq!(
            super::wait_for_gateway_health(&dir.path().join("missing"), Duration::ZERO),
            Err(super::GatewayHealthFailure::NotObserved)
        );
    }

    #[test]
    fn install_reports_preserve_transient_download_classification_after_output_failure() {
        struct FailedWriter(std::io::ErrorKind);

        impl std::io::Write for FailedWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(self.0, "controlled report failure"))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                panic!("report writer must not flush after a failed write");
            }
        }

        for name in ["install", "update", "self-update"] {
            let (options, _) = crate::parse_install_command_options(name, std::iter::empty())
                .expect("install command options");
            for kind in [
                std::io::ErrorKind::BrokenPipe,
                std::io::ErrorKind::PermissionDenied,
            ] {
                let original = anyhow::Error::new(super::InstallerTransientDownloadError {
                    url: "https://example.invalid/asset".into(),
                    attempts: 4,
                    last_error: "connection timeout".into(),
                })
                .context("resolve install source");
                let error = crate::finish_install_result(
                    options.command,
                    Err(original),
                    true,
                    &mut FailedWriter(kind),
                )
                .expect_err("transient installation failure remains a failed command");

                assert!(is_transient_download_error(&error));
                assert!(
                    error
                        .downcast_ref::<super::InstallerTransientDownloadError>()
                        .is_some()
                );
                assert!(format!("{error:#}").contains("resolve install source"));
                assert_eq!(
                    crate::install_error_code(options.command, &format!("{error:#}")),
                    "download_transient_network_failure"
                );
                assert_eq!(
                    error
                        .downcast_ref::<crate::InstallFailureReportError>()
                        .is_some(),
                    kind != std::io::ErrorKind::BrokenPipe
                );
            }
        }
    }

    #[test]
    fn parses_prefixed_checksum_line() {
        let path = unique_temp_path("checksum-prefixed");
        fs::write(
            &path,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa pioneer-gateway-linux-x86_64.zip\n",
        )
        .expect("write checksums file");

        let parsed = expected_checksum_for_asset(&path, "pioneer-gateway-linux-x86_64.zip")
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
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb pioneer-gateway-linux-aarch64.zip\n",
        )
        .expect("write checksums file");

        let error = expected_checksum_for_asset(&path, "pioneer-gateway-linux-x86_64.zip")
            .expect_err("missing asset should fail");

        assert!(format!("{error:#}").contains("checksum for"));
        let _ = fs::remove_file(path);
    }

    #[cfg(feature = "computer-use")]
    fn write_cua_fixture(payload: &std::path::Path, release: &str) -> &'static str {
        let daemon = if cfg!(target_os = "macos") {
            "Pioneer Gateway.app/Contents/Helpers/cua-driver"
        } else if cfg!(windows) {
            "cua-driver.exe"
        } else {
            "cua-driver"
        };
        fs::create_dir_all(payload.join(daemon).parent().unwrap()).unwrap();
        fs::write(payload.join(daemon), format!("{release} daemon bytes")).unwrap();
        fs::write(payload.join("LICENSE.cua-driver.md"), b"fixture license").unwrap();
        fs::write(payload.join("THIRD_PARTY_NOTICES.md"), b"fixture notices").unwrap();
        #[cfg(target_os = "macos")]
        {
            let contents = payload.join("Pioneer Gateway.app/Contents");
            fs::create_dir_all(contents.join("MacOS")).unwrap();
            let binary = contents.join("MacOS/pioneer");
            fs::write(&binary, format!("{release} Gateway bytes")).unwrap();
            super::make_binary_executable(&binary).unwrap();
            fs::write(contents.join("Info.plist"), b"fixture bundle metadata").unwrap();
            fs::write(
                contents.join("MacOS/libonnxruntime.dylib"),
                format!("{release} library"),
            )
            .unwrap();
            fs::write(
                contents.join(format!("MacOS/libonnxruntime.{release}.dylib")),
                release,
            )
            .unwrap();
            fs::create_dir(contents.join("_CodeSignature")).unwrap();
            fs::write(
                contents.join("_CodeSignature/CodeResources"),
                format!("{release} signature fixture"),
            )
            .unwrap();
        }
        #[cfg(windows)]
        fs::write(payload.join("cua-driver-uia.exe"), format!("{release} UIA")).unwrap();
        let os = if cfg!(target_os = "macos") {
            "apple-darwin"
        } else if cfg!(windows) {
            "pc-windows-msvc"
        } else {
            "unknown-linux-gnu"
        };
        fs::write(payload.join("manifest.json"), serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "repository": "https://github.com/trycua/cua",
            "target": format!("{}-{os}", std::env::consts::ARCH),
            "revision": "upstream-revision", "version": release, "daemon": daemon,
            "permission_owner": "gateway", "sha256": super::sha256_file(&payload.join(daemon)).unwrap(),
        })).unwrap()).unwrap();
        daemon
    }

    #[test]
    #[cfg(feature = "computer-use")]
    fn installer_rollback_restores_the_prior_daemon_payload() {
        for previously_installed in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let directory = temporary.path();
            let source = directory.join("fixture");
            let daemon = write_cua_fixture(&source, "candidate");
            if previously_installed {
                write_cua_fixture(&directory.join("cua"), "prior");
            }
            let manifest_bytes = fs::read(source.join("manifest.json")).unwrap();
            let mut defects = vec!["manifest", "owner", "hash", "companion", "none"];
            if cfg!(target_os = "macos") {
                defects.insert(0, "external executable");
            }
            for defect in defects {
                let mut manifest: serde_json::Value =
                    serde_json::from_slice(&manifest_bytes).unwrap();
                if defect == "owner" {
                    manifest["permission_owner"] = "client".into();
                }
                if defect == "hash" {
                    manifest["sha256"] = "wrong-hash".into();
                }
                let asset = directory.join("candidate.zip");
                let mut archive = zip::ZipWriter::new(fs::File::create(&asset).unwrap());
                let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
                if !cfg!(target_os = "macos") || defect == "external executable" {
                    archive
                        .start_file(
                            if cfg!(windows) {
                                "pioneer.exe"
                            } else {
                                "pioneer"
                            },
                            options,
                        )
                        .unwrap();
                    archive.write_all(b"candidate Gateway bytes").unwrap();
                }
                let mut pending = vec![source.clone()];
                while let Some(folder) = pending.pop() {
                    for entry in fs::read_dir(folder).unwrap() {
                        let path = entry.unwrap().path();
                        if path.is_dir() {
                            pending.push(path);
                            continue;
                        }
                        let name = path
                            .strip_prefix(&source)
                            .unwrap()
                            .to_string_lossy()
                            .replace('\\', "/");
                        let companion = if cfg!(windows) {
                            "cua-driver-uia.exe"
                        } else if cfg!(target_os = "macos") {
                            "Pioneer Gateway.app/Contents/Info.plist"
                        } else {
                            "THIRD_PARTY_NOTICES.md"
                        };
                        if (defect == "companion" && name == companion)
                            || (defect == "manifest" && name == "manifest.json")
                        {
                            continue;
                        }
                        archive.start_file(format!("cua/{name}"), options).unwrap();
                        let bytes = if name == "manifest.json" {
                            serde_json::to_vec(&manifest).unwrap()
                        } else {
                            fs::read(&path).unwrap()
                        };
                        archive.write_all(&bytes).unwrap();
                    }
                }
                archive.finish().unwrap();
                let admission = tempfile::tempdir_in(directory).unwrap();
                let candidate = admission.path().join("pioneer.next");
                let result = super::unpack_asset_to_binary(&asset, &candidate);
                if defect != "none" {
                    assert!(result.is_err(), "accepted corrupt {defect} archive");
                    assert!(!directory.join("cua.rollback").exists());
                    assert_eq!(
                        directory.join("cua").join(daemon).exists(),
                        previously_installed
                    );
                } else {
                    result.unwrap();
                    if cfg!(target_os = "macos") {
                        assert!(
                            !candidate.exists(),
                            "CU extraction must not create an external Gateway"
                        );
                        assert_eq!(
                            fs::read(
                                admission
                                    .path()
                                    .join("cua.next")
                                    .join(super::GATEWAY_APP_EXECUTABLE)
                            )
                            .unwrap(),
                            b"candidate Gateway bytes"
                        );
                    } else {
                        assert_eq!(fs::read(candidate).unwrap(), b"candidate Gateway bytes");
                    }
                    fs::rename(
                        admission.path().join("cua.next"),
                        directory.join("cua.next"),
                    )
                    .unwrap();
                }
            }
            super::activate_install_companions(directory).unwrap();
            assert_eq!(
                fs::read(directory.join("cua").join(daemon)).unwrap(),
                b"candidate daemon bytes"
            );
            #[cfg(target_os = "macos")]
            {
                let contents = directory.join("cua/Pioneer Gateway.app/Contents");
                assert!(!directory.join("pioneer").exists());
                assert_eq!(
                    fs::read(contents.join("MacOS/pioneer")).unwrap(),
                    b"candidate Gateway bytes"
                );
                assert_eq!(
                    fs::read(contents.join("MacOS/libonnxruntime.dylib")).unwrap(),
                    b"candidate library"
                );
                assert!(!contents.join("MacOS/libonnxruntime.prior.dylib").exists());
            }
            for _ in 0..2 {
                super::restore_cua_payload(directory).unwrap();
                assert!(directory.join("cua.rollback").is_dir());
                if previously_installed {
                    assert_eq!(
                        fs::read(directory.join("cua").join(daemon)).unwrap(),
                        b"prior daemon bytes"
                    );
                    #[cfg(target_os = "macos")]
                    {
                        let contents = directory.join("cua/Pioneer Gateway.app/Contents");
                        assert_eq!(
                            fs::read(contents.join("MacOS/pioneer")).unwrap(),
                            b"prior Gateway bytes"
                        );
                        assert_eq!(
                            fs::read(contents.join("MacOS/libonnxruntime.dylib")).unwrap(),
                            b"prior library"
                        );
                        assert_eq!(
                            fs::read(contents.join("_CodeSignature/CodeResources")).unwrap(),
                            b"prior signature fixture"
                        );
                        assert!(
                            !contents
                                .join("MacOS/libonnxruntime.candidate.dylib")
                                .exists()
                        );
                    }
                } else {
                    assert!(!directory.join("cua").join(daemon).exists());
                }
            }
        }
    }

    #[test]
    #[cfg(all(target_os = "macos", feature = "computer-use"))]
    fn bundle_recovery_obeys_schema_safety_and_retains_complete_prior_bundle() {
        for safety in [
            super::RollbackSafety::BeforeGatewayStart,
            super::RollbackSafety::SchemaMayHaveAdvanced,
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let directory = temporary.path();
            write_cua_fixture(&directory.join("cua"), "prior");
            write_cua_fixture(&directory.join("cua.next"), "candidate");
            super::activate_install_companions(directory).unwrap();
            let target = directory.join("cua").join(super::GATEWAY_APP_EXECUTABLE);
            let outcome = super::rollback_installation(
                &target,
                &directory.join("pioneer.rollback"),
                Some(&target),
                None,
                None,
                &pioneer_config::InstallManagedBy::Manual,
                safety,
            );
            let expected = if safety == super::RollbackSafety::BeforeGatewayStart {
                "prior"
            } else {
                "candidate"
            };
            assert_eq!(
                outcome.blocked_compatibility_unknown,
                safety == super::RollbackSafety::SchemaMayHaveAdvanced
            );
            assert_eq!(
                outcome.performed(),
                safety == super::RollbackSafety::BeforeGatewayStart
            );
            assert_eq!(
                fs::read(&target).unwrap(),
                format!("{expected} Gateway bytes").as_bytes()
            );
            let manifest: serde_json::Value =
                serde_json::from_slice(&fs::read(directory.join("cua/manifest.json")).unwrap())
                    .unwrap();
            assert_eq!(manifest["version"], expected);
            let contents = target.parent().unwrap().parent().unwrap();
            assert_eq!(
                fs::read(contents.join("_CodeSignature/CodeResources")).unwrap(),
                format!("{expected} signature fixture").as_bytes()
            );
            assert_eq!(
                fs::read(contents.join("MacOS/libonnxruntime.dylib")).unwrap(),
                format!("{expected} library").as_bytes()
            );
            assert_eq!(
                fs::read(
                    directory
                        .join("cua.rollback")
                        .join(super::GATEWAY_APP_EXECUTABLE)
                )
                .unwrap(),
                b"prior Gateway bytes"
            );
        }
    }

    #[test]
    #[cfg(all(target_os = "macos", not(feature = "computer-use")))]
    fn standalone_candidate_probe_uses_only_its_matching_native_libraries() {
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path();
        fs::write(directory.join("libonnxruntime.dylib"), b"live library").unwrap();
        fs::write(directory.join("libonnxruntime.prior.dylib"), b"prior only").unwrap();
        fs::create_dir_all(directory.join("cua.next/native-libs")).unwrap();
        fs::write(
            directory.join("cua.next/native-libs/libonnxruntime.dylib"),
            b"candidate library",
        )
        .unwrap();
        let staged = directory.join("pioneer.new");
        fs::write(
            &staged,
            br#"#!/bin/sh
set -eu
base=${0%/*}
[ "$1" = version ] && [ "$2" = --json ]
[ "$(cat "$base/libonnxruntime.dylib")" = 'candidate library' ]
[ ! -e "$base/libonnxruntime.prior.dylib" ]
printf '%s\n' '{"version":"candidate","compiled_database_migrations":["candidate-migration"]}'
"#,
        )
        .unwrap();
        super::make_binary_executable(&staged).unwrap();
        let (_layout, candidate) = super::prepare_candidate_probe_layout(&staged).unwrap();
        let version = super::query_binary_version_output(&candidate).unwrap();
        assert_eq!(version.version, "candidate");
        assert!(
            super::ensure_candidate_schema_compatible(
                &["unavailable-migration".into()],
                version.compiled_database_migrations.as_deref()
            )
            .is_err()
        );
        assert_eq!(
            fs::read(directory.join("libonnxruntime.dylib")).unwrap(),
            b"live library"
        );
        assert_eq!(
            fs::read(directory.join("libonnxruntime.prior.dylib")).unwrap(),
            b"prior only"
        );
        assert!(!directory.join("cua.rollback").exists());
    }

    #[test]
    fn installer_archive_rejects_unsafe_members_before_candidate_execution() {
        for name in [
            "../escape",
            "cua/../escape",
            "cua/a\\b",
            "cua/a:stream",
            "cua/CON.txt",
            "cua/name.",
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let archive_path = temporary.path().join("unsafe.zip");
            let mut archive = zip::ZipWriter::new(fs::File::create(&archive_path).unwrap());
            archive
                .start_file(name, zip::write::SimpleFileOptions::default())
                .unwrap();
            archive.write_all(b"untrusted bytes").unwrap();
            archive.finish().unwrap();
            let error =
                super::unpack_asset_to_binary(&archive_path, &temporary.path().join("candidate"))
                    .unwrap_err();
            assert!(
                error.to_string().contains("invalid Gateway archive path"),
                "{name}: {error:#}"
            );
            assert!(!temporary.path().join("escape").exists());
            assert!(!temporary.path().join("candidate").exists());
        }
        let temporary = tempfile::tempdir().unwrap();
        let archive_path = temporary.path().join("linked.zip");
        let mut archive = zip::ZipWriter::new(fs::File::create(&archive_path).unwrap());
        archive
            .add_symlink(
                "cua/link",
                "../../escape",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        archive.finish().unwrap();
        let error =
            super::unpack_asset_to_binary(&archive_path, &temporary.path().join("candidate"))
                .unwrap_err();
        assert!(error.to_string().contains("links, special files"));
        assert!(!temporary.path().join("cua.next/link").exists());
    }

    #[test]
    fn release_asset_name_preserves_server_updater_contract_and_cu_opt_in() {
        for (os, arch, server, computer_use) in [
            (
                "linux",
                "x86_64",
                "pioneer-gateway-linux-x86_64.gz",
                "pioneer-gateway-linux-x86_64-computer-use-package.zip",
            ),
            (
                "linux",
                "aarch64",
                "pioneer-gateway-linux-aarch64.gz",
                "pioneer-gateway-linux-aarch64-computer-use-package.zip",
            ),
            (
                "macos",
                "aarch64",
                "pioneer-gateway-macos-aarch64.gz",
                "pioneer-gateway-macos-aarch64-computer-use-package.zip",
            ),
            (
                "macos",
                "x86_64",
                "pioneer-gateway-macos-x86_64.zip",
                "pioneer-gateway-macos-x86_64-computer-use-package.zip",
            ),
            (
                "windows",
                "x86_64",
                "pioneer-gateway-windows-x86_64.zip",
                "pioneer-gateway-windows-x86_64-computer-use-package.zip",
            ),
            (
                "windows",
                "aarch64",
                "pioneer-gateway-windows-aarch64.zip",
                "pioneer-gateway-windows-aarch64-computer-use-package.zip",
            ),
        ] {
            assert_eq!(
                super::gateway_asset_file_name_for(os, arch, false).unwrap(),
                server
            );
            assert_eq!(
                super::gateway_asset_file_name_for(os, arch, true).unwrap(),
                computer_use
            );
        }
        assert_eq!(
            super::gateway_asset_file_name().unwrap(),
            super::gateway_asset_file_name_for(
                std::env::consts::OS,
                std::env::consts::ARCH,
                cfg!(feature = "computer-use")
            )
            .unwrap()
        );
    }

    #[test]
    fn server_zip_admission_needs_no_daemon_but_rejects_cu_payload() {
        let temp = tempfile::tempdir().unwrap();
        let gzip = temp.path().join("gateway.gz");
        let mut encoder = flate2::write::GzEncoder::new(
            fs::File::create(&gzip).unwrap(),
            flate2::Compression::default(),
        );
        encoder.write_all(b"server gzip executable").unwrap();
        encoder.finish().unwrap();
        let candidate = temp.path().join("gzip-candidate");
        let result = super::unpack_asset_to_binary(&gzip, &candidate);
        if !cfg!(feature = "computer-use")
            && !cfg!(windows)
            && !cfg!(all(target_os = "macos", target_arch = "x86_64"))
        {
            result.unwrap();
            assert_eq!(fs::read(&candidate).unwrap(), b"server gzip executable");
        } else {
            assert!(result.unwrap_err().to_string().contains("format"));
        }
        let wrong_format = temp.path().join("gateway.tar");
        fs::copy(&gzip, &wrong_format).unwrap();
        assert!(
            super::unpack_asset_to_binary(&wrong_format, &candidate)
                .unwrap_err()
                .to_string()
                .contains("format")
        );
        if cfg!(feature = "computer-use") {
            return;
        }
        for with_cua in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let asset = temp.path().join("gateway.zip");
            let mut zip = zip::ZipWriter::new(fs::File::create(&asset).unwrap());
            let options = zip::write::SimpleFileOptions::default();
            zip.start_file(
                if cfg!(windows) {
                    "pioneer.exe"
                } else {
                    "pioneer"
                },
                options,
            )
            .unwrap();
            zip.write_all(b"server executable fixture").unwrap();
            #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
            {
                zip.start_file("libonnxruntime.dylib", options).unwrap();
                zip.write_all(b"existing Intel companion").unwrap();
            }
            if with_cua {
                zip.start_file("cua/manifest.json", options).unwrap();
                zip.write_all(b"{}").unwrap();
            }
            zip.finish().unwrap();
            let candidate = temp.path().join("candidate");
            let result = super::unpack_asset_to_binary(&asset, &candidate);
            if with_cua {
                assert!(result.unwrap_err().to_string().contains("server archive"));
            } else {
                result.unwrap();
                assert_eq!(fs::read(&candidate).unwrap(), b"server executable fixture");
            }
        }
    }

    #[test]
    #[cfg(not(feature = "computer-use"))]
    fn server_transition_retires_cu_app_and_preserves_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path();
        super::activate_install_companions(directory).unwrap();
        assert!(!directory.join("cua").exists());
        assert!(!directory.join("cua.rollback").exists());
        fs::create_dir_all(directory.join("cua/Pioneer Gateway.app")).unwrap();
        fs::write(directory.join("cua/prior-daemon"), b"prior").unwrap();
        #[cfg(target_os = "macos")]
        {
            fs::write(
                directory.join("libonnxruntime.dylib"),
                b"prior Intel library",
            )
            .unwrap();
            fs::create_dir_all(directory.join("cua.next/native-libs")).unwrap();
            fs::write(
                directory.join("cua.next/native-libs/libonnxruntime.dylib"),
                b"server Intel library",
            )
            .unwrap();
        }
        super::activate_install_companions(directory).unwrap();
        #[cfg(target_os = "macos")]
        assert_eq!(
            fs::read(directory.join("libonnxruntime.dylib")).unwrap(),
            b"server Intel library"
        );
        assert!(!directory.join("cua/Pioneer Gateway.app").exists());
        assert!(!directory.join("cua/prior-daemon").exists());
        super::restore_cua_payload(directory).unwrap();
        assert_eq!(
            fs::read(directory.join("cua/prior-daemon")).unwrap(),
            b"prior"
        );
        assert!(directory.join("cua/Pioneer Gateway.app").is_dir());
        assert!(directory.join("cua.rollback/prior-daemon").is_file());
        #[cfg(target_os = "macos")]
        {
            assert_eq!(
                fs::read(directory.join("libonnxruntime.dylib")).unwrap(),
                b"prior Intel library"
            );
            assert_eq!(
                fs::read(directory.join("cua.rollback/native-libs/libonnxruntime.dylib")).unwrap(),
                b"prior Intel library"
            );
        }
    }

    #[test]
    fn release_version_preserves_repository_tag_convention() {
        assert_eq!(normalize_version_tag(" 0.53.12 "), "0.53.12");
        assert_eq!(normalize_version_tag("v0.53.12"), "v0.53.12");
    }

    #[test]
    fn stable_channel_skips_newer_release_until_gateway_assets_are_ready() {
        let releases = vec![
            release("0.53.12", false, &["desktop-update-manifest.json"]),
            release(
                "0.53.11",
                false,
                &["pioneer-gateway-linux-x86_64.zip", "SHA256SUMS"],
            ),
        ];

        let selected = select_ready_release_tag(
            ReleaseChannel::Stable,
            releases.iter(),
            "pioneer-gateway-linux-x86_64.zip",
        )
        .expect("previous complete stable release should be selected");

        assert_eq!(selected, "0.53.11");
    }

    #[test]
    fn release_selection_requires_both_gateway_and_checksum_assets() {
        let releases = vec![release(
            "0.53.12",
            false,
            &["pioneer-gateway-linux-x86_64.zip"],
        )];

        let error = select_ready_release_tag(
            ReleaseChannel::Stable,
            releases.iter(),
            "pioneer-gateway-linux-x86_64.zip",
        )
        .expect_err("release without checksums must remain unavailable");

        assert!(format!("{error:#}").contains("no fully published"));
    }

    #[test]
    fn stable_channel_ignores_prereleases_even_when_they_are_ready() {
        let releases = vec![
            release(
                "0.54.0-beta.1",
                true,
                &["pioneer-gateway-linux-x86_64.zip", "SHA256SUMS"],
            ),
            release(
                "0.53.12",
                false,
                &["pioneer-gateway-linux-x86_64.zip", "SHA256SUMS"],
            ),
        ];

        let selected = select_ready_release_tag(
            ReleaseChannel::Stable,
            releases.iter(),
            "pioneer-gateway-linux-x86_64.zip",
        )
        .expect("stable release should be selected");

        assert_eq!(selected, "0.53.12");
    }

    fn release(tag_name: &str, prerelease: bool, assets: &[&str]) -> GithubRelease {
        GithubRelease {
            tag_name: tag_name.to_owned(),
            draft: false,
            prerelease,
            assets: assets
                .iter()
                .map(|name| GithubReleaseAsset {
                    name: (*name).to_owned(),
                })
                .collect(),
        }
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
            &test_http_client(),
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
    fn parses_start_output_from_json() {
        let output = parse_start_output(
            br#"{
                "phase":"started",
                "warnings":[
                    {"code":"linux_linger_enable_failed","message":"run loginctl"}
                ],
                "stage_timings":[
                    {"stage":"service.manager.activate","started_after_ms":10,"duration_ms":20,"outcome":"ok"}
                ]
            }"#,
        );

        assert_eq!(output.warnings.len(), 1);
        assert_eq!(output.warnings[0].code, "linux_linger_enable_failed");
        assert_eq!(output.warnings[0].message, "run loginctl");
        assert_eq!(output.stage_timings.len(), 1);
        assert_eq!(output.stage_timings[0].stage, "service.manager.activate");
        assert_eq!(output.stage_timings[0].duration_ms, 20);
    }

    #[test]
    fn handles_missing_or_invalid_start_output_fields() {
        let missing = parse_start_output(br#"{"phase":"started"}"#);
        assert!(missing.warnings.is_empty());
        assert!(missing.stage_timings.is_empty());

        let invalid = parse_start_output(b"not json");
        assert_eq!(invalid.warnings.len(), 1);
        assert_eq!(
            invalid.warnings[0].code,
            "gateway_start_output_parse_failed"
        );
        assert!(invalid.stage_timings.is_empty());
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
        let success_response = http_response(200, "OK", body);
        let server = thread::spawn(move || {
            let (mut first_stream, _) = listener.accept().expect("accept first HTTP request");
            read_http_request(&mut first_stream);
            // Keep the first connection open without replying until the retry
            // arrives. This forces a request timeout without racing a sleep
            // against the client's deadline or starving the successful retry.
            let (mut second_stream, _) = listener.accept().expect("accept retry HTTP request");
            read_http_request(&mut second_stream);
            second_stream
                .write_all(&success_response)
                .expect("write successful retry response");
            drop(first_stream);
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
        let mut read = 0;
        // TCP may split headers across reads, even on loopback.
        while !request[..read]
            .windows(4)
            .any(|window| window == b"\r\n\r\n")
        {
            assert!(read < request.len(), "request headers exceed fixture limit");
            let received = stream
                .read(&mut request[read..])
                .expect("read HTTP request");
            assert!(
                received > 0,
                "connection closed before request headers completed"
            );
            read += received;
        }
    }

    #[test]
    fn candidate_must_know_every_applied_migration_without_version_ordering() {
        let applied = vec!["migration_new".to_owned()];
        let old_candidate = vec!["migration_old".to_owned()];
        assert!(super::ensure_candidate_schema_compatible(&applied, Some(&old_candidate)).is_err());
        let compatible = vec![
            "migration_old".to_owned(),
            "migration_new".to_owned(),
            "migration_future".to_owned(),
        ];
        super::ensure_candidate_schema_compatible(&applied, Some(&compatible)).unwrap();
        assert!(super::ensure_candidate_schema_compatible(&applied, None).is_err());
        assert!(super::ensure_candidate_schema_compatible(&applied, Some(&[])).is_err());
        assert!(
            super::ensure_candidate_schema_compatible(
                &applied,
                Some(&["migration_new".into(), "migration_new".into()])
            )
            .is_err()
        );
        // Legacy version JSON parses, but cannot supply compatibility proof.
        let legacy: super::VersionOutput =
            serde_json::from_str(r#"{"version":"999.0.0"}"#).unwrap();
        assert!(
            super::ensure_candidate_schema_compatible(
                &applied,
                legacy.compiled_database_migrations.as_deref()
            )
            .is_err()
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

    #[cfg(not(windows))]
    #[test]
    fn unix_path_setup_updates_shell_profile_without_creating_shadow_file() {
        let cases = [
            (
                "home-profile-no-bash-profile",
                "/bin/bash",
                &[(".profile", "# test profile\n")][..],
                ".profile",
                ".bash_profile",
            ),
            (
                "home-zsh-zprofile",
                "/bin/zsh",
                &[][..],
                ".zprofile",
                ".zshrc",
            ),
        ];
        for (prefix, shell, initial_files, target_name, absent_name) in cases {
            let _guard = env_lock().lock().expect("env lock poisoned");
            let home = unique_temp_path(prefix);
            fs::create_dir_all(&home).expect("create temp home dir");
            let bin_dir = home.join(".local").join("bin");
            fs::create_dir_all(&bin_dir).expect("create bin dir");
            for (name, content) in initial_files {
                fs::write(home.join(name), content).expect("write initial profile");
            }
            let old_home = std::env::var_os("HOME");
            let old_path = std::env::var_os("PATH");
            let old_shell = std::env::var_os("SHELL");
            unsafe {
                std::env::set_var("HOME", &home);
                std::env::set_var("PATH", "/usr/bin:/bin");
                std::env::set_var("SHELL", shell);
            }
            let result =
                ensure_unix_user_path_configured(&bin_dir).expect("path setup should not fail");
            restore_env_var("HOME", old_home);
            restore_env_var("PATH", old_path);
            restore_env_var("SHELL", old_shell);
            let profile_content =
                fs::read_to_string(home.join(target_name)).expect("read target profile");
            assert!(result.path_updated);
            assert!(
                result.warnings.is_empty(),
                "warnings: {:#?}",
                result.warnings
            );
            assert!(profile_content.contains("export PATH=\"$HOME/.local/bin:$PATH\""));
            assert!(
                !home.join(absent_name).exists(),
                "installer should not create {absent_name} for {shell}"
            );
            let _ = fs::remove_dir_all(&home);
        }
    }
}
