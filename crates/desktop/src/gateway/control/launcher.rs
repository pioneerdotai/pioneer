use crate::gateway::connectivity::is_gateway_reachable;
use crate::gateway::timings::GatewayTimings;
use anyhow::{Context, Result, bail};
use pioneer_config::AppConfig;
use pioneer_protocol::normalize_device_activation_code;
use serde::Deserialize;
use std::ffi::OsStr;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::Instant;
use tracing::info;
use zeroize::Zeroizing;

use super::install::managed_gateway_install;
use super::status::is_configured_service_active;

const DESKTOP_MANAGED_BY: &str = "desktop";

enum StartAttempt {
    Started {
        warnings: Vec<GatewayInstallWarning>,
    },
    ProgramNotFound,
}

enum DeviceCreateAttempt {
    Code(Zeroizing<String>),
    ProgramNotFound,
}

#[derive(Debug, Clone)]
struct BundledGatewayBootstrap {
    bootstrap_binary_path: PathBuf,
    asset_path: PathBuf,
    checksums_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct GatewayInstallWarning {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

#[derive(Debug, Deserialize)]
struct InstallCommandOutput {
    #[serde(default)]
    warnings: Vec<InstallWarningEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum InstallWarningEntry {
    Structured(GatewayInstallWarning),
    Message(String),
}

pub(crate) fn start_gateway_service(
    service_name: &str,
    listen_addr: &str,
    timings: &GatewayTimings,
) -> Result<Vec<GatewayInstallWarning>> {
    ensure_desktop_command_config_is_safe()?;

    if let Some(command) = make_bundled_gateway_install_command("install", true)
        && let Some(warnings) = try_start_with_launcher(
            "gateway bundled installer",
            command,
            service_name,
            listen_addr,
            timings,
        )?
    {
        return Ok(warnings);
    }

    if let Some(command) = make_managed_pioneer_start_command()
        && let Some(warnings) = try_start_with_launcher(
            "managed pioneer binary",
            command,
            service_name,
            listen_addr,
            timings,
        )?
    {
        return Ok(warnings);
    }

    if let Some(command) = make_development_pioneer_start_command()
        && let Some(warnings) = try_start_with_launcher(
            "development cargo pioneer-dev",
            command,
            service_name,
            listen_addr,
            timings,
        )?
    {
        return Ok(warnings);
    }

    if let Some(warnings) = try_start_with_launcher(
        "configured pioneer command in PATH",
        make_pioneer_start_command(),
        service_name,
        listen_addr,
        timings,
    )? {
        return Ok(warnings);
    }

    bail!("{}", t!("errors.gateway.start_command_failed_manual_help"))
}

pub(crate) fn update_gateway_service_from_desktop_binary(
    service_name: &str,
    listen_addr: &str,
    timings: &GatewayTimings,
) -> Result<Vec<GatewayInstallWarning>> {
    ensure_desktop_command_config_is_safe()?;

    if let Some(command) = make_bundled_gateway_install_command("update", false)
        && let Some(warnings) = try_start_with_launcher(
            "gateway bundled installer",
            command,
            service_name,
            listen_addr,
            timings,
        )?
    {
        return Ok(warnings);
    }

    if let Some(command) = make_managed_pioneer_start_command()
        && let Some(warnings) = try_start_with_launcher(
            "managed pioneer binary",
            command,
            service_name,
            listen_addr,
            timings,
        )?
    {
        return Ok(warnings);
    }

    if let Some(command) = make_development_pioneer_start_command()
        && let Some(warnings) = try_start_with_launcher(
            "development cargo pioneer-dev",
            command,
            service_name,
            listen_addr,
            timings,
        )?
    {
        return Ok(warnings);
    }

    if let Some(warnings) = try_start_with_launcher(
        "configured pioneer command in PATH",
        make_pioneer_start_command(),
        service_name,
        listen_addr,
        timings,
    )? {
        return Ok(warnings);
    }

    bail!("{}", t!("errors.gateway.start_command_failed_manual_help"))
}

pub(crate) fn create_local_pending_device_session() -> Result<Zeroizing<String>> {
    ensure_desktop_command_config_is_safe()?;

    if let Some(command) = make_managed_pioneer_device_create_command()
        && let Some(activation_code) =
            try_create_pending_device_session_with_launcher("managed pioneer binary", command)?
    {
        return Ok(activation_code);
    }

    if let Some(command) = make_development_pioneer_device_create_command()
        && let Some(activation_code) = try_create_pending_device_session_with_launcher(
            "development cargo pioneer-dev",
            command,
        )?
    {
        return Ok(activation_code);
    }

    if let Some(activation_code) = try_create_pending_device_session_with_launcher(
        "configured pioneer command in PATH",
        make_pioneer_device_create_command(),
    )? {
        return Ok(activation_code);
    }

    bail!(
        "failed to create a pending device session; make sure `pioneer device create` is available"
    )
}

fn try_start_with_launcher(
    launcher: &str,
    command: Command,
    service_name: &str,
    listen_addr: &str,
    timings: &GatewayTimings,
) -> Result<Option<Vec<GatewayInstallWarning>>> {
    let command_label = render_command(&command);
    let command_warnings = match try_start_with_command(command) {
        Ok(StartAttempt::ProgramNotFound) => {
            info!(
                launcher,
                command = %command_label,
                message = %t!("logs.gateway.launcher_unavailable")
            );
            return Ok(None);
        }
        Ok(StartAttempt::Started { warnings }) => warnings,
        Err(error) => {
            info!(
                launcher,
                command = %command_label,
                error = %format!("{error:#}"),
                message = %t!("logs.gateway.launcher_failed")
            );
            return Ok(None);
        }
    };

    if wait_for_gateway_service(listen_addr, timings).is_ok()
        && is_configured_service_active(service_name)?
    {
        info!(
            launcher,
            command = %command_label,
            message = %t!("logs.gateway.service_started")
        );
        return Ok(Some(command_warnings));
    }

    info!(
        launcher,
        command = %command_label,
        service = %service_name,
        listen_addr = %listen_addr,
        message = %t!("logs.gateway.launcher_did_not_reach_service")
    );
    Ok(None)
}

fn wait_for_gateway_service(listen_addr: &str, timings: &GatewayTimings) -> Result<()> {
    let deadline = Instant::now() + timings.startup_timeout;

    while Instant::now() < deadline {
        if is_gateway_reachable(listen_addr, timings.connect_timeout)? {
            return Ok(());
        }
        thread::sleep(timings.poll_interval);
    }

    bail!(
        "{}",
        t!(
            "errors.gateway.startup_timeout",
            listen_addr = listen_addr,
            startup_timeout_ms = timings.startup_timeout.as_millis()
        )
    )
}

fn make_pioneer_start_command() -> Command {
    let mut command = Command::new(configured_pioneer_command_file_name());
    command.arg("start");
    command.arg("--json");
    apply_desktop_command_env(&mut command);
    command
}

fn make_managed_pioneer_start_command() -> Option<Command> {
    let install = managed_gateway_install()?;
    if !install.binary_path.is_file() {
        return None;
    }

    let mut command = Command::new(install.binary_path);
    command.arg("start");
    command.arg("--json");
    apply_desktop_command_env(&mut command);
    Some(command)
}

fn make_development_pioneer_start_command() -> Option<Command> {
    make_development_pioneer_command("start", true)
}

fn make_pioneer_device_create_command() -> Command {
    let mut command = Command::new(configured_pioneer_command_file_name());
    command.args(["device", "create"]);
    apply_desktop_command_env(&mut command);
    command
}

fn make_managed_pioneer_device_create_command() -> Option<Command> {
    let install = managed_gateway_install()?;
    if !install.binary_path.is_file() {
        return None;
    }

    let mut command = Command::new(install.binary_path);
    command.args(["device", "create"]);
    apply_desktop_command_env(&mut command);
    Some(command)
}

fn make_development_pioneer_device_create_command() -> Option<Command> {
    let mut command = make_development_pioneer_command("device", false)?;
    command.arg("create");
    Some(command)
}

fn make_development_pioneer_command(
    subcommand: &'static str,
    json_output: bool,
) -> Option<Command> {
    if !cfg!(debug_assertions) {
        return None;
    }

    let workspace_root = development_workspace_root()?;
    let mut command = Command::new("cargo");
    command.arg("run");
    command.arg("--quiet");
    command.arg("-p");
    command.arg("pioneer-cli");
    command.arg("--features");
    command.arg("dev");
    command.arg("--bin");
    command.arg("pioneer-dev");
    command.arg("--");
    command.arg(subcommand);
    if json_output {
        command.arg("--json");
    }
    command.current_dir(workspace_root);
    apply_desktop_command_env(&mut command);
    Some(command)
}

fn development_workspace_root() -> Option<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    path.join("Cargo.toml").is_file().then_some(path)
}

fn make_bundled_gateway_install_command(
    subcommand: &'static str,
    force_start: bool,
) -> Option<Command> {
    let bundle = bundled_gateway_bootstrap()?;
    Some(make_bundled_gateway_install_command_from_bundle(
        &bundle,
        subcommand,
        force_start,
    ))
}

fn make_bundled_gateway_install_command_from_bundle(
    bundle: &BundledGatewayBootstrap,
    subcommand: &'static str,
    force_start: bool,
) -> Command {
    let mut command = Command::new(bundle.bootstrap_binary_path.as_os_str());
    command.arg(subcommand);
    command.arg("--source");
    command.arg("local");
    command.arg("--asset");
    command.arg(bundle.asset_path.as_os_str());
    command.arg("--checksums");
    command.arg(bundle.checksums_path.as_os_str());
    command.arg("--managed-by");
    command.arg(DESKTOP_MANAGED_BY);
    command.arg("--json");
    if force_start {
        command.arg("--force-start");
    }

    apply_desktop_command_env(&mut command);
    command
}

fn bundled_gateway_bootstrap() -> Option<BundledGatewayBootstrap> {
    let current_exe = std::env::current_exe().ok()?;
    let bootstrap_dir = bundled_bootstrap_dir_for_executable(current_exe.as_path())?;

    let bootstrap_binary_path = bootstrap_dir.join(bundled_gateway_bootstrap_binary_file_name());
    let asset_path = bootstrap_dir.join(bundled_gateway_asset_file_name()?);
    let checksums_path = bootstrap_dir.join("SHA256SUMS");

    if !bootstrap_binary_path.is_file() || !asset_path.is_file() || !checksums_path.is_file() {
        return None;
    }

    Some(BundledGatewayBootstrap {
        bootstrap_binary_path,
        asset_path,
        checksums_path,
    })
}

fn bundled_bootstrap_dir_for_executable(current_exe: &Path) -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let macos_dir = current_exe.parent()?;
        let contents_dir = macos_dir.parent()?;
        return Some(contents_dir.join("Resources").join("gateway"));
    }

    #[cfg(not(target_os = "macos"))]
    {
        Some(current_exe.parent()?.join("gateway"))
    }
}

fn bundled_gateway_bootstrap_binary_file_name() -> &'static str {
    if cfg!(windows) {
        "pioneer-bootstrap.exe"
    } else {
        "pioneer-bootstrap"
    }
}

fn bundled_gateway_asset_file_name() -> Option<String> {
    let arch = gateway_arch_label();

    #[cfg(target_os = "windows")]
    {
        return Some(format!("pioneer-gateway-windows-{arch}.zip"));
    }

    #[cfg(target_os = "macos")]
    {
        let extension = if arch == "x86_64" { "zip" } else { "gz" };
        return Some(format!("pioneer-gateway-macos-{arch}.{extension}"));
    }

    #[cfg(target_os = "linux")]
    {
        return Some(format!("pioneer-gateway-linux-{arch}.gz"));
    }

    #[allow(unreachable_code)]
    None
}

fn gateway_arch_label() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" | "amd64" => "x86_64",
        "aarch64" | "arm64" => "aarch64",
        other => other,
    }
}

fn apply_desktop_command_env(command: &mut Command) {
    command.env("PIONEER_MANAGED_BY", DESKTOP_MANAGED_BY);
    if let Some(config_path) = desktop_command_config_path() {
        command.env("PIONEER_CONFIG", config_path);
    }
}

fn ensure_desktop_command_config_is_safe() -> Result<()> {
    let Some(config_path) = desktop_command_config_path() else {
        return Ok(());
    };
    if !config_path.is_file() {
        bail!(
            "refusing to launch a Pioneer child process because the explicit config does not exist: {}",
            config_path.display()
        );
    }
    Ok(())
}

fn desktop_command_config_path() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("PIONEER_CONFIG") {
        let explicit = PathBuf::from(explicit);
        return Some(
            std::env::current_dir()
                .map(|current_dir| absolutize_config_path(explicit.as_path(), &current_dir))
                .unwrap_or(explicit),
        );
    }
    cfg!(debug_assertions).then(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("config")
            .join("local.toml")
    })
}

fn absolutize_config_path(path: &Path, current_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_dir.join(path)
    }
}

fn configured_pioneer_command_file_name() -> String {
    AppConfig::load()
        .ok()
        .and_then(|config| config.install_command_file_name().ok())
        .unwrap_or_else(default_pioneer_command_file_name)
}

fn default_pioneer_command_file_name() -> String {
    if cfg!(windows) {
        "pioneer.exe".to_owned()
    } else {
        "pioneer".to_owned()
    }
}

fn try_start_with_command(mut command: Command) -> Result<StartAttempt> {
    let command_label = render_command(&command);

    let output = match command.output() {
        Ok(output) => output,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(StartAttempt::ProgramNotFound);
        }
        Err(error) => {
            return Err(error).with_context(|| {
                t!(
                    "errors.command.run_failed",
                    command_label = command_label.as_str()
                )
                .to_string()
            });
        }
    };

    if output.status.success() {
        return Ok(StartAttempt::Started {
            warnings: extract_install_warnings(&output),
        });
    }

    bail!(
        "{}",
        t!(
            "errors.command.failed_with_output",
            command_label = command_label.as_str(),
            details = output_details(&output)
        )
    )
}

fn extract_install_warnings(output: &Output) -> Vec<GatewayInstallWarning> {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    parse_install_warnings_json(stdout.as_str())
}

fn parse_install_warnings_json(stdout: &str) -> Vec<GatewayInstallWarning> {
    if stdout.trim().is_empty() {
        return Vec::new();
    }

    let Ok(parsed) = serde_json::from_str::<InstallCommandOutput>(stdout) else {
        return Vec::new();
    };

    parsed
        .warnings
        .into_iter()
        .map(|warning| match warning {
            InstallWarningEntry::Structured(warning) => GatewayInstallWarning {
                code: warning.code.trim().to_owned(),
                message: warning.message.trim().to_owned(),
            },
            InstallWarningEntry::Message(message) => GatewayInstallWarning {
                code: String::new(),
                message: message.trim().to_owned(),
            },
        })
        .filter(|warning| !warning.code.is_empty() || !warning.message.is_empty())
        .collect()
}

fn try_create_pending_device_session_with_command(
    mut command: Command,
) -> Result<DeviceCreateAttempt> {
    let command_label = render_command(&command);

    let output = match command.output() {
        Ok(output) => output,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(DeviceCreateAttempt::ProgramNotFound);
        }
        Err(error) => {
            return Err(error).with_context(|| {
                t!(
                    "errors.command.run_failed",
                    command_label = command_label.as_str()
                )
                .to_string()
            });
        }
    };

    if !output.status.success() {
        // The command creates a pending device session and returns its one-time
        // activation code on stdout. Never attach either output stream to this
        // error: a partially successful or faulty launcher must not make
        // credential material observable through logs.
        bail!(
            "device activation command failed with exit status {}",
            output.status
        );
    }

    let stdout = Zeroizing::new(
        String::from_utf8(output.stdout)
            .context("failed to parse device activation command output as UTF-8")?,
    );

    let activation_code = Zeroizing::new(
        stdout
            .lines()
            .rev()
            .find_map(|line| {
                let trimmed = line.trim();
                (!trimmed.is_empty()).then_some(trimmed.to_owned())
            })
            .ok_or_else(|| anyhow::anyhow!("device activation command returned empty output"))?,
    );
    normalize_device_activation_code(activation_code.as_str())
        .map_err(|_| anyhow::anyhow!("device activation command returned an invalid credential"))?;

    Ok(DeviceCreateAttempt::Code(activation_code))
}

fn try_create_pending_device_session_with_launcher(
    launcher: &str,
    command: Command,
) -> Result<Option<Zeroizing<String>>> {
    let command_label = render_command(&command);

    match try_create_pending_device_session_with_command(command) {
        Ok(DeviceCreateAttempt::ProgramNotFound) => {
            info!(
                launcher,
                command = %command_label,
                message = "pending device session launcher unavailable"
            );
            Ok(None)
        }
        Ok(DeviceCreateAttempt::Code(activation_code)) => {
            info!(
                launcher,
                command = %command_label,
                message = "pending device session created"
            );
            Ok(Some(activation_code))
        }
        Err(error) => {
            info!(
                launcher,
                command = %command_label,
                error = %format!("{error:#}"),
                message = "device activation launcher failed"
            );
            Ok(None)
        }
    }
}

fn render_command(command: &Command) -> String {
    let program = command.get_program().to_string_lossy();
    let args = command
        .get_args()
        .map(os_to_lossy)
        .collect::<Vec<_>>()
        .join(" ");

    if args.is_empty() {
        program.into_owned()
    } else {
        format!("{program} {args}")
    }
}

fn os_to_lossy(value: &OsStr) -> String {
    value.to_string_lossy().into_owned()
}

fn output_details(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();

    if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        t!("errors.command.exit_status", status = output.status).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BundledGatewayBootstrap, DESKTOP_MANAGED_BY, DeviceCreateAttempt, absolutize_config_path,
        desktop_command_config_path, make_bundled_gateway_install_command_from_bundle,
        make_pioneer_device_create_command, make_pioneer_start_command,
        parse_install_warnings_json, try_create_pending_device_session_with_command,
    };
    use std::ffi::OsStr;
    use std::path::PathBuf;

    #[test]
    fn relative_explicit_config_is_pinned_before_a_child_changes_directory() {
        assert_eq!(
            absolutize_config_path(
                std::path::Path::new("config/local.toml"),
                std::path::Path::new("/tmp/pioneer-desktop"),
            ),
            PathBuf::from("/tmp/pioneer-desktop/config/local.toml")
        );
    }

    #[test]
    fn start_commands_use_desktop_managed_context() {
        let pioneer_command = make_pioneer_start_command();
        assert_eq!(command_args(&pioneer_command), vec!["start", "--json"]);
        assert_eq!(
            command_env(&pioneer_command, OsStr::new("PIONEER_MANAGED_BY")),
            Some(DESKTOP_MANAGED_BY.to_owned())
        );
    }

    #[test]
    fn device_create_command_uses_desktop_managed_context() {
        let command = make_pioneer_device_create_command();
        assert_eq!(command_args(&command), vec!["device", "create"]);
        assert_eq!(
            command_env(&command, OsStr::new("PIONEER_MANAGED_BY")),
            Some(DESKTOP_MANAGED_BY.to_owned())
        );
        if cfg!(debug_assertions) {
            assert_eq!(
                command_env(&command, OsStr::new("PIONEER_CONFIG")),
                desktop_command_config_path().map(|path| path.to_string_lossy().into_owned())
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn device_create_launcher_reads_only_the_last_non_empty_stdout_line() {
        let activation_code = "K7M4-P9Q2";
        let script = format!("printf 'launcher preface\\n\\n{activation_code}\\n'");
        let mut command = std::process::Command::new("sh");
        command.args(["-c", script.as_str()]);

        let result = try_create_pending_device_session_with_command(command)
            .expect("run activation fixture command");
        let DeviceCreateAttempt::Code(code) = result else {
            panic!("fixture command must return an activation code")
        };
        assert_eq!(code.as_str(), activation_code);
    }

    #[cfg(unix)]
    #[test]
    fn device_create_launcher_rejects_malformed_success_output_without_echoing_it() {
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "printf 'not-an-activation-secret\\n'"]);

        let error = match try_create_pending_device_session_with_command(command) {
            Ok(_) => panic!("malformed activation output must fail"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(rendered.contains("invalid credential"));
        assert!(!rendered.contains("not-an-activation-secret"));
    }

    #[cfg(unix)]
    #[test]
    fn failed_device_create_launcher_never_exposes_process_output() {
        let mut command = std::process::Command::new("sh");
        command.args([
            "-c",
            "printf 'secret-stdout\\n'; printf 'secret-stderr\\n' >&2; exit 7",
        ]);

        let error = match try_create_pending_device_session_with_command(command) {
            Ok(_) => panic!("fixture command must fail"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(!rendered.contains("secret-stdout"));
        assert!(!rendered.contains("secret-stderr"));
        assert!(rendered.contains("exit status"));
    }

    #[test]
    fn bundled_installer_command_uses_assets_and_desktop_context() {
        let bundle = BundledGatewayBootstrap {
            bootstrap_binary_path: PathBuf::from("/tmp/pioneer-bootstrap"),
            asset_path: PathBuf::from("/tmp/pioneer-gateway-linux-x86_64.gz"),
            checksums_path: PathBuf::from("/tmp/SHA256SUMS"),
        };
        let expected_asset_path = bundle.asset_path.to_string_lossy().into_owned();
        let expected_checksums_path = bundle.checksums_path.to_string_lossy().into_owned();
        let command = make_bundled_gateway_install_command_from_bundle(&bundle, "install", true);

        assert_eq!(
            command_env(&command, OsStr::new("PIONEER_MANAGED_BY")),
            Some(DESKTOP_MANAGED_BY.to_owned())
        );

        let args = command_args(&command).join(" ");
        assert!(args.contains("install"));
        assert!(args.contains("--json"));
        assert!(args.contains(expected_asset_path.as_str()));
        assert!(args.contains(expected_checksums_path.as_str()));
    }

    #[test]
    fn parses_structured_install_warnings_from_json() {
        let warnings = parse_install_warnings_json(
            r#"{
                "phase":"installed",
                "warnings":[
                    {"code":"path_update_skipped","message":"profile update failed"},
                    {"code":"other_warning","message":"other message"}
                ]
            }"#,
        );
        assert_eq!(warnings.len(), 2);
        assert_eq!(warnings[0].code, "path_update_skipped");
        assert_eq!(warnings[0].message, "profile update failed");
    }

    fn command_args(command: &std::process::Command) -> Vec<String> {
        command
            .get_args()
            .map(|value| value.to_string_lossy().into_owned())
            .collect()
    }

    fn command_env(command: &std::process::Command, key: &OsStr) -> Option<String> {
        command.get_envs().find_map(|(env_key, env_value)| {
            if env_key == key {
                env_value.map(|value| value.to_string_lossy().into_owned())
            } else {
                None
            }
        })
    }
}
