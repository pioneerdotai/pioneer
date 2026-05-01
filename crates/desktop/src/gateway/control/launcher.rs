use crate::gateway::connectivity::is_gateway_reachable;
use crate::gateway::timings::GatewayTimings;
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::ffi::OsStr;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::Instant;
use tracing::info;

use super::install::managed_gateway_install;
use super::status::is_configured_service_active;

const DESKTOP_MANAGED_BY: &str = "desktop";

enum StartAttempt {
    Started {
        warnings: Vec<GatewayInstallWarning>,
    },
    ProgramNotFound,
}

enum TokenAttempt {
    Token(String),
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
            "development cargo pioneer-cli",
            command,
            service_name,
            listen_addr,
            timings,
        )?
    {
        return Ok(warnings);
    }

    if let Some(warnings) = try_start_with_launcher(
        "pioneer in PATH",
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
            "development cargo pioneer-cli",
            command,
            service_name,
            listen_addr,
            timings,
        )?
    {
        return Ok(warnings);
    }

    if let Some(warnings) = try_start_with_launcher(
        "pioneer in PATH",
        make_pioneer_start_command(),
        service_name,
        listen_addr,
        timings,
    )? {
        return Ok(warnings);
    }

    bail!("{}", t!("errors.gateway.start_command_failed_manual_help"))
}

pub(crate) fn request_local_superuser_token() -> Result<String> {
    if let Some(command) = make_managed_pioneer_issue_superuser_token_command()
        && let Some(token) = try_request_token_with_launcher("managed pioneer binary", command)?
    {
        return Ok(token);
    }

    if let Some(command) = make_development_pioneer_issue_superuser_token_command()
        && let Some(token) =
            try_request_token_with_launcher("development cargo pioneer-cli", command)?
    {
        return Ok(token);
    }

    if let Some(token) = try_request_token_with_launcher(
        "pioneer in PATH",
        make_pioneer_issue_superuser_token_command(),
    )? {
        return Ok(token);
    }

    bail!(
        "failed to request superuser token; make sure `pioneer issue-superuser-token` is available"
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
    let mut command = Command::new("pioneer");
    command.arg("start");
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
    apply_desktop_command_env(&mut command);
    Some(command)
}

fn make_development_pioneer_start_command() -> Option<Command> {
    make_development_pioneer_command("start")
}

fn make_pioneer_issue_superuser_token_command() -> Command {
    let mut command = Command::new("pioneer");
    command.arg("issue-superuser-token");
    apply_desktop_command_env(&mut command);
    command
}

fn make_managed_pioneer_issue_superuser_token_command() -> Option<Command> {
    let install = managed_gateway_install()?;
    if !install.binary_path.is_file() {
        return None;
    }

    let mut command = Command::new(install.binary_path);
    command.arg("issue-superuser-token");
    apply_desktop_command_env(&mut command);
    Some(command)
}

fn make_development_pioneer_issue_superuser_token_command() -> Option<Command> {
    make_development_pioneer_command("issue-superuser-token")
}

fn make_development_pioneer_command(subcommand: &'static str) -> Option<Command> {
    if !cfg!(debug_assertions) {
        return None;
    }

    let workspace_root = development_workspace_root()?;
    let mut command = Command::new("cargo");
    command.arg("run");
    command.arg("--quiet");
    command.arg("-p");
    command.arg("pioneer-cli");
    command.arg("--");
    command.arg(subcommand);
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
        return Some(format!("pioneer-gateway-macos-{arch}.gz"));
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

fn try_request_token_with_command(mut command: Command) -> Result<TokenAttempt> {
    let command_label = render_command(&command);

    let output = match command.output() {
        Ok(output) => output,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(TokenAttempt::ProgramNotFound);
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
        bail!(
            "{}",
            t!(
                "errors.command.failed_with_output",
                command_label = command_label.as_str(),
                details = output_details(&output)
            )
        );
    }

    let stdout = String::from_utf8(output.stdout)
        .context("failed to parse token command output as UTF-8")?;

    let token = stdout
        .lines()
        .rev()
        .find_map(|line| {
            let trimmed = line.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_owned())
        })
        .ok_or_else(|| anyhow::anyhow!("token command returned empty output"))?;

    Ok(TokenAttempt::Token(token))
}

fn try_request_token_with_launcher(launcher: &str, command: Command) -> Result<Option<String>> {
    let command_label = render_command(&command);

    match try_request_token_with_command(command) {
        Ok(TokenAttempt::ProgramNotFound) => {
            info!(
                launcher,
                command = %command_label,
                message = "token launcher unavailable"
            );
            Ok(None)
        }
        Ok(TokenAttempt::Token(token)) => {
            info!(
                launcher,
                command = %command_label,
                message = "superuser token received"
            );
            Ok(Some(token))
        }
        Err(error) => {
            info!(
                launcher,
                command = %command_label,
                error = %format!("{error:#}"),
                message = "token launcher failed"
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
        BundledGatewayBootstrap, DESKTOP_MANAGED_BY,
        make_bundled_gateway_install_command_from_bundle, make_pioneer_start_command,
        parse_install_warnings_json,
    };
    use std::ffi::OsStr;
    use std::path::PathBuf;

    #[test]
    fn start_commands_use_desktop_managed_context() {
        let pioneer_command = make_pioneer_start_command();
        assert_eq!(command_args(&pioneer_command), vec!["start"]);
        assert_eq!(
            command_env(&pioneer_command, OsStr::new("PIONEER_MANAGED_BY")),
            Some(DESKTOP_MANAGED_BY.to_owned())
        );
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
