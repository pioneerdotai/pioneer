mod installer;
mod secrets;
mod service;
mod task_invariants;

use anyhow::Result;
use pioneer_config::InstallManagedBy;
use serde_json::json;
use std::env;
use std::ffi::OsString;
use std::fmt;
use tracing::warn;

pub fn main_entry() {
    // This must remain the first dispatch in the signed binary. The hidden
    // stdio helper cannot initialize tracing, Sentry, Gateway, or ordinary CLI
    // parsing because stdout is exclusively the provider MCP transport.
    if let Some(exit_code) = dispatch_hidden_cli_mcp_stdio(env::args_os().skip(1)) {
        if exit_code != 0 {
            std::process::exit(exit_code);
        }
        return;
    }

    // The service loads its persisted telemetry preference inside the Gateway.
    // Keep external reporting closed until then so an existing opt-out also
    // covers the service bootstrap performed by this shared CLI binary.
    if env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new(service::SERVICE_MODE_ARG)) {
        pioneer_observability::set_telemetry_enabled(false);
    }

    let sentry_guard =
        pioneer_observability::init_sentry(pioneer_observability::SentryTarget::Shared);
    pioneer_observability::init_tracing(sentry_guard.is_some());

    if let Err(error) = run() {
        if is_usage_error(&error) {
            eprintln!("{error:#}");
        } else if installer::is_transient_download_error(&error) {
            warn!(
                error = %format!("{error:#}"),
                "pioneer command failed after transient installer network failure"
            );
        } else {
            warn!(error = %format!("{error:#}"), "pioneer command failed");
            pioneer_observability::capture_anyhow(&error);
        }
        drop(sentry_guard);
        std::process::exit(1);
    }
}

fn dispatch_hidden_cli_mcp_stdio(args: impl Iterator<Item = OsString>) -> Option<i32> {
    let bootstrap_path = match pioneer_cli_mcp_bridge::helper::parse_hidden_helper_args(args) {
        Ok(None) => return None,
        Ok(Some(path)) => path,
        Err(error) => {
            eprintln!(
                "{}",
                pioneer_cli_mcp_bridge::helper::bounded_diagnostic(&error)
            );
            return Some(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("CLI MCP helper runtime initialization failed");
            return Some(1);
        }
    };
    match runtime.block_on(pioneer_cli_mcp_bridge::helper::run_hidden_helper(
        &bootstrap_path,
    )) {
        Ok(()) => Some(0),
        Err(error) => {
            eprintln!(
                "{}",
                pioneer_cli_mcp_bridge::helper::bounded_diagnostic(&error)
            );
            Some(1)
        }
    }
}

#[derive(Debug)]
pub(crate) struct CliUsageError {
    message: String,
}

impl fmt::Display for CliUsageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for CliUsageError {}

pub(crate) fn usage_error(message: impl Into<String>) -> anyhow::Error {
    CliUsageError {
        message: message.into(),
    }
    .into()
}

fn is_usage_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<CliUsageError>().is_some()
}

fn run() -> Result<()> {
    run_with_args(env::args().skip(1))
}

fn run_with_args(mut args: impl Iterator<Item = String>) -> Result<()> {
    match args.next().as_deref() {
        Some("install") => {
            let (options, json_output) =
                parse_install_options(installer::InstallCommand::Install, args)?;
            match installer::run_install(options.clone()) {
                Ok(report) => print_install_report(report, json_output),
                Err(error) => {
                    if json_output {
                        print_install_failure_json(options.command, &error)?;
                    }
                    Err(error)
                }
            }
        }
        Some("update") | Some("self-update") => {
            let (options, json_output) =
                parse_install_options(installer::InstallCommand::Update, args)?;
            match installer::run_install(options.clone()) {
                Ok(report) => print_install_report(report, json_output),
                Err(error) => {
                    if json_output {
                        print_install_failure_json(options.command, &error)?;
                    }
                    Err(error)
                }
            }
        }
        Some("start") => {
            let json_output = parse_optional_json_flag(args)?;
            let report = service::start_gateway_service()?;
            if json_output {
                print_start_report(report)
            } else {
                Ok(())
            }
        }
        Some("device") => match args.next().as_deref() {
            Some("create") => {
                let json_output = parse_optional_json_flag(args)?;
                let device = service::create_device()?;
                if json_output {
                    println!("{}", serde_json::to_string_pretty(&device)?);
                } else {
                    println!("{}", device.activation_code.expose_secret());
                }
                Ok(())
            }
            Some(command) => Err(usage_error(format!(
                "unknown device command: {command}; expected `create`"
            ))),
            None => Err(usage_error("missing device command; expected `create`")),
        },
        Some("task-invariants") => task_invariants::run(args),
        Some("secrets") => secrets::run(args),
        Some("status") => {
            let json_output = parse_optional_json_flag(args)?;
            print_status(json_output)
        }
        Some("version") | Some("--version") | Some("-V") => {
            let json_output = parse_optional_json_flag(args)?;
            print_version(json_output)
        }
        Some("stop") => {
            ensure_no_extra_args(args)?;
            service::stop_gateway_service()
        }
        Some(service::SERVICE_MODE_ARG) => {
            ensure_no_extra_args(args)?;
            service::run_gateway_service()
        }
        Some("help") | Some("--help") | Some("-h") | None => {
            print_help();
            Ok(())
        }
        Some(command) => Err(usage_error(format!(
            "unknown command: {command}\n\n{}",
            help_text()
        ))),
    }
}

fn ensure_no_extra_args(mut args: impl Iterator<Item = String>) -> Result<()> {
    if let Some(extra) = args.next() {
        return Err(usage_error(format!("unexpected argument: {extra}")));
    }
    Ok(())
}

fn parse_optional_json_flag(mut args: impl Iterator<Item = String>) -> Result<bool> {
    match args.next().as_deref() {
        None => Ok(false),
        Some("--json") => {
            ensure_no_extra_args(args)?;
            Ok(true)
        }
        Some(flag) => Err(usage_error(format!(
            "unexpected argument: {flag}; expected `--json`"
        ))),
    }
}

fn parse_install_options(
    command: installer::InstallCommand,
    mut args: impl Iterator<Item = String>,
) -> Result<(installer::InstallOptions, bool)> {
    let mut source_kind: Option<String> = None;
    let mut asset_path = None;
    let mut checksums_path = None;
    let mut channel = installer::ReleaseChannel::Stable;
    let mut channel_explicit = false;
    let mut version = None;
    let mut managed_by = InstallManagedBy::Manual;
    let mut no_start = false;
    let mut force_start = false;
    let mut json_output = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source" => {
                let value = args.next().ok_or_else(|| {
                    usage_error("`--source` requires a value: local|release|channel")
                })?;
                source_kind = Some(value.trim().to_ascii_lowercase());
            }
            "--asset" => {
                let value = args
                    .next()
                    .ok_or_else(|| usage_error("`--asset` requires a file path value"))?;
                asset_path = Some(std::path::PathBuf::from(value));
            }
            "--checksums" => {
                let value = args
                    .next()
                    .ok_or_else(|| usage_error("`--checksums` requires a file path value"))?;
                checksums_path = Some(std::path::PathBuf::from(value));
            }
            "--channel" => {
                let value = args.next().ok_or_else(|| {
                    usage_error("`--channel` requires a value: stable|beta|canary")
                })?;
                channel = parse_release_channel(value.as_str())?;
                channel_explicit = true;
            }
            "--version" => {
                let value = args
                    .next()
                    .ok_or_else(|| usage_error("`--version` requires a value"))?;
                version = Some(value);
            }
            "--managed-by" => {
                let value = args.next().ok_or_else(|| {
                    usage_error("`--managed-by` requires a value: script|desktop|manual|unknown")
                })?;
                managed_by = parse_managed_by_flag(value.as_str())?;
            }
            "--no-start" => {
                no_start = true;
            }
            "--force-start" => {
                force_start = true;
            }
            "--json" => {
                json_output = true;
            }
            flag => {
                return Err(usage_error(format!(
                    "unexpected argument for install/update: {flag}\n\n{}",
                    help_text()
                )));
            }
        }
    }

    let source = match source_kind.as_deref() {
        Some("local") => {
            let asset_path = asset_path.ok_or_else(|| {
                usage_error("missing required `--asset <path>` for --source local")
            })?;
            let checksums_path = checksums_path.ok_or_else(|| {
                usage_error("missing required `--checksums <path>` for --source local")
            })?;
            installer::InstallSourceSpec::Local {
                asset_path,
                checksums_path,
            }
        }
        Some("release") => {
            if asset_path.is_some() || checksums_path.is_some() {
                return Err(usage_error(
                    "`--asset/--checksums` cannot be used with `--source release`",
                ));
            }
            installer::InstallSourceSpec::Release { channel, version }
        }
        Some("channel") => {
            if asset_path.is_some() || checksums_path.is_some() {
                return Err(usage_error(
                    "`--asset/--checksums` cannot be used with `--source channel`",
                ));
            }
            if !channel_explicit {
                return Err(usage_error(
                    "`--source channel` requires explicit `--channel stable|beta|canary`",
                ));
            }
            if version.is_some() {
                return Err(usage_error(
                    "`--version` cannot be used with `--source channel`",
                ));
            }
            installer::InstallSourceSpec::Release {
                channel,
                version: None,
            }
        }
        Some(other) => {
            return Err(usage_error(format!(
                "invalid value for --source: {other}; expected local|release|channel"
            )));
        }
        None => {
            if asset_path.is_some() || checksums_path.is_some() {
                let asset_path =
                    asset_path.ok_or_else(|| usage_error("missing required `--asset <path>`"))?;
                let checksums_path = checksums_path
                    .ok_or_else(|| usage_error("missing required `--checksums <path>`"))?;
                installer::InstallSourceSpec::Local {
                    asset_path,
                    checksums_path,
                }
            } else {
                installer::InstallSourceSpec::Release { channel, version }
            }
        }
    };

    Ok((
        installer::InstallOptions {
            command,
            source,
            managed_by,
            no_start,
            force_start,
        },
        json_output,
    ))
}

fn parse_release_channel(value: &str) -> Result<installer::ReleaseChannel> {
    match value.trim().to_ascii_lowercase().as_str() {
        "stable" => Ok(installer::ReleaseChannel::Stable),
        "beta" => Ok(installer::ReleaseChannel::Beta),
        "canary" => Ok(installer::ReleaseChannel::Canary),
        _ => Err(usage_error(format!(
            "invalid value for --channel: {}; expected stable|beta|canary",
            value
        ))),
    }
}

fn parse_managed_by_flag(value: &str) -> Result<InstallManagedBy> {
    match value.trim().to_ascii_lowercase().as_str() {
        "script" => Ok(InstallManagedBy::Script),
        "desktop" => Ok(InstallManagedBy::Desktop),
        "manual" => Ok(InstallManagedBy::Manual),
        "unknown" => Ok(InstallManagedBy::Unknown),
        _ => Err(usage_error(format!(
            "invalid value for --managed-by: {}; expected script|desktop|manual|unknown",
            value
        ))),
    }
}

fn print_install_report(report: installer::InstallReport, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    println!("Phase: {}", report.phase);
    println!("Command: {}", report.command);
    println!("Install root: {}", report.install_root);
    println!("Installed binary: {}", report.installed_binary);
    println!("Installed version: {}", report.installed_version);
    println!("Service active before install: {}", report.was_active);
    println!("Service started after install: {}", report.started);
    println!("Command link created: {}", report.command_link_created);
    println!("PATH updated: {}", report.path_updated);
    println!("Service active now: {}", report.service_active);
    println!("Gateway reachable now: {}", report.gateway_reachable);
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in report.warnings {
            println!("- [{}] {}", warning.code, warning.message);
        }
    }
    Ok(())
}

fn print_install_failure_json(
    command: installer::InstallCommand,
    error: &anyhow::Error,
) -> Result<()> {
    let error_text = format!("{error:#}");
    let rolled_back = error_text.contains("rolled back");
    let error_code = install_error_code(command, error_text.as_str());
    let payload = json!({
        "phase": "failed",
        "command": match command {
            installer::InstallCommand::Install => "install",
            installer::InstallCommand::Update => "update",
        },
        "installed_version": env!("CARGO_PKG_VERSION"),
        "service_active": false,
        "gateway_reachable": false,
        "command_link_created": false,
        "path_updated": false,
        "rollback_performed": rolled_back,
        "error_code": error_code,
        "warnings": [],
        "stage_timings": [],
        "error": error_text,
    });
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn install_error_code(command: installer::InstallCommand, error_text: &str) -> &'static str {
    if error_text.contains("transient network failure") {
        return "download_transient_network_failure";
    }
    if error_text.contains("checksum mismatch") {
        return "checksum_mismatch";
    }
    if error_text.contains("health check") {
        return "health_check_failed";
    }
    if error_text.contains("persist install-state") {
        return "install_state_persist_failed";
    }
    if error_text.contains("systemd linger") {
        return "linux_linger_required";
    }
    if error_text.contains("Permission denied") || error_text.contains("Access is denied") {
        return "insufficient_privileges";
    }

    match command {
        installer::InstallCommand::Install => "install_failed",
        installer::InstallCommand::Update => "update_failed",
    }
}

fn print_start_report(mut report: service::GatewayServiceStartReport) -> Result<()> {
    let status = report.observe("service.status.probe", service::gateway_service_status)?;
    let payload = json!({
        "phase": "started",
        "installed_version": env!("CARGO_PKG_VERSION"),
        "service_active": status.service_active,
        "gateway_reachable": status.gateway_reachable,
        "rollback_performed": false,
        "error_code": serde_json::Value::Null,
        "warnings": report.warnings,
        "stage_timings": report.stage_timings,
    });
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn print_version(json_output: bool) -> Result<()> {
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "name": binary_display_name(),
                "version": env!("CARGO_PKG_VERSION"),
                "os": std::env::consts::OS,
                "arch": std::env::consts::ARCH
            }))?
        );
    } else {
        println!("{} {}", binary_display_name(), env!("CARGO_PKG_VERSION"));
    }

    Ok(())
}

fn print_status(json_output: bool) -> Result<()> {
    let status = service::gateway_service_status()?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&status)?);
        return Ok(());
    }

    println!("Service name: {}", status.service_name);
    println!("Listen address: {}", status.listen_addr);
    println!("Service active: {}", status.service_active);
    println!("Gateway reachable: {}", status.gateway_reachable);
    println!("Runtime home: {}", status.runtime_home);

    if let Some(install_state) = status.install_state {
        println!("Install state version: {}", install_state.version);
        println!(
            "Managed by: {}",
            managed_by_label(&install_state.managed_by)
        );
        println!("Installed version: {}", install_state.installed_version);
        println!("Binary path: {}", install_state.binary_path.display());
        if let Some(root) = install_state.install_root {
            println!("Install root: {}", root.display());
        }
        println!(
            "Install state updated_at_unix: {}",
            install_state.updated_at_unix
        );
    } else {
        println!("Install state: not found");
    }

    Ok(())
}

fn managed_by_label(value: &InstallManagedBy) -> &'static str {
    match value {
        InstallManagedBy::Script => "script",
        InstallManagedBy::Desktop => "desktop",
        InstallManagedBy::Manual => "manual",
        InstallManagedBy::Unknown => "unknown",
    }
}

fn print_help() {
    println!("{}", help_text());
}

fn help_text() -> String {
    let command = binary_display_name();
    format!(
        "Usage:
  {command} install [--source local|release|channel] [--asset <path> --checksums <path>] [--channel stable|beta|canary] [--version x.y.z]
                  [--managed-by desktop|script|manual|unknown] [--no-start] [--force-start] [--json]
  {command} update [--source local|release|channel] [--asset <path> --checksums <path>] [--channel stable|beta|canary] [--version x.y.z]
                 [--managed-by desktop|script|manual|unknown] [--no-start] [--force-start] [--json]
  {command} self-update (alias of `update`)
  {command} start [--json]      Install and start the persistent gateway service
  {command} status [--json]     Show gateway service status
  {command} device create [--json]  Create a pending device session and one-time activation code
  {command} task-invariants --db <path> [--json] [--stale-turn-after-seconds <seconds>]
                                Scan task/subagent runtime invariants in a SQLite gateway DB
  {command} secrets status [--json]  Show keystore status without secret values
  {command} secrets garbage-collection [--dry-run] [--json]  Clean orphan MCP secret values
  {command} stop                Stop and uninstall the persistent gateway service
  {command} version [--json]    Show {command} version
  {command} help                Show this help"
    )
}

fn binary_display_name() -> String {
    env::args_os()
        .next()
        .and_then(|arg| {
            let path = std::path::PathBuf::from(arg);
            path.file_stem()
                .or_else(|| path.file_name())
                .map(|value| value.to_string_lossy().into_owned())
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "pioneer".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args<'a>(values: &'a [&'a str]) -> impl Iterator<Item = String> + 'a {
        values.iter().map(|value| (*value).to_owned())
    }

    #[test]
    fn hidden_cli_mcp_stdio_is_classified_before_ordinary_cli() {
        let parsed = pioneer_cli_mcp_bridge::helper::parse_hidden_helper_args(
            [
                OsString::from("__cli-mcp-stdio"),
                OsString::from("--bootstrap-file"),
                OsString::from("/private/bootstrap"),
            ]
            .into_iter(),
        )
        .expect("hidden helper args");
        assert_eq!(parsed, Some(std::path::PathBuf::from("/private/bootstrap")));
        assert!(
            pioneer_cli_mcp_bridge::helper::parse_hidden_helper_args(
                [OsString::from("status")].into_iter()
            )
            .expect("ordinary args")
            .is_none()
        );
    }

    #[test]
    fn unknown_command_is_usage_error() {
        for command in ["/load", "/install", "-install"] {
            let error = run_with_args(args(&[command])).expect_err("unknown command should fail");

            assert!(is_usage_error(&error), "{command} should be a usage error");
            assert!(format!("{error:#}").contains(format!("unknown command: {command}").as_str()));
        }
    }

    #[test]
    fn unexpected_argument_is_usage_error() {
        let error = parse_optional_json_flag(args(&["--verbose"]))
            .expect_err("unexpected flag should fail");

        assert!(is_usage_error(&error));
        assert!(format!("{error:#}").contains("expected `--json`"));
    }

    #[test]
    fn invalid_install_option_is_usage_error() {
        let error = parse_install_options(
            installer::InstallCommand::Install,
            args(&["--channel", "nightly"]),
        )
        .expect_err("invalid channel should fail");

        assert!(is_usage_error(&error));
        assert!(format!("{error:#}").contains("invalid value for --channel"));
    }

    #[test]
    fn subcommand_parse_failures_are_usage_errors() {
        let error = run_with_args(args(&["secrets", "unknown"]))
            .expect_err("unknown secrets command should fail");
        assert!(is_usage_error(&error));

        let error = run_with_args(args(&["task-invariants", "--json"]))
            .expect_err("missing task-invariants db should fail");
        assert!(is_usage_error(&error));
    }

    #[test]
    fn transient_installer_network_failure_has_stable_error_code() {
        assert_eq!(
            install_error_code(
                installer::InstallCommand::Update,
                "download failed due to a transient network failure",
            ),
            "download_transient_network_failure"
        );
    }
}
