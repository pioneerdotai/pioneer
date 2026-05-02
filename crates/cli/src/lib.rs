mod installer;
mod service;

use anyhow::{Context, Result, bail};
use pioneer_config::InstallManagedBy;
use serde_json::json;
use std::env;
use tracing::error;

pub fn main_entry() {
    init_tracing();

    if let Err(error) = run() {
        error!(error = %format!("{error:#}"), "pioneer command failed");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args = env::args().skip(1);

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
            service::start_gateway_service()?;
            if json_output {
                print_start_report()
            } else {
                Ok(())
            }
        }
        Some("issue-superuser-token") => {
            ensure_no_extra_args(args)?;
            service::issue_superuser_token()
        }
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
        Some(command) => bail!("unknown command: {command}\n\n{}", help_text()),
    }
}

fn ensure_no_extra_args(mut args: impl Iterator<Item = String>) -> Result<()> {
    if let Some(extra) = args.next() {
        bail!("unexpected argument: {extra}");
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
        Some(flag) => bail!("unexpected argument: {flag}; expected `--json`"),
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
                let value = args
                    .next()
                    .context("`--source` requires a value: local|release|channel")?;
                source_kind = Some(value.trim().to_ascii_lowercase());
            }
            "--asset" => {
                let value = args
                    .next()
                    .context("`--asset` requires a file path value")?;
                asset_path = Some(std::path::PathBuf::from(value));
            }
            "--checksums" => {
                let value = args
                    .next()
                    .context("`--checksums` requires a file path value")?;
                checksums_path = Some(std::path::PathBuf::from(value));
            }
            "--channel" => {
                let value = args
                    .next()
                    .context("`--channel` requires a value: stable|beta|canary")?;
                channel = parse_release_channel(value.as_str())?;
                channel_explicit = true;
            }
            "--version" => {
                let value = args.next().context("`--version` requires a value")?;
                version = Some(value);
            }
            "--managed-by" => {
                let value = args
                    .next()
                    .context("`--managed-by` requires a value: script|desktop|manual|unknown")?;
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
            flag => bail!(
                "unexpected argument for install/update: {flag}\n\n{}",
                help_text()
            ),
        }
    }

    let source = match source_kind.as_deref() {
        Some("local") => {
            let asset_path =
                asset_path.context("missing required `--asset <path>` for --source local")?;
            let checksums_path = checksums_path
                .context("missing required `--checksums <path>` for --source local")?;
            installer::InstallSourceSpec::Local {
                asset_path,
                checksums_path,
            }
        }
        Some("release") => {
            if asset_path.is_some() || checksums_path.is_some() {
                bail!("`--asset/--checksums` cannot be used with `--source release`");
            }
            installer::InstallSourceSpec::Release { channel, version }
        }
        Some("channel") => {
            if asset_path.is_some() || checksums_path.is_some() {
                bail!("`--asset/--checksums` cannot be used with `--source channel`");
            }
            if !channel_explicit {
                bail!("`--source channel` requires explicit `--channel stable|beta|canary`");
            }
            if version.is_some() {
                bail!("`--version` cannot be used with `--source channel`");
            }
            installer::InstallSourceSpec::Release {
                channel,
                version: None,
            }
        }
        Some(other) => bail!("invalid value for --source: {other}; expected local|release|channel"),
        None => {
            if asset_path.is_some() || checksums_path.is_some() {
                let asset_path = asset_path.context("missing required `--asset <path>`")?;
                let checksums_path =
                    checksums_path.context("missing required `--checksums <path>`")?;
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
        _ => bail!(
            "invalid value for --channel: {}; expected stable|beta|canary",
            value
        ),
    }
}

fn parse_managed_by_flag(value: &str) -> Result<InstallManagedBy> {
    match value.trim().to_ascii_lowercase().as_str() {
        "script" => Ok(InstallManagedBy::Script),
        "desktop" => Ok(InstallManagedBy::Desktop),
        "manual" => Ok(InstallManagedBy::Manual),
        "unknown" => Ok(InstallManagedBy::Unknown),
        _ => bail!(
            "invalid value for --managed-by: {}; expected script|desktop|manual|unknown",
            value
        ),
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
        "error": error_text,
    });
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn install_error_code(command: installer::InstallCommand, error_text: &str) -> &'static str {
    if error_text.contains("checksum mismatch") {
        return "checksum_mismatch";
    }
    if error_text.contains("health check") {
        return "health_check_failed";
    }
    if error_text.contains("persist install-state") {
        return "install_state_persist_failed";
    }
    if error_text.contains("Permission denied") || error_text.contains("Access is denied") {
        return "insufficient_privileges";
    }

    match command {
        installer::InstallCommand::Install => "install_failed",
        installer::InstallCommand::Update => "update_failed",
    }
}

fn print_start_report() -> Result<()> {
    let status = service::gateway_service_status()?;
    let payload = json!({
        "phase": "started",
        "installed_version": env!("CARGO_PKG_VERSION"),
        "service_active": status.service_active,
        "gateway_reachable": status.gateway_reachable,
        "rollback_performed": false,
        "error_code": serde_json::Value::Null,
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
  {command} issue-superuser-token  Generate a superuser JWT and print it
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

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .without_time()
        .try_init();
}
