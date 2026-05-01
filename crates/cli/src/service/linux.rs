use anyhow::{Context, Result, bail};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tracing::info;

use super::{SERVICE_MODE_ARG, ServiceSettings};

pub fn start_gateway_service(settings: &ServiceSettings) -> Result<()> {
    let service_unit = service_unit(settings.service_name.as_str());
    let service_path = user_service_path(service_unit.as_str())?;
    let service_dir = service_path
        .parent()
        .context("failed to get systemd service directory")?;
    let executable = env::current_exe().context("failed to determine current executable path")?;

    fs::create_dir_all(service_dir).with_context(|| {
        format!(
            "failed to create systemd service directory at {}",
            service_dir.display()
        )
    })?;

    let service_path_env = super::resolve_service_path();
    let service_content = render_linux_systemd_service(&executable, service_path_env.as_deref());
    fs::write(&service_path, service_content).with_context(|| {
        format!(
            "failed to write systemd service file at {}",
            service_path.display()
        )
    })?;

    run_systemctl_user(&["daemon-reload"])?;
    run_systemctl_user(&["enable", "--now", service_unit.as_str()])?;

    info!(
        service = settings.service_name.as_str(),
        "gateway service is running and will auto-start after reboot"
    );
    Ok(())
}

pub fn stop_gateway_service(settings: &ServiceSettings) -> Result<()> {
    let service_unit = service_unit(settings.service_name.as_str());
    let service_path = user_service_path(service_unit.as_str())?;
    let mut stopped = false;

    stopped |= run_systemctl_user(&["stop", service_unit.as_str()]).is_ok();
    let _ = run_systemctl_user(&["disable", service_unit.as_str()]);

    if service_path.exists() {
        fs::remove_file(&service_path).with_context(|| {
            format!(
                "failed to remove systemd service file at {}",
                service_path.display()
            )
        })?;
        stopped = true;
    }

    let _ = run_systemctl_user(&["daemon-reload"]);
    let _ = run_systemctl_user(&["reset-failed", service_unit.as_str()]);

    if stopped {
        info!(
            service = settings.service_name.as_str(),
            "gateway service is stopped"
        );
    } else {
        info!(
            service = settings.service_name.as_str(),
            "gateway service is not running"
        );
    }

    Ok(())
}

pub fn is_gateway_service_active(settings: &ServiceSettings) -> Result<bool> {
    let service_unit = service_unit(settings.service_name.as_str());
    Ok(run_systemctl_user(&["is-active", service_unit.as_str()]).is_ok())
}

fn render_linux_systemd_service(executable: &Path, path_env: Option<&str>) -> String {
    let exec = systemd_quote(&executable.display().to_string());
    let mode_arg = systemd_quote(SERVICE_MODE_ARG);
    let environment_section = path_env
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let path = value.replace('%', "%%");
            let env = systemd_quote(format!("PATH={path}").as_str());
            format!("Environment={env}")
        })
        .unwrap_or_default();

    format!(
        r#"[Unit]
        Description=Pioneer Gateway Service
        After=network-online.target
        Wants=network-online.target

        [Service]
        Type=simple
        {environment_section}
        ExecStart={exec} {mode_arg}
        Restart=always
        RestartSec=2

        [Install]
        WantedBy=default.target
        "#
    )
}

fn systemd_quote(input: &str) -> String {
    let escaped = input.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

fn run_systemctl_user(args: &[&str]) -> Result<()> {
    let mut full_args = Vec::with_capacity(args.len() + 1);
    full_args.push("--user".to_owned());
    full_args.extend(args.iter().map(|value| (*value).to_owned()));
    run_command_checked("systemctl", &full_args)
}

fn run_command_checked(command: &str, args: &[String]) -> Result<()> {
    let output = Command::new(command)
        .args(args)
        .output()
        .with_context(|| format!("failed to run `{}`", render_command(command, args)))?;

    if output.status.success() {
        return Ok(());
    }

    bail!(
        "`{}` failed: {}",
        render_command(command, args),
        output_details(&output)
    )
}

fn render_command(command: &str, args: &[String]) -> String {
    if args.is_empty() {
        return command.to_owned();
    }
    format!("{command} {}", args.join(" "))
}

fn output_details(output: &Output) -> String {
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

fn service_unit(service_name: &str) -> String {
    if service_name.ends_with(".service") {
        service_name.to_owned()
    } else {
        format!("{service_name}.service")
    }
}

fn user_service_path(service_unit: &str) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(path)
            .join("systemd")
            .join("user")
            .join(service_unit));
    }

    let home =
        dirs::home_dir().context("failed to resolve current user home directory for systemd")?;
    Ok(home
        .join(".config")
        .join("systemd")
        .join("user")
        .join(service_unit))
}
