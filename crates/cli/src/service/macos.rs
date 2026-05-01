use anyhow::{Context, Result, bail};
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tracing::info;

use super::{SERVICE_MODE_ARG, ServiceSettings};

pub fn start_gateway_service(settings: &ServiceSettings) -> Result<()> {
    let service_label = settings.service_name.as_str();
    let plist_path = launch_agents_dir()?.join(format!("{service_label}.plist"));
    let executable = env::current_exe().context("failed to determine current executable path")?;
    let logs_dir = user_logs_dir()
        .context("failed to resolve user logs directory for launch agent")?
        .join("Logs")
        .join("Pioneer")
        .join(service_label);

    fs::create_dir_all(
        plist_path
            .parent()
            .context("failed to get launchd directory")?,
    )
    .context("failed to create launchd directory")?;
    fs::create_dir_all(&logs_dir).context("failed to create service logs directory")?;

    let launchd_path = super::resolve_service_path();
    let plist_content = render_launchd_plist(
        service_label,
        &executable,
        &logs_dir,
        launchd_path.as_deref(),
    );
    fs::write(&plist_path, plist_content).with_context(|| {
        format!(
            "failed to write launchd service file at {}",
            plist_path.display()
        )
    })?;

    let domain = launchctl_domain()?;
    let service_target = format!("{domain}/{service_label}");
    let plist_path_str = path_to_str(&plist_path)?;

    let _ = run_launchctl(&["bootout", domain.as_str(), plist_path_str.as_ref()]);
    run_launchctl(&["enable", &service_target])?;
    run_launchctl(&["bootstrap", domain.as_str(), plist_path_str.as_ref()])?;
    run_launchctl(&["kickstart", "-k", &service_target])?;

    info!(
        service = service_label,
        "gateway service is running and will auto-start after reboot"
    );
    Ok(())
}

pub fn stop_gateway_service(settings: &ServiceSettings) -> Result<()> {
    let service_label = settings.service_name.as_str();
    let domain = launchctl_domain()?;
    let plist_path = launch_agents_dir()?.join(format!("{service_label}.plist"));
    let service_target = format!("{domain}/{service_label}");

    let mut stopped = false;
    if plist_path.exists() {
        let plist_path_str = path_to_str(&plist_path)?;
        stopped |= run_launchctl(&["bootout", domain.as_str(), plist_path_str.as_ref()]).is_ok();
    }

    stopped |= run_launchctl(&["bootout", &service_target]).is_ok();
    let _ = run_launchctl(&["disable", &service_target]);

    if plist_path.exists() {
        fs::remove_file(&plist_path).with_context(|| {
            format!(
                "failed to remove launchd service file at {}",
                plist_path.display()
            )
        })?;
    }

    if stopped {
        info!(service = service_label, "gateway service is stopped");
    } else {
        info!(service = service_label, "gateway service is not running");
    }

    Ok(())
}

pub fn is_gateway_service_active(settings: &ServiceSettings) -> Result<bool> {
    let service_label = settings.service_name.as_str();
    let target = format!("{}/{}", launchctl_domain()?, service_label);

    let output = Command::new("launchctl")
        .args(["print", target.as_str()])
        .output()
        .with_context(|| format!("failed to run launchctl print {target}"))?;

    Ok(output.status.success())
}

fn render_launchd_plist(
    service_label: &str,
    executable: &Path,
    logs_dir: &Path,
    path_env: Option<&str>,
) -> String {
    let service_label = xml_escape(service_label);
    let executable = xml_escape(&executable.display().to_string());
    let stdout_log = xml_escape(&logs_dir.join("gateway.out.log").display().to_string());
    let stderr_log = xml_escape(&logs_dir.join("gateway.err.log").display().to_string());
    let environment_section = path_env
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let escaped_path = xml_escape(value);
            format!(
                "<key>EnvironmentVariables</key>
                <dict>
                <key>PATH</key>
                <string>{escaped_path}</string>
                </dict>"
            )
        })
        .unwrap_or_default();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
        <plist version="1.0">
        <dict>
            <key>Label</key>
            <string>{service_label}</string>
            <key>ProgramArguments</key>
            <array>
            <string>{executable}</string>
            <string>{SERVICE_MODE_ARG}</string>
            </array>
            <key>RunAtLoad</key>
            <true/>
            <key>KeepAlive</key>
            <true/>
            <key>StandardOutPath</key>
            <string>{stdout_log}</string>
            <key>StandardErrorPath</key>
            <string>{stderr_log}</string>
            {environment_section}
        </dict>
        </plist>
        "#
    )
}

fn xml_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn current_uid() -> Result<String> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .context("failed to run `id -u`")?;

    if !output.status.success() {
        bail!("`id -u` failed: {}", output_details(&output));
    }

    let uid = String::from_utf8(output.stdout)
        .context("failed to parse `id -u` output as UTF-8")?
        .trim()
        .to_owned();

    if uid.is_empty() {
        bail!("`id -u` returned an empty uid");
    }

    Ok(uid)
}

fn launchctl_domain() -> Result<String> {
    Ok(format!("gui/{}", current_uid()?))
}

fn launch_agents_dir() -> Result<PathBuf> {
    let home =
        dirs::home_dir().context("failed to resolve current user home directory for launchd")?;
    Ok(home.join("Library").join("LaunchAgents"))
}

fn user_logs_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|home| home.join("Library"))
}

fn path_to_str(path: &Path) -> Result<&str> {
    path.as_os_str()
        .to_str()
        .with_context(|| format!("path is not valid UTF-8: {}", path.display()))
}

fn run_launchctl<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args_vec: Vec<String> = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string_lossy().into_owned())
        .collect();

    let output = Command::new("launchctl")
        .args(&args_vec)
        .output()
        .with_context(|| format!("failed to run launchctl {}", args_vec.join(" ")))?;

    if output.status.success() {
        return Ok(());
    }

    bail!(
        "launchctl {} failed: {}",
        args_vec.join(" "),
        output_details(&output)
    )
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

#[cfg(test)]
mod tests {
    use super::render_launchd_plist;
    use std::path::Path;

    #[test]
    fn launchd_plist_includes_path_environment_when_provided() {
        let plist = render_launchd_plist(
            "com.pioneer.gateway.test",
            Path::new("/tmp/pioneer"),
            Path::new("/tmp/logs"),
            Some("/opt/homebrew/bin:/usr/bin:/bin"),
        );

        assert!(plist.contains("<key>EnvironmentVariables</key>"));
        assert!(plist.contains("<key>PATH</key>"));
        assert!(plist.contains("/opt/homebrew/bin:/usr/bin:/bin"));
    }
}
