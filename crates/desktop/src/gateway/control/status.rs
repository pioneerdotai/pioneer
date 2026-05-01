use anyhow::{Context, Result};
use std::process::{Command, Output};

pub(crate) fn is_configured_service_active(service_name: &str) -> Result<bool> {
    #[cfg(target_os = "macos")]
    {
        return is_service_active_macos(service_name);
    }

    #[cfg(target_os = "linux")]
    {
        return is_service_active_linux(service_name);
    }

    #[cfg(target_os = "windows")]
    {
        return is_service_active_windows(service_name);
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let _ = service_name;
        Ok(false)
    }
}

#[cfg(target_os = "macos")]
fn is_service_active_macos(service_name: &str) -> Result<bool> {
    let uid = current_uid_macos()?;
    let target = format!("gui/{uid}/{service_name}");
    let output = Command::new("launchctl")
        .args(["print", &target])
        .output()
        .context(t!("errors.command.launchctl_print_failed").to_string())?;

    Ok(output.status.success())
}

#[cfg(target_os = "macos")]
fn current_uid_macos() -> Result<String> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .context(t!("errors.command.launchctl_print_failed").to_string())?;
    if !output.status.success() {
        return Ok("0".to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[cfg(target_os = "linux")]
fn is_service_active_linux(service_name: &str) -> Result<bool> {
    let service_unit = service_unit_name_linux(service_name);
    let output = Command::new("systemctl")
        .args(["--user", "is-active", service_unit.as_str()])
        .output()
        .context(t!("errors.command.systemctl_is_active_failed").to_string())?;

    Ok(output.status.success())
}

#[cfg(target_os = "linux")]
fn service_unit_name_linux(service_name: &str) -> String {
    if service_name.ends_with(".service") {
        service_name.to_owned()
    } else {
        format!("{service_name}.service")
    }
}

#[cfg(target_os = "windows")]
fn is_service_active_windows(service_name: &str) -> Result<bool> {
    let task_name = service_name.replace('\'', "''");
    let script = format!(
        r#"$task = Get-ScheduledTask -TaskName '{task_name}' -ErrorAction SilentlyContinue
        if ($null -eq $task) {{
            exit 1
        }}
        if ($task.State -eq 'Running') {{
            exit 0
        }}
        exit 2"#
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
        .context(t!("errors.command.powershell_task_status_failed").to_string())?;

    Ok(output.status.success())
}

#[allow(dead_code)]
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
