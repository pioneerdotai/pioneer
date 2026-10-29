use anyhow::{Context, Result};
use std::process::Command;

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
    let uid = pioneer_observability::resources::captured_output(
        Command::new("id").arg("-u"),
        pioneer_observability::resources::ChildRole::PlatformHelper,
    )
    .context(t!("errors.command.launchctl_print_failed").to_string())?;
    anyhow::ensure!(
        uid.status.success(),
        "{}",
        t!("errors.command.launchctl_print_failed")
    );
    let uid: u32 = std::str::from_utf8(&uid.stdout)?.trim().parse()?;
    let domain = format!("gui/{uid}");
    let output = pioneer_observability::resources::captured_output(
        Command::new("launchctl").args(["print", &domain]),
        pioneer_observability::resources::ChildRole::PlatformHelper,
    )
    .context(t!("errors.command.launchctl_print_failed").to_string())?;
    anyhow::ensure!(
        output.status.success(),
        "{}",
        t!("errors.command.launchctl_print_failed")
    );
    macos_gui_service_active(std::str::from_utf8(&output.stdout)?, &domain, service_name)
        .context(t!("errors.command.launchctl_print_failed").to_string())
}

#[cfg(target_os = "macos")]
fn macos_gui_service_active(text: &str, domain: &str, service_name: &str) -> Result<bool> {
    // Same strict native-domain table boundary as CLI: unknown/truncated print
    // output is an error, never evidence of an inactive/absent Gateway.
    let mut lines = text.trim().lines();
    anyhow::ensure!(
        lines
            .next()
            .is_some_and(|line| line.trim() == format!("{domain} = {{"))
            && text.trim_end().ends_with('}'),
        "unrecognized launchctl GUI domain output"
    );
    let mut table = false;
    let mut found_table = false;
    let mut closed_table = false;
    let mut activity = None;
    let mut depth = 1usize;
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        anyhow::ensure!(depth > 0, "trailing launchctl domain output");
        if line == "}" {
            if table && depth == 2 {
                table = false;
                closed_table = true;
            }
            depth -= 1;
            continue;
        }
        if line.ends_with(" = {") {
            anyhow::ensure!(!table, "nested launchctl service row");
            if depth == 1 && line == "services = {" {
                anyhow::ensure!(!found_table, "ambiguous launchctl services table");
                found_table = true;
                table = true;
            }
            depth += 1;
            continue;
        }
        if !table {
            continue;
        }
        let (pid, rest) = line
            .split_once(char::is_whitespace)
            .context("invalid launchctl service row")?;
        let (status, name) = rest
            .trim_start()
            .split_once(char::is_whitespace)
            .context("invalid launchctl service row")?;
        anyhow::ensure!(
            !status.is_empty() && !name.trim().is_empty(),
            "invalid launchctl service row"
        );
        let pid = if pid == "-" {
            0
        } else {
            pid.parse::<u32>()
                .context("invalid launchctl service PID")?
        };
        if name.trim() == service_name {
            anyhow::ensure!(
                activity.is_none(),
                "duplicate launchctl Gateway registration"
            );
            activity = Some(pid > 0);
        }
    }
    anyhow::ensure!(
        found_table && closed_table && !table && depth == 0,
        "incomplete launchctl GUI services table"
    );
    Ok(activity.unwrap_or(false))
}

#[cfg(target_os = "linux")]
fn is_service_active_linux(service_name: &str) -> Result<bool> {
    let service_unit = service_unit_name_linux(service_name);
    let output = pioneer_observability::resources::captured_output(
        Command::new("systemctl").args(["--user", "is-active", service_unit.as_str()]),
        pioneer_observability::resources::ChildRole::PlatformHelper,
    )
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
    .context(t!("errors.command.powershell_task_status_failed").to_string())?;

    Ok(output.status.success())
}
