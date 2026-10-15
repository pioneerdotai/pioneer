use anyhow::{Context, Result, bail};
use std::env;
use std::process::{Command, Output};
use tracing::info;

use pioneer_config::InstallManagedBy;

use super::{GatewayServiceStartReport, GatewayServiceWarning, SERVICE_MODE_ARG, ServiceSettings};

const WINDOWS_LOGON_TASK_SCOPE_WARNING_CODE: &str = "windows_logon_task_login_session_scoped";

pub fn start_gateway_service(
    settings: &ServiceSettings,
    report: &mut GatewayServiceStartReport,
) -> Result<Vec<GatewayServiceWarning>> {
    let task_name = settings.service_name.as_str();
    let script = report.observe("service.definition.prepare", || {
        let task_name_escaped = powershell_escape_single_quoted(task_name);
        let executable =
            env::current_exe().context("failed to determine current executable path")?;
        let executable = powershell_escape_single_quoted(&executable.display().to_string());
        let service_path = super::resolve_service_path()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        let runner_script = render_gateway_runner_script(&executable, service_path.as_deref());
        let runner_script = powershell_escape_single_quoted(runner_script.as_str());

        Ok(format!(
            r#"$runner = '{runner_script}'
        $encoded = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($runner))
        $actionArgs = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand $encoded"
        $user = [Security.Principal.WindowsIdentity]::GetCurrent().Name
        $action = New-ScheduledTaskAction -Execute 'powershell' -Argument $actionArgs
        $trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
        $settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -StartWhenAvailable
        Register-ScheduledTask -TaskName '{task_name_escaped}' -Action $action -Trigger $trigger -Settings $settings -Description 'Persistent Pioneer gateway service' -User $user -RunLevel Limited -Force | Out-Null
        Start-ScheduledTask -TaskName '{task_name_escaped}'"#
        ))
    })?;

    let output = report.observe("service.manager.register_activate", || {
        run_powershell(&script)
    })?;
    if !output.status.success() {
        bail!(
            "failed to create/start Windows logon task `{task_name}`: {}",
            output_details(&output)
        );
    }

    info!(
        service = task_name,
        "gateway scheduled task is running and will auto-start at user logon"
    );
    Ok(logon_task_scope_warnings(settings))
}

pub fn stop_gateway_service(settings: &ServiceSettings) -> Result<()> {
    let task_name = settings.service_name.as_str();
    let task_name_escaped = powershell_escape_single_quoted(task_name);
    let script = format!(
        r#"$task = Get-ScheduledTask -TaskName '{task_name_escaped}' -ErrorAction SilentlyContinue
        if ($null -eq $task) {{
            Write-Output 'not_found'
            exit 0
        }}
        Stop-ScheduledTask -TaskName '{task_name_escaped}' -ErrorAction SilentlyContinue | Out-Null
        Unregister-ScheduledTask -TaskName '{task_name_escaped}' -Confirm:$false | Out-Null
        Write-Output 'stopped'"#
    );

    let output = run_powershell(&script)?;
    if !output.status.success() {
        bail!(
            "failed to stop/remove Windows logon task `{task_name}`: {}",
            output_details(&output)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    if stdout.contains("stopped") {
        info!(service = task_name, "gateway service is stopped");
    } else {
        info!(service = task_name, "gateway service is not running");
    }

    Ok(())
}

pub fn is_gateway_service_active(settings: &ServiceSettings) -> Result<bool> {
    let task_name = settings.service_name.as_str();
    let task_name_escaped = powershell_escape_single_quoted(task_name);
    let script = format!(
        r#"$task = Get-ScheduledTask -TaskName '{task_name_escaped}' -ErrorAction SilentlyContinue
        if ($null -eq $task) {{
            exit 1
        }}
        if ($task.State -eq 'Running') {{
            exit 0
        }}
        exit 2"#
    );

    let output = run_powershell(&script)?;
    Ok(output.status.success())
}

fn run_powershell(script: &str) -> Result<Output> {
    Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ])
        .output()
        .context("failed to run powershell")
}

fn powershell_escape_single_quoted(input: &str) -> String {
    input.replace('\'', "''")
}

fn render_gateway_runner_script(executable: &str, path_env: Option<&str>) -> String {
    match path_env {
        Some(path_env) => {
            let path = powershell_escape_single_quoted(path_env);
            format!("$env:PATH = '{path}'; & '{executable}' '{SERVICE_MODE_ARG}'")
        }
        None => format!("& '{executable}' '{SERVICE_MODE_ARG}'"),
    }
}

fn logon_task_scope_warnings(settings: &ServiceSettings) -> Vec<GatewayServiceWarning> {
    if matches!(settings.managed_by, InstallManagedBy::Desktop) {
        return Vec::new();
    }

    vec![GatewayServiceWarning::new(
        WINDOWS_LOGON_TASK_SCOPE_WARNING_CODE,
        "Windows gateway installs as a current-user Scheduled Task triggered at logon. It starts after the user logs in and is not available as a boot-time Windows Service before login.",
    )]
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
    use super::{WINDOWS_LOGON_TASK_SCOPE_WARNING_CODE, logon_task_scope_warnings};
    use crate::service::ServiceSettings;
    use pioneer_config::InstallManagedBy;
    use std::path::PathBuf;

    #[test]
    fn logon_task_scope_warning_is_suppressed_for_desktop_managed_installs() {
        let warnings = logon_task_scope_warnings(&test_settings(InstallManagedBy::Desktop));
        assert!(warnings.is_empty());
    }

    #[test]
    fn logon_task_scope_warning_is_reported_for_script_installs() {
        let warnings = logon_task_scope_warnings(&test_settings(InstallManagedBy::Script));
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, WINDOWS_LOGON_TASK_SCOPE_WARNING_CODE);
        assert!(warnings[0].message.contains("Scheduled Task"));
    }

    fn test_settings(managed_by: InstallManagedBy) -> ServiceSettings {
        ServiceSettings {
            service_name: "com.pioneer.gateway.test".to_owned(),
            legacy_service_names: Vec::new(),
            runtime_home_dir: PathBuf::from("C:\\Users\\Test\\Pioneer"),
            macos_background_item_name: "Pioneer".to_owned(),
            macos_associated_bundle_identifier: "ai.pioneer.test".to_owned(),
            managed_by,
        }
    }
}
