use anyhow::{Context, Result, bail};
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
        let executable = super::current_executable_path()?;
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
        $principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
        $settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -StartWhenAvailable
        Register-ScheduledTask -TaskName '{task_name_escaped}' -Action $action -Trigger $trigger -Settings $settings -Description 'Persistent Pioneer gateway service' -Principal $principal -Force | Out-Null
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
    let query = root_task_query(&settings.service_name);
    let script = format!(
        r#"$ErrorActionPreference = 'Stop'
        $task = {query}
        if ($task.Count -eq 0) {{ exit 0 }}
        Stop-ScheduledTask -InputObject $task[0] -ErrorAction Stop | Out-Null
        $deadline = [DateTime]::UtcNow.AddSeconds(10)
        do {{
            $task = {query}
            if ($task.Count -eq 0) {{ exit 0 }}
            if ($task[0].State -in @('Ready', 'Disabled')) {{ break }}
            if ([DateTime]::UtcNow -ge $deadline) {{ throw 'Gateway task has not stopped' }}
            Start-Sleep -Milliseconds 100
        }} while ($true)
        Unregister-ScheduledTask -InputObject $task[0] -Confirm:$false -ErrorAction Stop | Out-Null"#
    );
    let output = run_powershell(&script)?;
    anyhow::ensure!(
        output.status.success(),
        "failed to stop/remove Windows Gateway task: {}",
        output_details(&output)
    );
    info!(
        service = settings.service_name.as_str(),
        "gateway service is stopped"
    );
    Ok(())
}

pub fn is_gateway_service_active(settings: &ServiceSettings) -> Result<bool> {
    let query = root_task_query(&settings.service_name);
    let output = run_powershell(&format!(
        r#"$ErrorActionPreference = 'Stop'
        [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
        $task = {query}
        if ($task.Count -eq 0) {{ Write-Output 'false' }} else {{
            if ($task[0].State -notin @('Running', 'Queued', 'Ready', 'Disabled')) {{ throw 'Unknown Gateway task state' }}
            ($task[0].State -eq 'Running') | ConvertTo-Json -Compress
        }}"#
    ))?;
    anyhow::ensure!(
        output.status.success(),
        "failed to inspect Windows Gateway task: {}",
        output_details(&output)
    );
    serde_json::from_slice(&output.stdout).context("invalid Gateway task activity")
}

fn root_task_query(name: &str) -> String {
    let name = powershell_escape_single_quoted(name);
    // Wildcard enumeration has an empty-result contract; retain a strict root
    // path/name filter so no same-named task in a subfolder can be captured.
    format!(
        r"@(Get-ScheduledTask -TaskPath '\*' -ErrorAction Stop | Where-Object {{ $_.TaskPath -eq '\' -and $_.TaskName -eq '{name}' }})"
    )
}

pub(super) struct RegistrationSnapshot {
    xml: Option<String>,
}

impl RegistrationSnapshot {
    pub(super) fn is_registered(&self) -> bool {
        self.xml.is_some()
    }
}

pub(super) fn capture_registration(settings: &ServiceSettings) -> Result<RegistrationSnapshot> {
    let name = powershell_escape_single_quoted(&settings.service_name);
    let query = root_task_query(&settings.service_name);
    let script = format!(
        r#"$ErrorActionPreference = 'Stop'
        [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
        $task = {query}
        if ($task.Count -eq 0) {{ Write-Output 'null' }} else {{
            Export-ScheduledTask -TaskName '{name}' -TaskPath '\' -ErrorAction Stop | ConvertTo-Json -Compress
        }}"#
    );
    let output = run_powershell(&script)?;
    anyhow::ensure!(
        output.status.success(),
        "failed to snapshot Gateway task: {}",
        output_details(&output)
    );
    let xml = serde_json::from_slice(&output.stdout).context("invalid Gateway task snapshot")?;
    Ok(RegistrationSnapshot { xml })
}

pub(super) fn restore_registration(
    settings: &ServiceSettings,
    snapshot: &RegistrationSnapshot,
) -> Result<()> {
    let Some(xml) = &snapshot.xml else {
        return Ok(());
    };
    let name = powershell_escape_single_quoted(&settings.service_name);
    let xml = powershell_escape_single_quoted(xml);
    let output = run_powershell(&format!(
        r#"$ErrorActionPreference = 'Stop'
        Register-ScheduledTask -TaskName '{name}' -TaskPath '\' -Xml '{xml}' -Force -ErrorAction Stop | Out-Null"#
    ))?;
    anyhow::ensure!(
        output.status.success(),
        "failed to restore Gateway task: {}",
        output_details(&output)
    );
    // Register only; no Start-ScheduledTask for an originally inactive task.
    Ok(())
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
            macos_associated_bundle_identifier: "ai.pioneer.test".to_owned(),
            managed_by,
        }
    }
}
