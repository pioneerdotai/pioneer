use anyhow::{Context, Result, bail};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tracing::info;

use pioneer_config::InstallManagedBy;

use super::{GatewayServiceStartReport, GatewayServiceWarning, SERVICE_MODE_ARG, ServiceSettings};

const LINUX_LINGER_WARNING_CODE: &str = "linux_linger_enable_failed";

pub fn start_gateway_service(
    settings: &ServiceSettings,
    report: &mut GatewayServiceStartReport,
    executable: &Path,
    config_path: Option<&Path>,
) -> Result<Vec<GatewayServiceWarning>> {
    let warnings = report.observe("service.persistence.prepare", || {
        ensure_persistent_user_service(settings)
    })?;
    let service_unit = report.observe("service.definition.prepare", || {
        let service_unit = service_unit(settings.service_name.as_str());
        let service_path = user_service_path(service_unit.as_str())?;
        let service_dir = service_path
            .parent()
            .context("failed to get systemd service directory")?;

        fs::create_dir_all(service_dir).with_context(|| {
            format!(
                "failed to create systemd service directory at {}",
                service_dir.display()
            )
        })?;

        let service_path_env = super::resolve_service_path();
        let service_content =
            render_linux_systemd_service(executable, service_path_env.as_deref(), config_path);
        fs::write(&service_path, service_content).with_context(|| {
            format!(
                "failed to write systemd service file at {}",
                service_path.display()
            )
        })?;
        Ok(service_unit)
    })?;

    report.observe("service.manager.reload", || {
        run_systemctl_user(&["daemon-reload"])
    })?;
    report.observe("service.manager.enable_activate", || {
        run_systemctl_user(&["enable", "--now", service_unit.as_str()])
    })?;

    info!(
        service = settings.service_name.as_str(),
        "gateway service is running and will auto-start after reboot"
    );
    Ok(warnings)
}

pub fn stop_gateway_service(settings: &ServiceSettings) -> Result<()> {
    let service_unit = service_unit(settings.service_name.as_str());
    let service_path = user_service_path(service_unit.as_str())?;
    let active = is_gateway_service_active(settings)?;
    if active || service_path.exists() {
        // A policy/access/manager failure must leave the definition intact and
        // prevent the installer from replacing a still-running payload.
        run_systemctl_user(&["stop", service_unit.as_str()])?;
    }
    anyhow::ensure!(
        !is_gateway_service_active(settings)?,
        "Gateway process has not stopped; installation unchanged"
    );

    if service_path.exists() {
        run_systemctl_user(&["disable", service_unit.as_str()])?;
        fs::remove_file(&service_path).with_context(|| {
            format!(
                "failed to remove systemd service file at {}",
                service_path.display()
            )
        })?;
        run_systemctl_user(&["daemon-reload"])?;
    }
    if active {
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
    let output = Command::new("systemctl")
        .args(["--user", "is-active", service_unit.as_str()])
        .output()
        .context("failed to inspect systemd Gateway activity")?;
    systemd_activity(&output)
}

fn systemd_activity(output: &Output) -> Result<bool> {
    let state = std::str::from_utf8(&output.stdout)?.trim();
    // is-active's documented LSB 3/4 results differ from a manager failure.
    // Transitional states are conservatively treated as requiring a stop.
    match (output.status.code(), state) {
        (Some(0), "active" | "reloading" | "refreshing") => Ok(true),
        (Some(3), "activating" | "deactivating") => Ok(true),
        (Some(3), "inactive" | "failed" | "maintenance" | "unknown")
        | (Some(4), "inactive" | "unknown") => Ok(false),
        _ => bail!(
            "failed to inspect systemd Gateway activity: {}",
            output_details(output)
        ),
    }
}

pub(super) struct RegistrationSnapshot {
    definition: super::ServiceDefinitionBackup,
    enablement: String,
}

impl RegistrationSnapshot {
    pub(super) fn is_registered(&self) -> bool {
        self.definition.is_registered()
    }
}

pub(super) fn capture_registration(settings: &ServiceSettings) -> Result<RegistrationSnapshot> {
    let unit = service_unit(&settings.service_name);
    let definition = super::ServiceDefinitionBackup::capture(user_service_path(&unit)?)?;
    let enablement = if definition.is_registered() {
        let output = Command::new("systemctl")
            .args(["--user", "is-enabled", &unit])
            .output()?;
        let state = String::from_utf8(output.stdout)?.trim().to_owned();
        anyhow::ensure!(
            matches!(state.as_str(), "enabled" | "enabled-runtime" | "disabled"),
            "cannot snapshot Gateway service enablement: {state}"
        );
        state
    } else {
        "disabled".to_owned()
    };
    Ok(RegistrationSnapshot {
        definition,
        enablement,
    })
}

pub(super) fn restore_registration(
    settings: &ServiceSettings,
    snapshot: &RegistrationSnapshot,
) -> Result<()> {
    snapshot.definition.restore()?;
    if !snapshot.is_registered() {
        return Ok(());
    }
    let unit = service_unit(&settings.service_name);
    run_systemctl_user(&["daemon-reload"])?;
    // No --now: an inactive registered service must stay inactive.
    match snapshot.enablement.as_str() {
        "enabled" => run_systemctl_user(&["enable", &unit]),
        "enabled-runtime" => {
            // A prior release's start may have enabled the unit persistently.
            // Remove that policy before restoring the captured runtime-only one.
            run_systemctl_user(&["disable", &unit])?;
            run_systemctl_user(&["enable", "--runtime", &unit])
        }
        _ => run_systemctl_user(&["disable", &unit]),
    }
}

fn ensure_persistent_user_service(
    settings: &ServiceSettings,
) -> Result<Vec<GatewayServiceWarning>> {
    let user = match current_user_name() {
        Ok(user) if !user.is_empty() => user,
        Ok(_) => bail!("failed to resolve current user: `id -un` returned an empty user name"),
        Err(error) => {
            return Err(error).context("failed to resolve current user for Linux service");
        }
    };

    match is_linger_enabled(user.as_str()) {
        Ok(true) => return Ok(Vec::new()),
        Ok(false) => {}
        Err(error) => {
            return handle_persistence_setup_failure(
                settings,
                user.as_str(),
                format!("failed to inspect systemd linger state: {error:#}"),
            );
        }
    }

    match enable_linger(user.as_str()) {
        Ok(()) => {
            if is_linger_enabled(user.as_str()).unwrap_or(true) {
                info!(
                    user = user.as_str(),
                    "enabled systemd linger for gateway service"
                );
                return Ok(Vec::new());
            }
            handle_persistence_setup_failure(
                settings,
                user.as_str(),
                "`loginctl enable-linger` completed but Linger is still not enabled".to_owned(),
            )
        }
        Err(error) => handle_persistence_setup_failure(
            settings,
            user.as_str(),
            format!("`loginctl enable-linger` failed: {error:#}"),
        ),
    }
}

fn current_user_name() -> Result<String> {
    let args = vec!["-un".to_owned()];
    let output = Command::new("id")
        .args(args.as_slice())
        .output()
        .with_context(|| format!("failed to run `{}`", render_command("id", args.as_slice())))?;

    if !output.status.success() {
        bail!(
            "`{}` failed: {}",
            render_command("id", args.as_slice()),
            output_details(&output)
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn is_linger_enabled(user: &str) -> Result<bool> {
    let args = vec![
        "show-user".to_owned(),
        user.to_owned(),
        "-p".to_owned(),
        "Linger".to_owned(),
    ];
    let output = Command::new("loginctl")
        .args(args.as_slice())
        .output()
        .with_context(|| {
            format!(
                "failed to run `{}`",
                render_command("loginctl", args.as_slice())
            )
        })?;

    if !output.status.success() {
        bail!(
            "`{}` failed: {}",
            render_command("loginctl", args.as_slice()),
            output_details(&output)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_linger_enabled(stdout.as_ref()).with_context(|| {
        format!(
            "unexpected output from `{}`: {}",
            render_command("loginctl", args.as_slice()),
            stdout.trim()
        )
    })
}

fn parse_linger_enabled(output: &str) -> Option<bool> {
    for line in output.lines() {
        let value = line.trim();
        match value {
            "Linger=yes" | "yes" => return Some(true),
            "Linger=no" | "no" => return Some(false),
            _ => {}
        }
    }
    None
}

fn enable_linger(user: &str) -> Result<()> {
    let args = vec!["enable-linger".to_owned(), user.to_owned()];
    run_command_checked("loginctl", args.as_slice())
}

fn handle_persistence_setup_failure(
    settings: &ServiceSettings,
    user: &str,
    detail: String,
) -> Result<Vec<GatewayServiceWarning>> {
    let message = format!(
        "Linux user-level gateway services require systemd linger to survive logout and start after reboot. Run `sudo loginctl enable-linger {user}` on this machine, then run `pioneer start` again. Detail: {detail}"
    );

    if matches!(settings.managed_by, InstallManagedBy::Desktop) {
        return Ok(vec![GatewayServiceWarning::new(
            LINUX_LINGER_WARNING_CODE,
            message,
        )]);
    }

    bail!("{message}")
}

fn render_linux_systemd_service(
    executable: &Path,
    path_env: Option<&str>,
    config_path: Option<&Path>,
) -> String {
    let exec = systemd_quote(&executable.display().to_string().replace('%', "%%"));
    let mode_arg = systemd_quote(SERVICE_MODE_ARG);
    let mut environment_section = path_env
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let path = value.replace('%', "%%");
            let env = systemd_quote(format!("PATH={path}").as_str());
            format!("Environment={env}")
        })
        .unwrap_or_default();
    if let Some(config_path) = config_path {
        let config_env =
            systemd_quote(&format!("PIONEER_CONFIG={}", config_path.display()).replace('%', "%%"));
        environment_section.push_str(&format!("\nEnvironment={config_env}"));
    }

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

#[cfg(test)]
mod tests {
    #[test]
    fn service_definition_pins_source_paths_and_explicit_config() {
        let unit = super::render_linux_systemd_service(
            std::path::Path::new("/checkout % one/debug/pioneer"),
            None,
            Some(std::path::Path::new("/checkout % one/dev.toml")),
        );
        assert!(unit.contains("ExecStart=\"/checkout %% one/debug/pioneer\" \"gateway-service\""));
        assert!(unit.contains("Environment=\"PIONEER_CONFIG=/checkout %% one/dev.toml\""));
    }

    #[test]
    #[cfg(unix)]
    fn stop_refusal_or_manager_failure_preserves_the_definition_and_payload() {
        use std::os::unix::fs::PermissionsExt;
        use std::{fs, process::Command};
        const FIXTURE: &str = "PIONEER_TEST_STOP_REFUSAL_ROOT";
        if let Some(root) = std::env::var_os(FIXTURE) {
            let root = std::path::PathBuf::from(root);
            let settings = crate::service::ServiceSettings {
                service_name: "pioneer-stop-test".to_owned(),
                legacy_service_names: vec![],
                macos_associated_bundle_identifier: String::new(),
                managed_by: pioneer_config::InstallManagedBy::Manual,
            };
            let error = super::stop_gateway_service(&settings).unwrap_err();
            assert!(format!("{error:#}").contains("refused by fixture provider"));
            assert_eq!(
                fs::read(root.join("systemd/user/pioneer-stop-test.service")).unwrap(),
                b"[Unit]\nRefuseManualStop=yes\n[Service]\nExecStart=pioneer gateway-service\n"
            );
            assert_eq!(fs::read(root.join("pioneer")).unwrap(), b"prior executable");
            assert_eq!(
                fs::read(root.join("cua/manifest.json")).unwrap(),
                b"prior manifest"
            );
            return;
        }
        // A child test process isolates PATH/XDG_CONFIG_HOME from other tests.
        // Only this operation-owned fake provider can run, never host systemctl.
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::create_dir_all(root.join("systemd/user")).unwrap();
        fs::create_dir(root.join("cua")).unwrap();
        fs::create_dir(root.join("commands")).unwrap();
        fs::write(
            root.join("systemd/user/pioneer-stop-test.service"),
            b"[Unit]\nRefuseManualStop=yes\n[Service]\nExecStart=pioneer gateway-service\n",
        )
        .unwrap();
        fs::write(root.join("pioneer"), b"prior executable").unwrap();
        fs::write(root.join("cua/manifest.json"), b"prior manifest").unwrap();
        let provider = root.join("commands/systemctl");
        fs::write(&provider, "#!/bin/sh\nif [ \"$2\" = is-active ] && [ \"$PIONEER_TEST_MANAGER_FAIL\" != yes ]; then\n  printf 'active\\n'; exit 0\nfi\nprintf 'refused by fixture provider\\n' >&2\nexit 1\n").unwrap();
        fs::set_permissions(&provider, fs::Permissions::from_mode(0o755)).unwrap();
        let name = format!(
            "{}::stop_refusal_or_manager_failure_preserves_the_definition_and_payload",
            module_path!().split_once("::").unwrap().1
        );
        for fail_query in ["no", "yes"] {
            let status = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &name])
                .env(FIXTURE, root)
                .env("XDG_CONFIG_HOME", root)
                .env("PATH", root.join("commands"))
                .env("PIONEER_TEST_MANAGER_FAIL", fail_query)
                .status()
                .unwrap();
            assert!(status.success(), "isolated stop-refusal regression failed");
        }
    }

    use super::parse_linger_enabled;

    #[test]
    fn linger_output_maps_to_enabled_state() {
        for (output, expected) in [
            ("Linger=yes\n", Some(true)),
            ("Linger=no\n", Some(false)),
            ("yes\n", Some(true)),
            ("no\n", Some(false)),
            ("Name=oskin\nState=active\n", None),
        ] {
            assert_eq!(parse_linger_enabled(output), expected);
        }
    }
}
