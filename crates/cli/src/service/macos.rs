use anyhow::{Context, Result, bail};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tracing::info;

use pioneer_config::InstallManagedBy;

use super::{GatewayServiceStartReport, GatewayServiceWarning, SERVICE_MODE_ARG, ServiceSettings};

const MACOS_LAUNCH_AGENT_SCOPE_WARNING_CODE: &str = "macos_launch_agent_login_session_scoped";
const MACOS_LAUNCH_SERVICES_WARNING_CODE: &str =
    "macos_gateway_launch_services_registration_failed";

pub fn start_gateway_service(
    settings: &ServiceSettings,
    report: &mut GatewayServiceStartReport,
    executable: &Path,
    config_path: Option<&Path>,
) -> Result<Vec<GatewayServiceWarning>> {
    let service_label = settings.service_name.as_str();
    let (plist_path, domain, executable) = report.observe("service.definition.prepare", || {
        let plist_path = launch_agents_dir()?.join(format!("{service_label}.plist"));
        let executable = executable.to_path_buf();
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
            settings.macos_associated_bundle_identifier.as_str(),
            config_path,
        );
        fs::write(&plist_path, plist_content).with_context(|| {
            format!(
                "failed to write launchd service file at {}",
                plist_path.display()
            )
        })?;

        Ok((plist_path, launchctl_domain()?, executable))
    })?;
    let service_target = format!("{domain}/{service_label}");
    let plist_path_string = path_to_str(&plist_path)?.to_owned();
    let mut warnings = launch_agent_scope_warnings(settings);

    // Launch Services supplies the background item's application name. Register
    // the Gateway's own signed bundle before launchd registers that item.
    if let Some(bundle) = gateway_app_bundle(&executable) {
        if let Err(error) = report.observe("service.application.register", || {
            register_gateway_application(bundle)
        }) {
            warnings.push(GatewayServiceWarning::new(
                MACOS_LAUNCH_SERVICES_WARNING_CODE,
                format!("failed to register Pioneer Gateway with Launch Services: {error:#}"),
            ));
        }
    }

    report.observe("service.previous.remove", || {
        remove_legacy_services(domain.as_str(), settings.legacy_service_names.as_slice())?;
        let _ = run_launchctl(&["bootout", domain.as_str(), plist_path_string.as_str()]);
        Ok(())
    })?;
    report.observe("service.manager.enable", || {
        run_launchctl(&["enable", &service_target])
    })?;
    report.observe("service.manager.register", || {
        run_launchctl(&["bootstrap", domain.as_str(), plist_path_string.as_str()])
    })?;
    report.observe("service.manager.activate", || {
        run_launchctl(&["kickstart", "-k", &service_target])
    })?;

    info!(
        service = service_label,
        "gateway launch agent is running and will auto-start at user login"
    );
    Ok(warnings)
}

pub fn stop_gateway_service(settings: &ServiceSettings) -> Result<()> {
    let label = settings.service_name.as_str();
    let domain = launchctl_domain()?;
    let plist_path = launch_agents_dir()?.join(format!("{label}.plist"));
    let target = format!("{domain}/{label}");
    let registered = listed_service_activity(label)?.is_some();
    if registered {
        run_launchctl(&["bootout", &target])?;
        // bootout removes the job, preventing KeepAlive from restarting it.
        // Observe completion before removing files or admitting activation.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while listed_service_activity(label)?.is_some() {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "Gateway launch job has not stopped"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    if registered || plist_path.exists() {
        run_launchctl(&["disable", &target])?;
    }
    if plist_path.exists() {
        fs::remove_file(&plist_path).context("failed to remove Gateway LaunchAgent definition")?;
    }
    info!(service = label, "gateway service is stopped");
    Ok(())
}

pub fn is_gateway_service_active(settings: &ServiceSettings) -> Result<bool> {
    Ok(listed_service_activity(&settings.service_name)?.unwrap_or(false))
}

fn listed_service_activity(label: &str) -> Result<Option<bool>> {
    let domain = launchctl_domain()?;
    // Query the managed GUI domain explicitly. Caller-local list/managername
    // describe a different bootstrap when the same user connects over SSH.
    let output = Command::new("launchctl")
        .args(["print", &domain])
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "failed to inspect Gateway GUI domain: {}",
        output_details(&output)
    );
    gui_domain_service_activity(std::str::from_utf8(&output.stdout)?, &domain, label)
}

fn gui_domain_service_activity(text: &str, domain: &str, label: &str) -> Result<Option<bool>> {
    // launchctl print is native diagnostic output, not a stable serialization
    // API (launchctl(1)). Accept only a complete, identified domain and one
    // complete PID/status/label service table. Format drift fails closed.
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
        if name.trim() == label {
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
    Ok(activity)
}

pub(super) struct RegistrationSnapshot {
    definition: super::ServiceDefinitionBackup,
    disabled: bool,
}

impl RegistrationSnapshot {
    pub(super) fn is_registered(&self) -> bool {
        self.definition.is_registered()
    }
}

pub(super) fn capture_registration(settings: &ServiceSettings) -> Result<RegistrationSnapshot> {
    let definition = super::ServiceDefinitionBackup::capture(
        launch_agents_dir()?.join(format!("{}.plist", settings.service_name)),
    )?;
    let disabled = if definition.is_registered() {
        let output = Command::new("launchctl")
            .args(["print-disabled", &launchctl_domain()?])
            .output()?;
        anyhow::ensure!(
            output.status.success(),
            "failed to snapshot launchd enablement: {}",
            output_details(&output)
        );
        disabled_service_override(std::str::from_utf8(&output.stdout)?, &settings.service_name)
            .context("failed to parse launchd enablement snapshot")?
    } else {
        false
    };
    Ok(RegistrationSnapshot {
        definition,
        disabled,
    })
}

fn disabled_service_override(text: &str, label: &str) -> Result<bool> {
    let mut lines = text.trim().lines();
    anyhow::ensure!(
        lines
            .next()
            .is_some_and(|line| line.trim() == "disabled services = {"),
        "unrecognized launchctl disabled services output"
    );
    let mut closed = false;
    let mut target = None;
    for line in lines.map(str::trim).filter(|line| !line.is_empty()) {
        anyhow::ensure!(!closed, "trailing launchctl disabled services output");
        if line == "}" {
            closed = true;
            continue;
        }
        let (name, value) = line
            .split_once("=>")
            .context("invalid launchctl disabled service row")?;
        let name = name
            .trim()
            .strip_prefix('"')
            .and_then(|name| name.strip_suffix('"'))
            .filter(|name| !name.is_empty() && !name.contains('"'))
            .context("invalid launchctl disabled service label")?;
        // Supported macOS reports words, not the older boolean format. Unknown
        // policy must never be mistaken for permission to enable this service.
        let disabled = match value.trim().strip_suffix(';').unwrap_or(value.trim()) {
            "disabled" => true,
            "enabled" => false,
            _ => bail!("unrecognized launchctl disabled service policy"),
        };
        if name == label {
            anyhow::ensure!(
                target.is_none(),
                "duplicate launchctl disabled service target"
            );
            target = Some(disabled);
        }
    }
    anyhow::ensure!(closed, "incomplete launchctl disabled services output");
    // Only a complete valid table can establish the absence of an override.
    Ok(target.unwrap_or(false))
}

pub(super) fn restore_registration(
    settings: &ServiceSettings,
    snapshot: &RegistrationSnapshot,
) -> Result<()> {
    snapshot.definition.restore()?;
    if !snapshot.is_registered() {
        return Ok(());
    }
    let target = format!("{}/{}", launchctl_domain()?, settings.service_name);
    // Do not bootstrap an inactive RunAtLoad/KeepAlive agent: that would start it.
    run_launchctl(&[
        if snapshot.disabled {
            "disable"
        } else {
            "enable"
        },
        &target,
    ])
}

fn render_launchd_plist(
    service_label: &str,
    executable: &Path,
    logs_dir: &Path,
    path_env: Option<&str>,
    associated_bundle_identifier: &str,
    config_path: Option<&Path>,
) -> String {
    let service_label = xml_escape(service_label);
    let executable = xml_escape(&executable.display().to_string());
    let stdout_log = xml_escape(&logs_dir.join("gateway.out.log").display().to_string());
    let stderr_log = xml_escape(&logs_dir.join("gateway.err.log").display().to_string());
    let associated_bundle_section = if associated_bundle_identifier.trim().is_empty() {
        String::new()
    } else {
        let bundle_identifier = xml_escape(associated_bundle_identifier.trim());
        format!(
            "<key>AssociatedBundleIdentifiers</key>
            <array>
            <string>{bundle_identifier}</string>
            </array>"
        )
    };
    let mut environment_entries = path_env
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let escaped_path = xml_escape(value);
            format!("<key>PATH</key><string>{escaped_path}</string>")
        })
        .unwrap_or_default();
    if let Some(config_path) = config_path {
        environment_entries.push_str(&format!(
            "<key>PIONEER_CONFIG</key><string>{}</string>",
            xml_escape(&config_path.display().to_string())
        ));
    }
    let environment_section = if environment_entries.is_empty() {
        String::new()
    } else {
        format!("<key>EnvironmentVariables</key><dict>{environment_entries}</dict>")
    };

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
            {associated_bundle_section}
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

fn gateway_app_bundle(executable: &Path) -> Option<&Path> {
    if cfg!(feature = "computer-use")
        && executable.ends_with("Pioneer Gateway.app/Contents/MacOS/pioneer")
    {
        executable.ancestors().nth(3)
    } else {
        None
    }
}

fn register_gateway_application(bundle: &Path) -> Result<()> {
    let output = Command::new(
        "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister",
    )
    .arg("-f")
    .arg(bundle)
    .output()
    .context("failed to run Launch Services registration tool")?;
    if !output.status.success() {
        bail!(
            "Launch Services registration failed: {}",
            output_details(&output)
        );
    }
    Ok(())
}

fn remove_legacy_services(domain: &str, legacy_service_names: &[String]) -> Result<()> {
    for service_name in legacy_service_names {
        let plist_path = launch_agents_dir()?.join(format!("{service_name}.plist"));
        let service_target = format!("{domain}/{service_name}");
        if plist_path.exists() {
            let plist_path_str = path_to_str(&plist_path)?;
            let _ = run_launchctl(&["bootout", domain, plist_path_str.as_ref()]);
            fs::remove_file(&plist_path).with_context(|| {
                format!(
                    "failed to remove legacy launchd service file at {}",
                    plist_path.display()
                )
            })?;
        }
        let _ = run_launchctl(&["bootout", service_target.as_str()]);
        let _ = run_launchctl(&["disable", service_target.as_str()]);
        let _ = run_launchctl(&["remove", service_name.as_str()]);
    }
    Ok(())
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

fn launch_agent_scope_warnings(settings: &ServiceSettings) -> Vec<GatewayServiceWarning> {
    if matches!(settings.managed_by, InstallManagedBy::Desktop) {
        return Vec::new();
    }

    vec![GatewayServiceWarning::new(
        MACOS_LAUNCH_AGENT_SCOPE_WARNING_CODE,
        "macOS gateway installs as a per-user LaunchAgent. It starts after the user logs in and is not available as a boot-time LaunchDaemon before login.",
    )]
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
    #[test]
    fn registration_recovery_preserves_autostart_policy_and_rejects_unknown_capture() {
        use std::os::unix::fs::PermissionsExt;
        use std::{fs, process::Command};
        const FIXTURE: &str = "PIONEER_TEST_REGISTRATION_POLICY_ROOT";
        if let Some(root) = std::env::var_os(FIXTURE) {
            let root = std::path::PathBuf::from(root);
            let mut settings = test_settings(InstallManagedBy::Script);
            settings.service_name = "ai.pioneer.gateway".to_owned();
            let definition = root.join("home/Library/LaunchAgents/ai.pioneer.gateway.plist");
            fs::create_dir_all(definition.parent().unwrap()).unwrap();
            let original = render_launchd_plist(
                &settings.service_name,
                &root.join("managed/bin/pioneer"),
                &root.join("logs"),
                None,
                "ai.pioneer.gateway",
                None,
            );
            for (rows, expected) in [
                ("\"ai.pioneer.gateway\" => disabled\n", "disabled\n"),
                ("\"ai.pioneer.gateway\" => enabled\n", "enabled\n"),
                // A similarly named service is not an override for our target.
                ("\"ai.pioneer.gateway.other\" => disabled\n", "enabled\n"),
                ("", "enabled\n"),
            ] {
                fs::write(&definition, &original).unwrap();
                fs::write(root.join("loaded"), b"registered with no process").unwrap();
                fs::write(
                    root.join("policy.txt"),
                    format!("disabled services = {{\n{rows}}}\n"),
                )
                .unwrap();
                let snapshot = super::capture_registration(&settings).unwrap();
                super::stop_gateway_service(&settings).unwrap();
                assert!(!definition.exists());
                assert_eq!(
                    fs::read_to_string(root.join("policy-result")).unwrap(),
                    "disabled\n"
                );
                // Actual filesystem failure before any candidate start. This
                // owner test restores the native snapshot, not run_install.
                let blocked = root.join("blocked-state");
                fs::write(&blocked, b"not a directory").unwrap();
                assert!(fs::write(blocked.join("state.toml"), b"candidate state").is_err());
                fs::write(&definition, b"candidate definition").unwrap();
                super::restore_registration(&settings, &snapshot).unwrap();
                assert_eq!(fs::read(&definition).unwrap(), original.as_bytes());
                assert_eq!(
                    fs::read_to_string(root.join("policy-result")).unwrap(),
                    expected
                );
                assert!(
                    !root.join("loaded").exists(),
                    "passive recovery loaded the job"
                );
                assert!(!super::is_gateway_service_active(&settings).unwrap());
            }

            fs::remove_file(root.join("mutations")).unwrap();
            fs::write(root.join("loaded"), b"registered with no process").unwrap();
            let previous_policy = fs::read(root.join("policy-result")).unwrap();
            for invalid in [
                "disabled services = {\n\"ai.pioneer.gateway\" => unknown\n}\n",
                "disabled services = {\n\"ai.pioneer.gateway\" => disabled\n",
                "unrecognized provider output\n",
                "disabled services = {\n\"ai.pioneer.gateway\" => enabled\n}\ntrailing\n",
                "disabled services = {\n\"ai.pioneer.gateway\" => enabled\n\"ai.pioneer.gateway\" => disabled\n}\n",
            ] {
                fs::write(root.join("policy.txt"), invalid).unwrap();
                assert!(super::capture_registration(&settings).is_err());
                assert_eq!(fs::read(&definition).unwrap(), original.as_bytes());
                assert_eq!(
                    fs::read(root.join("policy-result")).unwrap(),
                    previous_policy
                );
                assert!(root.join("loaded").exists());
                assert!(!root.join("mutations").exists());
            }
            fs::write(root.join("provider-error"), b"unavailable").unwrap();
            let error = super::capture_registration(&settings)
                .err()
                .expect("provider failure");
            assert!(format!("{error:#}").contains("fixture policy provider unavailable"));
            assert_eq!(fs::read(&definition).unwrap(), original.as_bytes());
            assert_eq!(
                fs::read(root.join("policy-result")).unwrap(),
                previous_policy
            );
            assert!(root.join("loaded").exists());
            assert!(!root.join("mutations").exists());
            return;
        }
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::create_dir(root.join("commands")).unwrap();
        let id = root.join("commands/id");
        fs::write(&id, "#!/bin/sh\nprintf '501\\n'\n").unwrap();
        fs::set_permissions(&id, fs::Permissions::from_mode(0o755)).unwrap();
        let launchctl = root.join("commands/launchctl");
        fs::write(
            &launchctl,
            r#"#!/bin/sh
case "$1" in
  print-disabled)
    [ "$2" = gui/501 ] || exit 91
    if [ -f "$PIONEER_TEST_REGISTRATION_POLICY_ROOT/provider-error" ]; then
      printf 'fixture policy provider unavailable\n' >&2; exit 1
    fi
    /bin/cat "$PIONEER_TEST_REGISTRATION_POLICY_ROOT/policy.txt"
    exit $?
    ;;
  print)
    [ "$2" = gui/501 ] || exit 91
    printf 'gui/501 = {\nservices = {\n'
    if [ -f "$PIONEER_TEST_REGISTRATION_POLICY_ROOT/loaded" ]; then
      printf '0 - ai.pioneer.gateway\n'
    fi
    printf '}\n}\n'
    ;;
  bootout)
    [ "$2" = gui/501/ai.pioneer.gateway ] || exit 91
    printf 'bootout\n' >> "$PIONEER_TEST_REGISTRATION_POLICY_ROOT/mutations"
    /bin/rm "$PIONEER_TEST_REGISTRATION_POLICY_ROOT/loaded"
    ;;
  enable|disable)
    [ "$2" = gui/501/ai.pioneer.gateway ] || exit 91
    printf '%s\n' "$1" >> "$PIONEER_TEST_REGISTRATION_POLICY_ROOT/mutations"
    printf '%sd\n' "$1" > "$PIONEER_TEST_REGISTRATION_POLICY_ROOT/policy-result"
    ;;
  *) exit 92 ;;
esac
"#,
        )
        .unwrap();
        fs::set_permissions(&launchctl, fs::Permissions::from_mode(0o755)).unwrap();
        let name = format!(
            "{}::registration_recovery_preserves_autostart_policy_and_rejects_unknown_capture",
            module_path!().split_once("::").unwrap().1
        );
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &name])
            .env(FIXTURE, root)
            .env("HOME", root.join("home"))
            .env("PATH", root.join("commands"))
            .status()
            .unwrap();
        assert!(
            status.success(),
            "isolated registration policy regression failed"
        );
    }

    #[test]
    fn gui_target_observation_works_from_background_and_rejects_provider_failure() {
        use std::os::unix::fs::PermissionsExt;
        use std::{fs, process::Command};
        const FIXTURE: &str = "PIONEER_TEST_GUI_OBSERVATION_ROOT";
        if let Some(root) = std::env::var_os(FIXTURE) {
            let root = std::path::PathBuf::from(root);
            let mut settings = test_settings(pioneer_config::InstallManagedBy::Manual);
            settings.service_name = "ai.pioneer.gateway".to_owned();
            // The caller's unscoped list is empty and managername is Background.
            // Only the scoped GUI domain contains these independently supplied
            // native PID rows. Exercise the actual command boundary, not just a parser.
            for (rows, expected) in [
                ("0 - ai.pioneer.gateway\n", Some(false)),
                ("912 0 ai.pioneer.gateway\n", Some(true)),
                ("513 0 ai.pioneer.other\n", None),
            ] {
                fs::write(
                    root.join("domain.txt"),
                    format!("gui/501 = {{\nservices = {{\n{rows}}}\n}}\n"),
                )
                .unwrap();
                assert_eq!(
                    super::listed_service_activity(&settings.service_name).unwrap(),
                    expected
                );
            }
            for malformed in [
                "user/501 = {\nservices = {\n912 0 ai.pioneer.gateway\n}\n}\n",
                "gui/501 = {\nservices = {\n912 0 ai.pioneer.gateway\n}\n",
                "gui/501 = {\nservices = {\nnot-a-pid 0 ai.pioneer.gateway\n}\n}\n",
            ] {
                fs::write(root.join("domain.txt"), malformed).unwrap();
                assert!(super::listed_service_activity(&settings.service_name).is_err());
            }
            fs::write(root.join("error"), b"provider unavailable").unwrap();
            let error = super::listed_service_activity(&settings.service_name).unwrap_err();
            assert!(format!("{error:#}").contains("fixture provider unavailable"));
            return;
        }
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::create_dir(root.join("commands")).unwrap();
        let id = root.join("commands/id");
        fs::write(&id, "#!/bin/sh\nprintf '501\\n'\n").unwrap();
        fs::set_permissions(&id, fs::Permissions::from_mode(0o755)).unwrap();
        let launchctl = root.join("commands/launchctl");
        fs::write(
            &launchctl,
            r#"#!/bin/sh
case "$1" in
  managername) printf 'Background\n'; exit 0 ;;
  list) printf 'PID\tStatus\tLabel\n'; exit 0 ;;
  print)
    [ "$2" = gui/501 ] || exit 91
    if [ -f "$PIONEER_TEST_GUI_OBSERVATION_ROOT/error" ]; then
      printf 'fixture provider unavailable\n' >&2; exit 1
    fi
    /bin/cat "$PIONEER_TEST_GUI_OBSERVATION_ROOT/domain.txt"
    exit $?
    ;;
  *) exit 92 ;;
esac
"#,
        )
        .unwrap();
        fs::set_permissions(&launchctl, fs::Permissions::from_mode(0o755)).unwrap();
        let name = format!(
            "{}::gui_target_observation_works_from_background_and_rejects_provider_failure",
            module_path!().split_once("::").unwrap().1
        );
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &name])
            .env(FIXTURE, root)
            .env("PATH", root.join("commands"))
            .status()
            .unwrap();
        assert!(
            status.success(),
            "isolated GUI domain observation regression failed"
        );
    }

    use super::{
        MACOS_LAUNCH_AGENT_SCOPE_WARNING_CODE, launch_agent_scope_warnings, render_launchd_plist,
    };
    use crate::service::ServiceSettings;
    use pioneer_config::InstallManagedBy;
    use std::path::Path;

    #[test]
    fn launchd_plist_includes_path_environment_when_provided() {
        let plist = render_launchd_plist(
            "com.pioneer.gateway.test",
            Path::new("/tmp/pioneer"),
            Path::new("/tmp/logs"),
            Some("/opt/homebrew/bin:/usr/bin:/bin"),
            "ai.pioneer.test",
            Some(Path::new("/tmp/dev & checkout/config.toml")),
        );

        assert!(plist.contains("<key>EnvironmentVariables</key>"));
        assert!(plist.contains("<key>PATH</key>"));
        assert!(plist.contains("/opt/homebrew/bin:/usr/bin:/bin"));
        assert!(plist.contains("<key>AssociatedBundleIdentifiers</key>"));
        assert!(plist.contains("ai.pioneer.test"));
        assert!(plist.contains("<key>PIONEER_CONFIG</key>"));
        assert!(plist.contains("/tmp/dev &amp; checkout/config.toml"));
    }

    #[test]
    fn launch_agent_scope_warning_is_suppressed_for_desktop_managed_installs() {
        let warnings = launch_agent_scope_warnings(&test_settings(InstallManagedBy::Desktop));
        assert!(warnings.is_empty());
    }

    #[test]
    fn launch_agent_scope_warning_is_reported_for_script_installs() {
        let warnings = launch_agent_scope_warnings(&test_settings(InstallManagedBy::Script));
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, MACOS_LAUNCH_AGENT_SCOPE_WARNING_CODE);
        assert!(warnings[0].message.contains("LaunchAgent"));
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
