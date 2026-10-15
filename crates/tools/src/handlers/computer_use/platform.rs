use super::model::{AppHandle, AppMeta, AppTarget};
use crate::error::ToolError;
use crate::process_policy::ProcessEnvironmentPlan;
#[cfg(target_os = "macos")]
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommandPlan {
    pub(crate) program: String,
    pub(crate) args: Vec<String>,
    pub(crate) wait_for_exit: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RunningAppInfo {
    pub(crate) pid: Option<u32>,
    pub(crate) bundle_id: Option<String>,
    pub(crate) localized_name: Option<String>,
    pub(crate) executable_path: Option<String>,
}

pub(crate) fn launch_app_target(
    target: &AppTarget,
    launch_command: Option<&str>,
    environment: &ProcessEnvironmentPlan,
) -> Result<(), ToolError> {
    let plan = launch_command_plan(target, launch_command)?;
    run_command_plan(&plan, "launch", environment)
}

pub(crate) fn activate_app(
    app: &AppHandle,
    environment: &ProcessEnvironmentPlan,
) -> Result<(), ToolError> {
    #[cfg(not(target_os = "macos"))]
    {
        // xa11y does not currently expose a cross-platform foreground/activate API.
        // Keep app resolution semantic and avoid synthetic clicks; explicit input_* actions
        // may still require the user to foreground the app on Windows/Linux.
        let _ = app;
        let _ = environment;
        return Ok(());
    }

    #[cfg(target_os = "macos")]
    {
        let plan = activation_command_plan(app)?;
        run_command_plan(&plan, "activate", environment)
    }
}

pub(crate) fn open_path(
    path: &Path,
    environment: &ProcessEnvironmentPlan,
) -> Result<(), ToolError> {
    let plan = open_path_command_plan(path)?;
    run_command_plan(&plan, "open path", environment)
}

pub(crate) fn reveal_path(
    path: &Path,
    environment: &ProcessEnvironmentPlan,
) -> Result<(), ToolError> {
    let plan = reveal_path_command_plan(path)?;
    run_command_plan(&plan, "reveal path", environment)
}

pub(crate) fn open_url(url: &Url, environment: &ProcessEnvironmentPlan) -> Result<(), ToolError> {
    let plan = open_url_command_plan(url)?;
    run_command_plan(&plan, "open URL", environment)
}

pub(crate) fn enrich_app_identity(app: AppMeta, environment: &ProcessEnvironmentPlan) -> AppMeta {
    let Some(pid) = app.pid else {
        return app;
    };
    let Some(info) = running_app_info_by_pid(environment)
        .into_iter()
        .find(|info| info.pid == Some(pid))
    else {
        return app;
    };

    AppMeta {
        identity_key: Some(super::model::derive_app_identity_key(
            app.name.as_str(),
            app.pid,
            info.bundle_id.as_deref().or(app.bundle_id.as_deref()),
            info.executable_path
                .as_deref()
                .or(app.executable_path.as_deref()),
        )),
        bundle_id: info.bundle_id.or(app.bundle_id),
        localized_name: info.localized_name.or(app.localized_name),
        executable_path: info.executable_path.or(app.executable_path),
        ..app
    }
}

pub(crate) fn running_app_info_by_pid(environment: &ProcessEnvironmentPlan) -> Vec<RunningAppInfo> {
    #[cfg(target_os = "macos")]
    {
        return macos_running_app_info(environment);
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = environment;
        Vec::new()
    }
}

pub(crate) fn app_bundle_name_from_path(path: &Path) -> Option<String> {
    if path.extension().and_then(|value| value.to_str()) == Some("app") {
        return path.file_stem()?.to_str().map(str::to_owned);
    }
    path.file_name()?.to_str().map(str::to_owned)
}

pub(crate) fn app_bundle_path_from_path(path: &Path) -> Option<PathBuf> {
    path.ancestors()
        .find(|ancestor| {
            ancestor
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("app"))
        })
        .map(Path::to_path_buf)
}

pub(crate) fn enrich_launch_target(
    target: &AppTarget,
    environment: &ProcessEnvironmentPlan,
) -> AppTarget {
    #[cfg(target_os = "macos")]
    {
        let mut enriched = target.clone();
        let needs_bundle_id = enriched
            .bundle_id
            .as_deref()
            .is_none_or(|value| value.trim().is_empty());
        let needs_executable_path = enriched
            .executable_path
            .as_deref()
            .is_none_or(|value| value.trim().is_empty());
        if needs_bundle_id || needs_executable_path {
            if let Some(name) = enriched
                .name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                if let Some(bundle_path) = macos_find_app_bundle_by_name(name) {
                    if needs_bundle_id {
                        enriched.bundle_id =
                            macos_bundle_identifier(bundle_path.as_path(), environment);
                    }
                    if needs_executable_path {
                        enriched.executable_path =
                            macos_bundle_executable_path(bundle_path.as_path(), environment)
                                .or_else(|| {
                                    Some(bundle_path.as_os_str().to_string_lossy().into_owned())
                                });
                    }
                }
            }
        }
        return enriched;
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = environment;
        target.clone()
    }
}

#[cfg(test)]
pub(crate) fn app_identity_matches(app: &AppMeta, requested: &str) -> bool {
    let requested = normalize_app_identity_token(requested);
    if requested.is_empty() {
        return false;
    }
    [
        app.identity_key.as_deref().unwrap_or_default(),
        app.name.as_str(),
        app.localized_name.as_deref().unwrap_or_default(),
        app.bundle_id.as_deref().unwrap_or_default(),
        app.executable_path.as_deref().unwrap_or_default(),
    ]
    .into_iter()
    .any(|candidate| normalize_app_identity_token(candidate) == requested)
}

#[cfg(test)]
fn normalize_app_identity_token(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '-' && *ch != '_' && *ch != '.')
        .flat_map(char::to_lowercase)
        .collect()
}

pub(crate) fn normalize_existing_path(raw: &str) -> Result<PathBuf, ToolError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ToolError::invalid_arguments("path must be non-empty"));
    }
    let expanded = expand_home(trimmed)?;
    if !expanded.is_absolute() {
        return Err(ToolError::invalid_arguments(format!(
            "path `{}` must be absolute; relative paths are not supported by computer_use OS actions",
            trimmed
        )));
    }
    std::fs::metadata(expanded.as_path()).map_err(|error| {
        ToolError::invalid_arguments(format!(
            "path `{}` is not accessible: {error}",
            expanded.display()
        ))
    })?;
    Ok(expanded)
}

pub(crate) fn normalize_open_url(raw: &str) -> Result<Url, ToolError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ToolError::invalid_arguments("url must be non-empty"));
    }
    let url = Url::parse(trimmed)
        .map_err(|error| ToolError::invalid_arguments(format!("invalid open_url URL: {error}")))?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        scheme => Err(ToolError::invalid_arguments(format!(
            "unsupported open_url scheme `{scheme}`; supported schemes: http, https"
        ))),
    }
}

pub(crate) fn launch_command_plan(
    target: &AppTarget,
    launch_command: Option<&str>,
) -> Result<CommandPlan, ToolError> {
    if let Some(command) = launch_command
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(shell_command_plan(command));
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(bundle_id) = target
            .bundle_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Ok(CommandPlan {
                program: "/usr/bin/open".to_owned(),
                args: vec!["-b".to_owned(), bundle_id.to_owned()],
                wait_for_exit: true,
            });
        }
        if let Some(path) = target
            .executable_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let path = Path::new(path);
            let launch_path = app_bundle_path_from_path(path).unwrap_or_else(|| path.to_path_buf());
            return Ok(CommandPlan {
                program: "/usr/bin/open".to_owned(),
                args: vec![path_to_command_arg(launch_path.as_path())],
                wait_for_exit: true,
            });
        }
        if let Some(name) = target
            .name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Ok(CommandPlan {
                program: "/usr/bin/open".to_owned(),
                args: vec!["-a".to_owned(), name.to_owned()],
                wait_for_exit: true,
            });
        }
    }

    #[cfg(target_os = "windows")]
    {
        if let Some(path) = target
            .executable_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Ok(CommandPlan {
                program: path.to_owned(),
                args: Vec::new(),
                wait_for_exit: false,
            });
        }
        if let Some(name) = target
            .name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Ok(CommandPlan {
                program: "cmd".to_owned(),
                args: vec![
                    "/C".to_owned(),
                    "start".to_owned(),
                    "\"\"".to_owned(),
                    name.to_owned(),
                ],
                wait_for_exit: true,
            });
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Some(path) = target
            .executable_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Ok(CommandPlan {
                program: path.to_owned(),
                args: Vec::new(),
                wait_for_exit: false,
            });
        }
    }

    Err(ToolError::NotFound(
        "computer_use cannot launch this app target: provide target.name, target.bundle_id, target.executable_path, or launch_command."
            .to_owned(),
    ))
}

pub(crate) fn open_path_command_plan(path: &Path) -> Result<CommandPlan, ToolError> {
    let path = path_to_command_arg(path);
    #[cfg(target_os = "macos")]
    {
        return Ok(CommandPlan {
            program: "/usr/bin/open".to_owned(),
            args: vec![path],
            wait_for_exit: true,
        });
    }
    #[cfg(target_os = "windows")]
    {
        return Ok(CommandPlan {
            program: "explorer.exe".to_owned(),
            args: vec![path],
            wait_for_exit: false,
        });
    }
    #[cfg(target_os = "linux")]
    {
        return Ok(CommandPlan {
            program: "xdg-open".to_owned(),
            args: vec![path],
            wait_for_exit: true,
        });
    }
    #[allow(unreachable_code)]
    Err(ToolError::execution_failed(
        "computer_use open_path is unsupported on this platform",
    ))
}

pub(crate) fn reveal_path_command_plan(path: &Path) -> Result<CommandPlan, ToolError> {
    #[cfg(target_os = "macos")]
    {
        return Ok(CommandPlan {
            program: "/usr/bin/open".to_owned(),
            args: vec!["-R".to_owned(), path_to_command_arg(path)],
            wait_for_exit: true,
        });
    }
    #[cfg(target_os = "windows")]
    {
        return Ok(CommandPlan {
            program: "explorer.exe".to_owned(),
            args: vec![format!("/select,{}", path_to_command_arg(path))],
            wait_for_exit: false,
        });
    }
    #[cfg(target_os = "linux")]
    {
        let reveal_target = if path.is_dir() {
            path.to_path_buf()
        } else {
            path.parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| path.to_path_buf())
        };
        return Ok(CommandPlan {
            program: "xdg-open".to_owned(),
            args: vec![path_to_command_arg(reveal_target.as_path())],
            wait_for_exit: true,
        });
    }
    #[allow(unreachable_code)]
    Err(ToolError::execution_failed(
        "computer_use reveal_path is unsupported on this platform",
    ))
}

pub(crate) fn open_url_command_plan(url: &Url) -> Result<CommandPlan, ToolError> {
    #[cfg(target_os = "macos")]
    {
        return Ok(CommandPlan {
            program: "/usr/bin/open".to_owned(),
            args: vec![url.as_str().to_owned()],
            wait_for_exit: true,
        });
    }
    #[cfg(target_os = "windows")]
    {
        return Ok(CommandPlan {
            program: "cmd".to_owned(),
            args: vec![
                "/C".to_owned(),
                "start".to_owned(),
                "".to_owned(),
                url.as_str().to_owned(),
            ],
            wait_for_exit: true,
        });
    }
    #[cfg(target_os = "linux")]
    {
        return Ok(CommandPlan {
            program: "xdg-open".to_owned(),
            args: vec![url.as_str().to_owned()],
            wait_for_exit: true,
        });
    }
    #[allow(unreachable_code)]
    Err(ToolError::execution_failed(
        "computer_use open_url is unsupported on this platform",
    ))
}

pub(crate) fn activation_command_plan(app: &AppHandle) -> Result<CommandPlan, ToolError> {
    #[cfg(not(target_os = "macos"))]
    let _ = app;

    #[cfg(target_os = "macos")]
    {
        if let Some(bundle_id) = app
            .bundle_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Ok(CommandPlan {
                program: "/usr/bin/open".to_owned(),
                args: vec!["-b".to_owned(), bundle_id.to_owned()],
                wait_for_exit: true,
            });
        }
        if let Some(path) = app
            .executable_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let path = Path::new(path);
            let launch_path = app_bundle_path_from_path(path).unwrap_or_else(|| path.to_path_buf());
            return Ok(CommandPlan {
                program: "/usr/bin/open".to_owned(),
                args: vec![path_to_command_arg(launch_path.as_path())],
                wait_for_exit: true,
            });
        }
        let name = app.name.trim();
        if !name.is_empty() {
            return Ok(CommandPlan {
                program: "/usr/bin/open".to_owned(),
                args: vec!["-a".to_owned(), name.to_owned()],
                wait_for_exit: true,
            });
        }
    }

    #[allow(unreachable_code)]
    Err(ToolError::execution_failed(
        "computer_use cannot activate app because the platform helper requires a non-empty app name"
            .to_owned(),
    ))
}

fn shell_command_plan(command: &str) -> CommandPlan {
    if cfg!(target_os = "windows") {
        CommandPlan {
            program: "cmd".to_owned(),
            args: vec!["/C".to_owned(), command.to_owned()],
            wait_for_exit: false,
        }
    } else {
        CommandPlan {
            program: "/bin/sh".to_owned(),
            args: vec!["-lc".to_owned(), command.to_owned()],
            wait_for_exit: false,
        }
    }
}

fn expand_home(value: &str) -> Result<PathBuf, ToolError> {
    if value == "~" {
        return home_dir();
    }
    if let Some(rest) = value.strip_prefix("~/") {
        return Ok(home_dir()?.join(rest));
    }
    #[cfg(target_os = "windows")]
    if let Some(rest) = value.strip_prefix("~\\") {
        return Ok(home_dir()?.join(rest));
    }
    Ok(PathBuf::from(value))
}

fn home_dir() -> Result<PathBuf, ToolError> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| ToolError::invalid_arguments("cannot expand `~`: home directory is unknown"))
}

fn path_to_command_arg(path: &Path) -> String {
    path.as_os_str().to_string_lossy().into_owned()
}

#[cfg(target_os = "macos")]
fn macos_find_app_bundle_by_name(name: &str) -> Option<PathBuf> {
    let desired = app_bundle_file_name_for_name(name)?;
    let mut roots = vec![
        PathBuf::from("/Applications"),
        PathBuf::from("/System/Applications"),
        PathBuf::from("/System/Library/CoreServices"),
    ];
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        roots.push(home.join("Applications"));
    }
    roots
        .into_iter()
        .filter(|root| root.is_dir())
        .find_map(|root| find_app_bundle_exact(root.as_path(), desired.as_str(), 5))
}

#[cfg(target_os = "macos")]
fn app_bundle_file_name_for_name(name: &str) -> Option<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }
    let file_name = Path::new(trimmed)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(trimmed);
    if file_name.to_ascii_lowercase().ends_with(".app") {
        Some(file_name.to_ascii_lowercase())
    } else {
        Some(format!("{}.app", file_name).to_ascii_lowercase())
    }
}

#[cfg(target_os = "macos")]
fn find_app_bundle_exact(
    root: &Path,
    desired_file_name: &str,
    max_depth: usize,
) -> Option<PathBuf> {
    let mut queue = VecDeque::new();
    queue.push_back((root.to_path_buf(), 0usize));
    while let Some((path, depth)) = queue.pop_front() {
        let Ok(entries) = std::fs::read_dir(path.as_path()) else {
            continue;
        };
        for entry in entries.flatten() {
            let candidate = entry.path();
            let file_name = candidate
                .file_name()
                .and_then(|value| value.to_str())
                .map(|value| value.to_ascii_lowercase());
            let is_app = candidate
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("app"));
            if is_app {
                if file_name.as_deref() == Some(desired_file_name) {
                    return Some(candidate);
                }
                continue;
            }
            if depth < max_depth && candidate.is_dir() {
                queue.push_back((candidate, depth + 1));
            }
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn macos_bundle_identifier(
    bundle_path: &Path,
    environment: &ProcessEnvironmentPlan,
) -> Option<String> {
    macos_bundle_plist_value(bundle_path, "CFBundleIdentifier", environment)
}

#[cfg(target_os = "macos")]
fn macos_bundle_executable_path(
    bundle_path: &Path,
    environment: &ProcessEnvironmentPlan,
) -> Option<String> {
    let executable = macos_bundle_plist_value(bundle_path, "CFBundleExecutable", environment)?;
    Some(
        bundle_path
            .join("Contents")
            .join("MacOS")
            .join(executable)
            .as_os_str()
            .to_string_lossy()
            .into_owned(),
    )
}

#[cfg(target_os = "macos")]
fn macos_bundle_plist_value(
    bundle_path: &Path,
    key: &str,
    environment: &ProcessEnvironmentPlan,
) -> Option<String> {
    let info_plist = bundle_path.join("Contents").join("Info.plist");
    let mut command = Command::new("/usr/bin/plutil");
    command
        .args(["-extract", key, "raw", "-o", "-"])
        .arg(info_plist.as_path());
    environment.apply_to_std_command(&mut command);
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(output.stdout.as_slice())
        .trim()
        .to_owned();
    (!value.is_empty()).then_some(value)
}

fn run_command_plan(
    plan: &CommandPlan,
    purpose: &str,
    environment: &ProcessEnvironmentPlan,
) -> Result<(), ToolError> {
    if plan.wait_for_exit {
        let mut command = Command::new(plan.program.as_str());
        command.args(plan.args.iter().map(String::as_str));
        environment.apply_to_std_command(&mut command);
        let output = command.output().map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to {purpose} computer_use target with `{} {}`: {error}",
                plan.program,
                plan.args.join(" ")
            ))
        })?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(output.stderr.as_slice());
        let stdout = String::from_utf8_lossy(output.stdout.as_slice());
        let diagnostic = if !stderr.trim().is_empty() {
            stderr.trim()
        } else {
            stdout.trim()
        };
        return Err(ToolError::execution_failed(format!(
            "failed to {purpose} computer_use target with `{} {}`: exit status {}{}",
            plan.program,
            plan.args.join(" "),
            output.status,
            if diagnostic.is_empty() {
                String::new()
            } else {
                format!(": {diagnostic}")
            }
        )));
    }

    let mut command = Command::new(plan.program.as_str());
    command.args(plan.args.iter().map(String::as_str));
    environment.apply_to_std_command(&mut command);
    let mut child = command.spawn().map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to {purpose} computer_use target with `{} {}`: {error}",
            plan.program,
            plan.args.join(" ")
        ))
    })?;
    let _ = child.try_wait();
    Ok(())
}

#[cfg(target_os = "macos")]
fn macos_running_app_info(environment: &ProcessEnvironmentPlan) -> Vec<RunningAppInfo> {
    let mut command = Command::new("/usr/bin/lsappinfo");
    command.arg("visibleProcessList");
    environment.apply_to_std_command(&mut command);
    let output = command.output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let visible = String::from_utf8_lossy(output.stdout.as_slice());
    parse_lsappinfo_visible_process_list(visible.as_ref())
        .into_iter()
        .filter_map(|(asn, name)| macos_running_app_info_for_asn(asn.as_str(), name, environment))
        .collect()
}

#[cfg(target_os = "macos")]
fn macos_running_app_info_for_asn(
    asn: &str,
    visible_name: String,
    environment: &ProcessEnvironmentPlan,
) -> Option<RunningAppInfo> {
    let mut command = Command::new("/usr/bin/lsappinfo");
    command.args([
        "info",
        "-only",
        "pid,bundleid,bundlepath,executablepath,LSDisplayName",
        asn,
    ]);
    environment.apply_to_std_command(&mut command);
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(output.stdout.as_slice());
    let fields = parse_lsappinfo_key_values(text.as_ref());
    Some(RunningAppInfo {
        pid: fields
            .get("pid")
            .and_then(|value| value.parse::<u32>().ok()),
        bundle_id: fields.get("CFBundleIdentifier").cloned(),
        localized_name: fields
            .get("LSDisplayName")
            .cloned()
            .or_else(|| Some(visible_name)),
        executable_path: fields.get("CFBundleExecutablePath").cloned(),
    })
}

#[cfg(target_os = "macos")]
fn parse_lsappinfo_visible_process_list(text: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
    let mut rest = text;
    while let Some(asn_start) = rest.find("ASN:") {
        rest = &rest[asn_start..];
        let Some(quote_start) = rest.find('"') else {
            break;
        };
        let asn = rest[..quote_start]
            .trim_end_matches(|ch| ch == ':' || ch == '-')
            .trim()
            .to_owned();
        let name_start = quote_start + 1;
        let Some(quote_end_rel) = rest[name_start..].find('"') else {
            break;
        };
        let name_end = name_start + quote_end_rel;
        result.push((asn, rest[name_start..name_end].replace('_', " ")));
        rest = &rest[name_end + 1..];
    }
    result
}

#[cfg(target_os = "macos")]
fn parse_lsappinfo_key_values(text: &str) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        let Some((raw_key, raw_value)) = line.split_once('=') else {
            continue;
        };
        let key = raw_key.trim().trim_matches('"');
        let value = raw_value.trim().trim_matches('"');
        if !key.is_empty() && !value.is_empty() {
            fields.insert(key.to_owned(), value.to_owned());
        }
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[test]
    fn computer_use_macos_launch_plan_uses_open_a_for_app_name() {
        let plan = launch_command_plan(
            &AppTarget {
                name: Some("ExampleApp".to_owned()),
                pid: None,
                identity_key: None,
                bundle_id: None,
                executable_path: None,
            },
            None,
        )
        .expect("plan");
        assert_eq!(plan.program, "/usr/bin/open");
        assert_eq!(plan.args, vec!["-a".to_owned(), "ExampleApp".to_owned()]);
        assert!(plan.wait_for_exit);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn computer_use_macos_launch_plan_supports_bundle_id_and_executable_path() {
        let bundle_plan = launch_command_plan(
            &AppTarget {
                name: None,
                pid: None,
                identity_key: None,
                bundle_id: Some("com.example.App".to_owned()),
                executable_path: None,
            },
            None,
        )
        .expect("bundle plan");
        assert_eq!(bundle_plan.program, "/usr/bin/open");
        assert_eq!(
            bundle_plan.args,
            vec!["-b".to_owned(), "com.example.App".to_owned()]
        );
        assert!(bundle_plan.wait_for_exit);

        let path_plan = launch_command_plan(
            &AppTarget {
                name: None,
                pid: None,
                identity_key: None,
                bundle_id: None,
                executable_path: Some("/Applications/Example.app".to_owned()),
            },
            None,
        )
        .expect("path plan");
        assert_eq!(path_plan.program, "/usr/bin/open");
        assert_eq!(path_plan.args, vec!["/Applications/Example.app".to_owned()]);
        assert!(path_plan.wait_for_exit);

        let executable_plan = launch_command_plan(
            &AppTarget {
                name: None,
                pid: None,
                identity_key: None,
                bundle_id: None,
                executable_path: Some(
                    "/Applications/Example.app/Contents/MacOS/Example".to_owned(),
                ),
            },
            None,
        )
        .expect("executable path plan");
        assert_eq!(
            executable_plan.args,
            vec!["/Applications/Example.app".to_owned()]
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn computer_use_macos_activation_prefers_stable_identity_over_display_name() {
        let bundle_plan = activation_command_plan(&AppHandle {
            identity_key: None,
            name: "Localized Example".to_owned(),
            pid: Some(42),
            role: None,
            window_title: None,
            bundle_id: Some("com.example.App".to_owned()),
            localized_name: Some("Localized Example".to_owned()),
            executable_path: Some("/Applications/Example.app/Contents/MacOS/Example".to_owned()),
            frontmost: None,
        })
        .expect("bundle activation plan");
        assert_eq!(
            bundle_plan.args,
            vec!["-b".to_owned(), "com.example.App".to_owned()]
        );

        let executable_plan = activation_command_plan(&AppHandle {
            identity_key: None,
            name: "Localized Example".to_owned(),
            pid: Some(42),
            role: None,
            window_title: None,
            bundle_id: None,
            localized_name: Some("Localized Example".to_owned()),
            executable_path: Some("/Applications/Example.app/Contents/MacOS/Example".to_owned()),
            frontmost: None,
        })
        .expect("executable activation plan");
        assert_eq!(
            executable_plan.args,
            vec!["/Applications/Example.app".to_owned()]
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn lsappinfo_parsers_extract_visible_process_metadata() {
        let visible =
            r#"ASN:0x0-0x111111-"Example_App": ASN:0x0-0x222222-"Localized_Example_App":"#;
        let apps = parse_lsappinfo_visible_process_list(visible);
        assert_eq!(
            apps,
            vec![
                ("ASN:0x0-0x111111".to_owned(), "Example App".to_owned()),
                (
                    "ASN:0x0-0x222222".to_owned(),
                    "Localized Example App".to_owned()
                )
            ]
        );

        let fields = parse_lsappinfo_key_values(
            "\"pid\"=27779\n\"CFBundleIdentifier\"=\"com.example.App\"\n\"CFBundleExecutablePath\"=\"/Applications/Example.app/Contents/MacOS/Example\"",
        );
        assert_eq!(fields.get("pid").map(String::as_str), Some("27779"));
        assert_eq!(
            fields.get("CFBundleIdentifier").map(String::as_str),
            Some("com.example.App")
        );
    }

    #[test]
    fn computer_use_macos_launch_plan_allows_explicit_command() {
        let plan = launch_command_plan(
            &AppTarget {
                name: None,
                pid: Some(42),
                identity_key: None,
                bundle_id: None,
                executable_path: None,
            },
            Some("echo ok"),
        )
        .expect("plan");
        assert!(plan.args.iter().any(|arg| arg == "echo ok"));
    }

    #[test]
    fn open_path_normalizes_home_and_rejects_relative_paths() {
        let home = normalize_existing_path("~").expect("home path");
        assert!(home.is_absolute());
        let error = normalize_existing_path("relative/path").expect_err("relative path");
        assert!(
            error
                .to_string()
                .contains("relative paths are not supported")
        );
    }

    #[test]
    fn open_path_command_plan_uses_platform_handler() {
        let plan = open_path_command_plan(std::env::temp_dir().as_path()).expect("open path plan");
        assert!(!plan.program.is_empty());
        assert!(!plan.args.is_empty());
    }

    #[test]
    fn reveal_path_command_plan_uses_platform_handler() {
        let plan =
            reveal_path_command_plan(std::env::temp_dir().as_path()).expect("reveal path plan");
        assert!(!plan.program.is_empty());
        assert!(!plan.args.is_empty());
    }

    #[test]
    fn open_url_validates_scheme_and_builds_command_plan() {
        let url = normalize_open_url("https://example.com/path").expect("valid URL");
        let plan = open_url_command_plan(&url).expect("open url plan");
        assert!(!plan.program.is_empty());
        assert!(plan.args.iter().any(|arg| arg.contains("example.com")));

        let error = normalize_open_url("ftp://example.com/file").expect_err("unsupported scheme");
        assert!(error.to_string().contains("unsupported open_url scheme"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn computer_use_linux_requires_explicit_launch_command() {
        let error = launch_command_plan(
            &AppTarget {
                name: Some("ExampleApp".to_owned()),
                pid: None,
                identity_key: None,
                bundle_id: None,
                executable_path: None,
            },
            None,
        )
        .expect_err("linux requires explicit command");
        assert!(error.to_string().contains("Linux"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn computer_use_linux_open_path_uses_xdg_open() {
        let plan = open_path_command_plan(Path::new("/tmp")).expect("open path plan");
        assert_eq!(plan.program, "xdg-open");
        assert_eq!(plan.args, vec!["/tmp".to_owned()]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn computer_use_linux_reveal_path_opens_parent_with_xdg_open() {
        let plan = reveal_path_command_plan(Path::new("/tmp/file.txt")).expect("reveal path plan");
        assert_eq!(plan.program, "xdg-open");
        assert_eq!(plan.args, vec!["/tmp".to_owned()]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn computer_use_linux_open_url_uses_xdg_open() {
        let url = normalize_open_url("https://example.com").expect("valid url");
        let plan = open_url_command_plan(&url).expect("open url plan");
        assert_eq!(plan.program, "xdg-open");
        assert_eq!(plan.args, vec!["https://example.com/".to_owned()]);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn computer_use_windows_launch_plan_uses_shell_start() {
        let plan = launch_command_plan(
            &AppTarget {
                name: Some("notepad".to_owned()),
                pid: None,
                identity_key: None,
                bundle_id: None,
                executable_path: None,
            },
            None,
        )
        .expect("plan");
        assert_eq!(plan.program, "cmd");
        assert!(plan.args.iter().any(|arg| arg == "start"));
        assert!(plan.args.iter().any(|arg| arg == "notepad"));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn computer_use_windows_open_path_uses_explorer() {
        let plan = open_path_command_plan(Path::new("C:\\Users\\Public")).expect("open path plan");
        assert_eq!(plan.program, "explorer.exe");
        assert_eq!(plan.args, vec!["C:\\Users\\Public".to_owned()]);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn computer_use_windows_reveal_path_uses_explorer_select() {
        let plan =
            reveal_path_command_plan(Path::new("C:\\Users\\Public\\file.txt")).expect("plan");
        assert_eq!(plan.program, "explorer.exe");
        assert_eq!(
            plan.args,
            vec!["/select,C:\\Users\\Public\\file.txt".to_owned()]
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn computer_use_windows_open_url_uses_default_handler() {
        let url = normalize_open_url("https://example.com").expect("valid url");
        let plan = open_url_command_plan(&url).expect("open url plan");
        assert_eq!(plan.program, "cmd");
        assert!(plan.args.iter().any(|arg| arg == "start"));
        assert!(plan.args.iter().any(|arg| arg == "https://example.com/"));
    }
}
