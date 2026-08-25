use crate::context::ExecCommandArgs;
use crate::error::ToolError;
use crate::{FilePolicyChecker, FilePolicyDecision};
use pioneer_protocol::{
    PermissionBehavior, TurnEnvironmentPolicy, TurnExecutionSecuritySnapshot,
    TurnProcessPolicySnapshot,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpawnPlan {
    pub cwd: PathBuf,
    pub timeout_ms: u64,
    pub inherit_environment: bool,
    pub environment: BTreeMap<String, String>,
    pub removed_environment: Vec<String>,
}

pub fn build_process_spawn_plan(
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    base_dir: &Path,
    args: &ExecCommandArgs,
    invocation_environment: &BTreeMap<String, String>,
    default_timeout_ms: u64,
) -> Result<ProcessSpawnPlan, ToolError> {
    build_process_spawn_plan_with_host_environment(
        snapshot,
        base_dir,
        args,
        invocation_environment,
        std::env::vars(),
        default_timeout_ms,
    )
}

fn build_process_spawn_plan_with_host_environment<I>(
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    base_dir: &Path,
    args: &ExecCommandArgs,
    invocation_environment: &BTreeMap<String, String>,
    host_environment: I,
    default_timeout_ms: u64,
) -> Result<ProcessSpawnPlan, ToolError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let command = command_argv(args)?;
    let cwd = resolve_process_cwd(base_dir, args.workdir.as_deref());

    let Some(snapshot) = snapshot else {
        return Err(ToolError::Rejected(
            "missing turn execution security snapshot; refusing to spawn process without resolved sandbox policy".to_owned(),
        ));
    };

    enforce_process_policy(snapshot, command)?;
    enforce_process_cwd(snapshot, cwd.as_path())?;

    let (inherit_environment, environment, removed_environment) = sanitize_environment(
        &snapshot.process.environment,
        invocation_environment,
        host_environment,
    );

    Ok(ProcessSpawnPlan {
        cwd,
        timeout_ms: capped_timeout(args.timeout_ms, default_timeout_ms, &snapshot.process),
        inherit_environment,
        environment,
        removed_environment,
    })
}

fn enforce_process_policy(
    snapshot: &TurnExecutionSecuritySnapshot,
    command: &[String],
) -> Result<(), ToolError> {
    if !snapshot.process.shell.enabled {
        return Err(ToolError::Rejected(
            "process policy denied shell command: shell execution is disabled".to_owned(),
        ));
    }
    if snapshot.permission_profile.effective_policy.shell_command == PermissionBehavior::Deny {
        return Err(ToolError::Rejected(
            "process policy denied shell command by turn permission profile".to_owned(),
        ));
    }

    let executable = command
        .first()
        .map(|value| value.as_str())
        .unwrap_or_default();
    if command_matches_any(executable, &snapshot.process.command_risk.denied_commands) {
        return Err(ToolError::Rejected(format!(
            "process policy denied command `{executable}`"
        )));
    }
    if !snapshot
        .process
        .command_risk
        .allowed_command_families
        .is_empty()
        && !command_matches_any(
            executable,
            &snapshot.process.command_risk.allowed_command_families,
        )
    {
        return Err(ToolError::Rejected(format!(
            "process policy denied command `{executable}` outside allowed command families"
        )));
    }

    Ok(())
}

fn enforce_process_cwd(
    snapshot: &TurnExecutionSecuritySnapshot,
    cwd: &Path,
) -> Result<(), ToolError> {
    match FilePolicyChecker::check_read(snapshot, cwd) {
        FilePolicyDecision::Allowed(_) => Ok(()),
        FilePolicyDecision::Denied(deny) => Err(ToolError::Rejected(format!(
            "process policy denied cwd `{}`: {}",
            deny.requested_path.display(),
            deny.message
        ))),
    }
}

fn sanitize_environment<I>(
    policy: &TurnEnvironmentPolicy,
    invocation_environment: &BTreeMap<String, String>,
    host_environment: I,
) -> (bool, BTreeMap<String, String>, Vec<String>)
where
    I: IntoIterator<Item = (String, String)>,
{
    let allowed_vars = policy
        .allowed_vars
        .iter()
        .map(|value| value.to_ascii_uppercase())
        .collect::<BTreeSet<_>>();
    let allow_list_active = !policy.inherit || !allowed_vars.is_empty();

    let mut host_environment = host_environment.into_iter().collect::<BTreeMap<_, _>>();
    for (key, value) in invocation_environment {
        host_environment.insert(key.clone(), value.clone());
    }

    if allow_list_active {
        let environment = host_environment
            .iter()
            .filter(|(key, _)| allowed_vars.contains(&key.to_ascii_uppercase()))
            .filter(|(key, _)| !env_denied(policy, key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let removed_environment = host_environment
            .keys()
            .filter(|key| {
                !allowed_vars.contains(&key.to_ascii_uppercase()) || env_denied(policy, key)
            })
            .cloned()
            .collect::<Vec<_>>();
        return (false, environment, removed_environment);
    }

    let environment = invocation_environment
        .iter()
        .filter(|(key, _)| !env_denied(policy, key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let removed_environment = host_environment
        .keys()
        .filter(|key| env_denied(policy, key))
        .cloned()
        .collect::<Vec<_>>();

    (policy.inherit, environment, removed_environment)
}

fn env_denied(policy: &TurnEnvironmentPolicy, name: &str) -> bool {
    policy
        .denied_patterns
        .iter()
        .any(|pattern| policy_pattern_matches(pattern, name))
}

fn command_matches_any(command: &str, patterns: &[String]) -> bool {
    patterns
        .iter()
        .any(|pattern| command_matches(pattern.as_str(), command))
}

fn command_matches(pattern: &str, command: &str) -> bool {
    let command_name = Path::new(command)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(command);
    policy_pattern_matches(pattern, command) || policy_pattern_matches(pattern, command_name)
}

fn policy_pattern_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return false;
    }
    let normalized_pattern = pattern
        .trim_start_matches(".*")
        .trim_end_matches(".*")
        .to_ascii_uppercase();
    let normalized_value = value.to_ascii_uppercase();
    if normalized_pattern.is_empty() {
        return false;
    }
    normalized_value == normalized_pattern || normalized_value.contains(&normalized_pattern)
}

fn capped_timeout(
    requested_timeout_ms: Option<u64>,
    default_timeout_ms: u64,
    policy: &TurnProcessPolicySnapshot,
) -> u64 {
    requested_timeout_ms
        .unwrap_or(default_timeout_ms)
        .min(policy.timeout.max_duration_ms)
}

fn command_argv(args: &ExecCommandArgs) -> Result<&[String], ToolError> {
    let command = args
        .command
        .as_deref()
        .ok_or_else(|| ToolError::invalid_arguments("`command` argv is required"))?;
    if command.is_empty() {
        return Err(ToolError::invalid_arguments("`command` argv is required"));
    }
    Ok(command)
}

pub fn resolve_process_cwd(base_dir: &Path, requested: Option<&str>) -> PathBuf {
    let Some(requested) = requested else {
        return base_dir.to_path_buf();
    };
    let path = Path::new(requested);
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    };
    normalize_path_lexically(path)
}

fn normalize_path_lexically(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                let rooted = matches!(
                    normalized.components().next_back(),
                    Some(std::path::Component::RootDir | std::path::Component::Prefix(_))
                );
                if !rooted && !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            std::path::Component::Normal(_)
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        TurnCommandRiskPolicy, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
        TurnPermissionMode, TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
        TurnProcessTimeoutPolicy,
    };

    fn exec_args(command: &[&str]) -> ExecCommandArgs {
        ExecCommandArgs {
            command: Some(command.iter().map(|value| (*value).to_owned()).collect()),
            workdir: None,
            timeout_ms: None,
            max_output_tokens: None,
            yield_time_ms: None,
            tty: Some(false),
        }
    }

    fn workspace_write_snapshot(root: &Path) -> TurnExecutionSecuritySnapshot {
        TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            root.to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                root.to_string_lossy(),
            )],
            1,
        )
    }

    #[test]
    fn process_policy_rejects_missing_security_snapshot() {
        let root = tempfile::tempdir().expect("root");

        let error = build_process_spawn_plan_with_host_environment(
            None,
            root.path(),
            &exec_args(&["sh", "-c", "true"]),
            &BTreeMap::new(),
            BTreeMap::new(),
            60_000,
        )
        .expect_err("missing security snapshot should fail closed");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("missing turn execution security snapshot"))
        );
    }

    #[test]
    fn process_policy_denies_cwd_outside_snapshot_before_spawn() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.as_path()).expect("create root");
        std::fs::create_dir_all(outside.as_path()).expect("create outside");
        let mut args = exec_args(&["sh", "-c", "true"]);
        args.workdir = Some("../outside".to_owned());
        let snapshot = workspace_write_snapshot(root.as_path());

        let error = build_process_spawn_plan_with_host_environment(
            Some(&snapshot),
            root.as_path(),
            &args,
            &BTreeMap::new(),
            BTreeMap::new(),
            60_000,
        )
        .expect_err("outside cwd should be denied");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("process policy denied cwd") && message.contains("outside the allowed sandbox roots"))
        );
    }

    #[test]
    fn process_policy_scrubs_denied_env_vars_from_plan() {
        let root = tempfile::tempdir().expect("root");
        let snapshot = workspace_write_snapshot(root.path());
        let mut invocation_env = BTreeMap::new();
        invocation_env.insert("SAFE_VALUE".to_owned(), "ok".to_owned());
        invocation_env.insert("API_TOKEN".to_owned(), "secret".to_owned());
        let host_env = BTreeMap::from([
            ("PATH".to_owned(), "/bin".to_owned()),
            ("DB_PASSWORD".to_owned(), "secret".to_owned()),
        ]);

        let plan = build_process_spawn_plan_with_host_environment(
            Some(&snapshot),
            root.path(),
            &exec_args(&["sh", "-c", "true"]),
            &invocation_env,
            host_env,
            60_000,
        )
        .expect("plan should build");

        assert!(!plan.environment.contains_key("SAFE_VALUE"));
        assert!(!plan.environment.contains_key("API_TOKEN"));
        assert!(plan.removed_environment.contains(&"SAFE_VALUE".to_owned()));
        assert!(plan.removed_environment.contains(&"API_TOKEN".to_owned()));
        assert!(plan.removed_environment.contains(&"DB_PASSWORD".to_owned()));
    }

    #[test]
    fn restricted_process_policy_does_not_inherit_authority_environment() {
        let root = tempfile::tempdir().expect("root");
        let snapshot = workspace_write_snapshot(root.path());
        let invocation_env = BTreeMap::from([
            ("OPENAI_API_KEY".to_owned(), "openai-canary".to_owned()),
            (
                "custom_workspace_credential".to_owned(),
                "custom-canary".to_owned(),
            ),
        ]);
        let host_env = BTreeMap::from([
            ("AWS_ACCESS_KEY_ID".to_owned(), "aws-canary".to_owned()),
            (
                "AWS_SESSION_TOKEN".to_owned(),
                "aws-session-canary".to_owned(),
            ),
            (
                "GOOGLE_APPLICATION_CREDENTIALS".to_owned(),
                "gcp-canary".to_owned(),
            ),
            ("AZURE_CLIENT_ID".to_owned(), "azure-canary".to_owned()),
            ("DATABASE_URL".to_owned(), "db-canary".to_owned()),
            ("SSH_AUTH_SOCK".to_owned(), "ssh-canary".to_owned()),
            ("PATH".to_owned(), "/trusted/bin".to_owned()),
        ]);

        let plan = build_process_spawn_plan_with_host_environment(
            Some(&snapshot),
            root.path(),
            &exec_args(&["sh", "-c", "true"]),
            &invocation_env,
            host_env,
            60_000,
        )
        .expect("restricted process plan should build");

        assert!(!plan.inherit_environment);
        for name in [
            "OPENAI_API_KEY",
            "custom_workspace_credential",
            "AWS_ACCESS_KEY_ID",
            "AWS_SESSION_TOKEN",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "AZURE_CLIENT_ID",
            "DATABASE_URL",
            "SSH_AUTH_SOCK",
            "PATH",
        ] {
            assert!(
                !plan.environment.contains_key(name),
                "restricted child environment leaked {name}"
            );
            assert!(
                plan.removed_environment
                    .iter()
                    .any(|removed| removed == name),
                "restricted child environment did not account for removed {name}"
            );
        }
    }

    #[test]
    fn process_policy_denies_command_listed_by_risk_policy() {
        let root = tempfile::tempdir().expect("root");
        let mut snapshot = workspace_write_snapshot(root.path());
        snapshot.process.command_risk = TurnCommandRiskPolicy {
            denied_commands: vec!["rm".to_owned()],
            allowed_command_families: Vec::new(),
        };

        let error = build_process_spawn_plan_with_host_environment(
            Some(&snapshot),
            root.path(),
            &exec_args(&["/bin/rm", "-rf", "tmp"]),
            &BTreeMap::new(),
            BTreeMap::new(),
            60_000,
        )
        .expect_err("denied command should fail");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("denied command"))
        );
    }

    #[test]
    fn process_policy_denies_shell_command_permission_behavior() {
        let root = tempfile::tempdir().expect("root");
        let mut snapshot = workspace_write_snapshot(root.path());
        snapshot.permission_profile.effective_policy.shell_command = PermissionBehavior::Deny;

        let error = build_process_spawn_plan_with_host_environment(
            Some(&snapshot),
            root.path(),
            &exec_args(&["sh", "-c", "true"]),
            &BTreeMap::new(),
            BTreeMap::new(),
            60_000,
        )
        .expect_err("denied shell permission should fail");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("permission profile"))
        );
    }

    #[test]
    fn process_policy_caps_timeout_from_snapshot() {
        let root = tempfile::tempdir().expect("root");
        let mut snapshot = workspace_write_snapshot(root.path());
        snapshot.process.timeout = TurnProcessTimeoutPolicy {
            max_duration_ms: 5_000,
        };
        let mut args = exec_args(&["sh", "-c", "true"]);
        args.timeout_ms = Some(30_000);

        let plan = build_process_spawn_plan_with_host_environment(
            Some(&snapshot),
            root.path(),
            &args,
            &BTreeMap::new(),
            BTreeMap::new(),
            60_000,
        )
        .expect("plan should build");

        assert_eq!(plan.timeout_ms, 5_000);
    }
}
