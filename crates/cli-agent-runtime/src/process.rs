//! Local CLI process lifecycle primitives.

use anyhow::{Context, Result, anyhow, bail};
#[cfg(windows)]
use process_wrap::tokio::JobObject;
#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use zeroize::{Zeroize, ZeroizeOnDrop};

const DEFAULT_STDERR_RING_LINES: usize = 200;
const REDACTED_SECRET: &str = "[REDACTED]";

#[derive(Clone, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(REDACTED_SECRET)
    }
}

#[derive(Clone, PartialEq, Eq)]
enum EnvironmentValue {
    Plain(String),
    Secret(SecretString),
}

impl EnvironmentValue {
    fn expose(&self) -> &str {
        match self {
            Self::Plain(value) => value.as_str(),
            Self::Secret(value) => value.expose_secret(),
        }
    }
}

impl fmt::Debug for EnvironmentValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain(value) => value.fmt(formatter),
            Self::Secret(value) => value.fmt(formatter),
        }
    }
}

/// Environment values carried across the CLI process boundary.
///
/// The type intentionally does not implement serde. Secret insertion is a
/// separate operation, and both this type and its containing process configs
/// redact those values from `Debug` output.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SensitiveEnvironment(BTreeMap<String, EnvironmentValue>);

impl SensitiveEnvironment {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn insert_plain(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0
            .insert(key.into(), EnvironmentValue::Plain(value.into()));
    }

    pub fn insert_secret(&mut self, key: impl Into<String>, value: SecretString) {
        self.0.insert(key.into(), EnvironmentValue::Secret(value));
    }

    pub fn expose(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(EnvironmentValue::expose)
    }

    pub fn expose_iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.expose()))
    }

    pub fn extend_from(&mut self, other: &Self) {
        self.0.extend(other.0.clone());
    }

    pub fn redact_text(&self, value: impl Into<String>) -> String {
        redact_exact_values(value.into(), self.secret_values().as_slice())
    }

    fn remove(&mut self, key: &str) {
        self.0.remove(key);
    }

    fn secret_values(&self) -> Vec<SecretString> {
        self.0
            .values()
            .filter_map(|value| match value {
                EnvironmentValue::Plain(_) => None,
                EnvironmentValue::Secret(value) if value.expose_secret().is_empty() => None,
                EnvironmentValue::Secret(value) => Some(value.clone()),
            })
            .collect()
    }
}

impl fmt::Debug for SensitiveEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.0.iter()).finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CLIAgentProcessSpawnConfig {
    pub executable: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub home_path: Option<String>,
    pub home_dir: Option<PathBuf>,
    pub env: SensitiveEnvironment,
    pub env_remove: Vec<String>,
    pub stderr_ring_lines: usize,
    pub process_group: bool,
    pub process_generation: Option<u64>,
}

impl CLIAgentProcessSpawnConfig {
    pub fn codex_app_server(executable: impl Into<String>, home_path: impl Into<String>) -> Self {
        Self {
            executable: executable.into(),
            args: vec!["app-server".to_owned()],
            cwd: None,
            home_path: Some(home_path.into()),
            home_dir: None,
            env: SensitiveEnvironment::new(),
            env_remove: Vec::new(),
            stderr_ring_lines: DEFAULT_STDERR_RING_LINES,
            process_group: true,
            process_generation: None,
        }
    }

    pub fn with_cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn with_home_dir(mut self, home_dir: impl Into<PathBuf>) -> Self {
        self.home_dir = Some(home_dir.into());
        self
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert_plain(key, value);
        self
    }

    pub fn with_sensitive_env(mut self, key: impl Into<String>, value: SecretString) -> Self {
        self.env.insert_secret(key, value);
        self
    }

    pub fn with_environment(mut self, env: &SensitiveEnvironment) -> Self {
        self.env.extend_from(env);
        self
    }

    pub fn with_env_removed(mut self, key: impl Into<String>) -> Self {
        self.env_remove.push(key.into());
        self
    }

    pub fn with_stderr_ring_lines(mut self, stderr_ring_lines: usize) -> Self {
        self.stderr_ring_lines = stderr_ring_lines;
        self
    }

    pub fn with_arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn with_process_generation(mut self, generation: u64) -> Result<Self> {
        if generation == 0 {
            bail!("CLI process generation must be greater than zero");
        }
        self.process_generation = Some(generation);
        Ok(self)
    }

    pub fn prepare(&self) -> Result<PreparedCLIAgentCommand> {
        if self.executable.trim().is_empty() {
            bail!("CLI executable must not be empty");
        }

        let mut env = self.env.clone();
        for (key, value) in self.env.expose_iter() {
            validate_env_key(key)?;
            validate_env_value(value)?;
        }
        let mut env_remove = inherited_sensitive_environment_names();
        for key in &self.env_remove {
            validate_env_key(key)?;
            env.remove(key);
            if !env_remove.contains(key) {
                env_remove.push(key.clone());
            }
        }

        if let Some(home_path) = self.home_path.as_deref() {
            let expanded = expand_home_path(home_path, self.home_dir.as_deref())?;
            env.insert_plain(
                "CODEX_HOME".to_owned(),
                expanded.to_string_lossy().into_owned(),
            );
        }

        let stderr_redactions = env.secret_values();

        Ok(PreparedCLIAgentCommand {
            executable: self.executable.clone(),
            args: self.args.clone(),
            cwd: self.cwd.clone(),
            env,
            env_remove,
            stderr_redactions,
            stderr_ring_lines: self.stderr_ring_lines.max(1),
            process_group: self.process_group,
            process_generation: self.process_generation,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedCLIAgentCommand {
    pub executable: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: SensitiveEnvironment,
    pub env_remove: Vec<String>,
    stderr_redactions: Vec<SecretString>,
    pub stderr_ring_lines: usize,
    pub process_group: bool,
    pub process_generation: Option<u64>,
}

pub struct CLIAgentProcess {
    child: Box<dyn ChildWrapper>,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    stderr: StderrRing,
    stderr_reader: Option<JoinHandle<()>>,
}

impl CLIAgentProcess {
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn stderr(&self) -> StderrRing {
        self.stderr.clone()
    }

    pub fn take_stdio(&mut self) -> Result<(ChildStdout, ChildStdin)> {
        let stdout = self
            .stdout
            .take()
            .ok_or_else(|| anyhow!("CLI process stdout pipe is not available"))?;
        let Some(stdin) = self.stdin.take() else {
            self.stdout = Some(stdout);
            bail!("CLI process stdin pipe is not available");
        };

        Ok((stdout, stdin))
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>> {
        self.child.try_wait().context("failed to poll CLI process")
    }

    pub async fn wait(&mut self) -> Result<ExitStatus> {
        let status = self
            .child
            .wait()
            .await
            .context("failed to wait for CLI process")?;
        self.wait_for_stderr().await?;
        Ok(status)
    }

    pub async fn terminate_with_grace(&mut self, grace: Duration) -> Result<ExitStatus> {
        if let Some(status) = self.try_wait()? {
            self.wait_for_stderr().await?;
            return Ok(status);
        }

        #[cfg(unix)]
        {
            let _ = self.child.signal(libc::SIGTERM);
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.start_kill();
        }

        let status = match timeout(grace, self.child.wait()).await {
            Ok(status) => status.context("failed to wait for gracefully terminated CLI process"),
            Err(_) => {
                Box::into_pin(self.child.kill())
                    .await
                    .context("failed to force-kill CLI process")?;
                self.child
                    .wait()
                    .await
                    .context("failed to wait for force-killed CLI process")
            }
        }?;
        self.wait_for_stderr().await?;
        Ok(status)
    }

    async fn wait_for_stderr(&mut self) -> Result<()> {
        if let Some(reader) = self.stderr_reader.take() {
            reader.await.context("failed to drain CLI process stderr")?;
        }
        Ok(())
    }
}

impl Drop for CLIAgentProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[derive(Clone)]
pub struct StderrRing {
    inner: Arc<Mutex<VecDeque<String>>>,
    max_lines: usize,
    exact_redactions: Arc<Vec<SecretString>>,
    process_generation: Option<u64>,
}

impl fmt::Debug for StderrRing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StderrRing")
            .field("max_lines", &self.max_lines)
            .field("exact_redactions", &REDACTED_SECRET)
            .field("process_generation", &self.process_generation)
            .finish_non_exhaustive()
    }
}

impl StderrRing {
    pub fn new(max_lines: usize) -> Self {
        Self::new_with_redactions(max_lines, Vec::new(), None)
    }

    fn new_with_redactions(
        max_lines: usize,
        exact_redactions: Vec<SecretString>,
        process_generation: Option<u64>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
            max_lines: max_lines.max(1),
            exact_redactions: Arc::new(exact_redactions),
            process_generation,
        }
    }

    pub fn spawn_reader<R>(&self, reader: R)
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        drop(self.spawn_reader_tracked(reader));
    }

    fn spawn_reader_tracked<R>(&self, mut reader: R) -> JoinHandle<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let ring = self.clone();
        tokio::spawn(async move {
            let mut buffer = [0_u8; 1024];
            let mut pending = String::new();
            loop {
                let read = match reader.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(_) => break,
                };
                pending.push_str(String::from_utf8_lossy(&buffer[..read]).as_ref());
                while let Some(newline) = pending.find('\n') {
                    let line = pending.drain(..=newline).collect::<String>();
                    ring.push_line(normalize_stderr_line(line.as_str())).await;
                }
            }
            if !pending.is_empty() {
                ring.push_line(normalize_stderr_line(pending.as_str()))
                    .await;
            }
        })
    }

    pub async fn lines(&self) -> Vec<String> {
        self.inner.lock().await.iter().cloned().collect()
    }

    pub async fn joined(&self) -> String {
        self.lines().await.join("\n")
    }

    pub const fn process_generation(&self) -> Option<u64> {
        self.process_generation
    }

    async fn push_line(&self, line: String) {
        let line = redact_exact_values(line, self.exact_redactions.as_slice());
        let mut guard = self.inner.lock().await;
        guard.push_back(line);
        while guard.len() > self.max_lines {
            guard.pop_front();
        }
    }
}

pub fn spawn_cli_agent_process(config: &CLIAgentProcessSpawnConfig) -> Result<CLIAgentProcess> {
    let prepared = config.prepare()?;
    spawn_prepared_cli_agent_process(&prepared)
}

pub fn spawn_prepared_cli_agent_process(
    prepared: &PreparedCLIAgentCommand,
) -> Result<CLIAgentProcess> {
    let mut command = Command::new(prepared.executable.as_str());
    scrub_inherited_cli_environment(&mut command);
    command.args(prepared.args.iter().map(String::as_str));
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    if let Some(cwd) = prepared.cwd.as_deref() {
        command.current_dir(cwd);
    }
    for key in &prepared.env_remove {
        command.env_remove(key);
    }
    command.envs(prepared.env.expose_iter());

    let mut wrapped = CommandWrap::from(command);
    wrapped.wrap(KillOnDrop);
    #[cfg(unix)]
    if prepared.process_group {
        wrapped.wrap(ProcessGroup::leader());
    }
    #[cfg(windows)]
    if prepared.process_group {
        wrapped.wrap(JobObject);
    }

    let mut child = wrapped
        .spawn()
        .with_context(|| format!("failed to spawn CLI process `{}`", prepared.executable))?;
    let stdin = child.stdin().take();
    let stdout = child.stdout().take();
    let stderr = StderrRing::new_with_redactions(
        prepared.stderr_ring_lines,
        prepared.stderr_redactions.clone(),
        prepared.process_generation,
    );
    let stderr_reader = child
        .stderr()
        .take()
        .map(|stderr_pipe| stderr.spawn_reader_tracked(stderr_pipe));

    Ok(CLIAgentProcess {
        child,
        stdin,
        stdout,
        stderr,
        stderr_reader,
    })
}

pub fn scrub_inherited_cli_environment(command: &mut Command) {
    for key in inherited_sensitive_environment_names() {
        command.env_remove(key);
    }
}

pub fn expand_home_path(raw: &str, home_dir: Option<&Path>) -> Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("home path must not be empty");
    }
    if trimmed == "~" {
        return Ok(resolve_home_dir(home_dir)?);
    }
    if let Some(rest) = trimmed.strip_prefix("~/") {
        return Ok(resolve_home_dir(home_dir)?.join(rest));
    }
    if trimmed.starts_with('~') {
        bail!("home path `{raw}` uses unsupported user-home expansion");
    }
    Ok(PathBuf::from(trimmed))
}

fn resolve_home_dir(home_dir: Option<&Path>) -> Result<PathBuf> {
    if let Some(home_dir) = home_dir {
        return Ok(home_dir.to_path_buf());
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME is required to expand `~` in CLI runtime home path"))
}

fn validate_env_key(key: &str) -> Result<()> {
    if key.is_empty() {
        bail!("environment variable name must not be empty");
    }
    if key.contains('=') || key.contains('\0') {
        bail!("environment variable name `{key}` contains an invalid character");
    }
    Ok(())
}

fn validate_env_value(value: &str) -> Result<()> {
    if value.contains('\0') {
        bail!("environment variable value contains an invalid NUL byte");
    }
    Ok(())
}

fn inherited_sensitive_environment_names() -> Vec<String> {
    std::env::vars_os()
        .filter_map(|(key, _)| key.into_string().ok())
        .filter(|key| inherited_environment_name_is_sensitive(key))
        .collect()
}

fn inherited_environment_name_is_sensitive(key: &str) -> bool {
    let normalized = key.to_ascii_uppercase();
    [
        "TOKEN",
        "SECRET",
        "PASSWORD",
        "PASSWD",
        "CREDENTIAL",
        "PRIVATE_KEY",
        "ACCESS_KEY",
        "SESSION_KEY",
        "MCP_GRANT",
        "MCP_BOOTSTRAP",
    ]
    .iter()
    .any(|pattern| normalized.contains(pattern))
}

fn redact_exact_values(mut line: String, redactions: &[SecretString]) -> String {
    for secret in redactions {
        let secret = secret.expose_secret();
        if !secret.is_empty() && line.contains(secret) {
            line = line.replace(secret, REDACTED_SECRET);
        }
    }
    line
}

fn normalize_stderr_line(line: &str) -> String {
    line.trim_end_matches('\n')
        .trim_end_matches('\r')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn expands_tilde_home_path_with_explicit_home_dir() {
        let home = PathBuf::from("/tmp/pioneer-home");
        let expanded = expand_home_path("~/.codex_work", Some(home.as_path()))
            .expect("home path should expand");
        assert_eq!(expanded, PathBuf::from("/tmp/pioneer-home/.codex_work"));
    }

    #[test]
    fn codex_app_server_prepare_never_passes_literal_tilde_home() {
        let home = PathBuf::from("/tmp/pioneer-home");
        let prepared = CLIAgentProcessSpawnConfig::codex_app_server("codex", "~/.codex_work")
            .with_home_dir(home.as_path())
            .prepare()
            .expect("config should prepare");

        assert_eq!(prepared.executable, "codex");
        assert_eq!(prepared.args, vec!["app-server"]);
        assert_eq!(
            prepared.env.expose("CODEX_HOME"),
            Some("/tmp/pioneer-home/.codex_work")
        );
        assert_ne!(prepared.env.expose("CODEX_HOME"), Some("~/.codex_work"));
    }

    #[test]
    fn sensitive_environment_redacts_debug_and_constructed_serde_output() {
        let canary = "proposal53-sensitive-environment-canary";
        let config = CLIAgentProcessSpawnConfig::codex_app_server("codex", "/tmp/codex-home")
            .with_env("SAFE_OPTION", "visible")
            .with_sensitive_env("PIONEER_CLI_MCP_GRANT", SecretString::new(canary));
        let prepared = config.prepare().expect("config should prepare");

        assert_eq!(prepared.env.expose("PIONEER_CLI_MCP_GRANT"), Some(canary));
        assert!(format!("{config:?}").contains("visible"));
        assert!(!format!("{config:?}").contains(canary));
        assert!(!format!("{prepared:?}").contains(canary));
        let serialized_log_field = serde_json::to_string(&format!("{prepared:?}"))
            .expect("debug log field should serialize");
        assert!(!serialized_log_field.contains(canary));
    }

    #[test]
    fn inherited_secret_environment_names_are_scrubbed_by_default() {
        for key in [
            "OPENAI_API_TOKEN",
            "SERVICE_SECRET",
            "DATABASE_PASSWORD",
            "AWS_ACCESS_KEY_ID",
            "PIONEER_CLI_MCP_GRANT",
        ] {
            assert!(inherited_environment_name_is_sensitive(key), "{key}");
        }
        for key in ["PATH", "HOME", "LANG", "HTTPS_PROXY", "NO_PROXY"] {
            assert!(!inherited_environment_name_is_sensitive(key), "{key}");
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn spawns_child_with_expanded_codex_home_cwd_env_and_stderr_ring() {
        let root = unique_temp_dir("spawn-child");
        let script = root.join("fake-codex");
        let cwd = root.join("workspace");
        let home = root.join("home");
        let output = root.join("observed.txt");
        fs::create_dir_all(cwd.as_path()).expect("workspace dir");
        fs::create_dir_all(home.as_path()).expect("home dir");
        write_unix_script(
            script.as_path(),
            r#"#!/bin/sh
printf 'args=%s\n' "$*" > "$OUTPUT_FILE"
printf 'codex_home=%s\n' "$CODEX_HOME" >> "$OUTPUT_FILE"
printf 'pwd=%s\n' "$(pwd)" >> "$OUTPUT_FILE"
echo err-one >&2
echo err-two >&2
echo err-three >&2
"#,
        );

        let mut process = spawn_cli_agent_process(
            &CLIAgentProcessSpawnConfig::codex_app_server(
                script.to_string_lossy().into_owned(),
                "~/.codex_work",
            )
            .with_home_dir(home.as_path())
            .with_cwd(cwd.as_path())
            .with_env("OUTPUT_FILE", output.to_string_lossy())
            .with_stderr_ring_lines(2),
        )
        .expect("process should spawn");
        let status = process.wait().await.expect("process should exit");
        assert!(status.success());

        let observed = fs::read_to_string(output.as_path()).expect("observed output");
        assert!(observed.contains("args=app-server"));
        assert!(observed.contains(&format!(
            "codex_home={}",
            home.join(".codex_work").display()
        )));
        let canonical_cwd = fs::canonicalize(cwd.as_path()).expect("canonical workspace path");
        assert!(
            observed.contains(&format!("pwd={}", canonical_cwd.display())),
            "observed output did not include canonical cwd: {observed}"
        );

        let stderr = process.stderr().lines().await;
        assert_eq!(stderr, vec!["err-two".to_owned(), "err-three".to_owned()]);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn sensitive_environment_values_are_redacted_from_child_stderr() {
        let root = unique_temp_dir("redacted-stderr");
        let script = root.join("echo-secret");
        let canary = "proposal53-child-stderr-secret-canary";
        write_unix_script(
            script.as_path(),
            r#"#!/bin/sh
echo "grant=$PIONEER_CLI_MCP_GRANT" >&2
"#,
        );

        let mut process = spawn_cli_agent_process(
            &CLIAgentProcessSpawnConfig::codex_app_server(
                script.to_string_lossy().into_owned(),
                root.join("home").to_string_lossy(),
            )
            .with_sensitive_env("PIONEER_CLI_MCP_GRANT", SecretString::new(canary))
            .with_process_generation(42)
            .expect("generation should be valid"),
        )
        .expect("process should spawn");
        assert_eq!(process.stderr().process_generation(), Some(42));
        assert!(process.wait().await.expect("process should exit").success());
        let stderr = process.stderr().joined().await;
        assert!(!stderr.contains(canary));
        assert!(stderr.contains(REDACTED_SECRET));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn terminate_with_grace_cleans_up_long_running_child() {
        let root = unique_temp_dir("terminate-child");
        let script = root.join("sleepy-codex");
        write_unix_script(
            script.as_path(),
            r#"#!/bin/sh
echo ready >&2
sleep 30
"#,
        );

        let mut process = spawn_cli_agent_process(&CLIAgentProcessSpawnConfig::codex_app_server(
            script.to_string_lossy().into_owned(),
            root.join("home").to_string_lossy(),
        ))
        .expect("process should spawn");

        let status = process
            .terminate_with_grace(Duration::from_millis(100))
            .await
            .expect("process should terminate");
        assert!(!status.success());
        assert!(process.try_wait().expect("poll after terminate").is_some());
    }

    #[cfg(unix)]
    fn write_unix_script(path: &Path, content: &str) {
        fs::write(path, content).expect("write fake executable");
        let mut permissions = fs::metadata(path).expect("script metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("script permissions");
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pioneer-cli-agent-runtime-{prefix}-{nanos}-{}",
            std::process::id()
        ));
        fs::create_dir_all(path.as_path()).expect("create temp dir");
        path
    }
}
